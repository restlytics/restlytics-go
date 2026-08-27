package restlytics

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type recordingExporter struct {
	mu              sync.Mutex
	traces          []ExportTraceServiceRequest
	logs            []ExportLogsServiceRequest
	contextsBounded bool
	flushes         atomic.Int64
	shutdowns       atomic.Int64
}

func (e *recordingExporter) ExportTraces(ctx context.Context, payload ExportTraceServiceRequest) error {
	e.mu.Lock()
	e.contextsBounded = e.contextsBounded || contextHasDeadline(ctx)
	e.traces = append(e.traces, payload)
	e.mu.Unlock()
	return nil
}

func (e *recordingExporter) ExportLogs(ctx context.Context, payload ExportLogsServiceRequest) error {
	e.mu.Lock()
	e.contextsBounded = e.contextsBounded || contextHasDeadline(ctx)
	e.logs = append(e.logs, payload)
	e.mu.Unlock()
	return nil
}

func (e *recordingExporter) Flush(context.Context) error {
	e.flushes.Add(1)
	return nil
}

func (e *recordingExporter) Shutdown(context.Context) error {
	e.shutdowns.Add(1)
	return nil
}

func contextHasDeadline(ctx context.Context) bool {
	_, ok := ctx.Deadline()
	return ok
}

func (e *recordingExporter) snapshot() ([]ExportTraceServiceRequest, []ExportLogsServiceRequest, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]ExportTraceServiceRequest(nil), e.traces...), append([]ExportLogsServiceRequest(nil), e.logs...), e.contextsBounded
}

func TestCustomExporterDeliversBothSignalsWithoutRestlyticsCredentials(t *testing.T) {
	exporter := &recordingExporter{}
	t.Setenv("RESTLYTICS_KEY", "")
	if !(Config{CustomExporter: exporter}).Resolve().Enabled() {
		t.Fatal("custom exporter must work without a Restlytics ingest key")
	}
	t.Setenv("RESTLYTICS_KEY", "rl_tenant_a_canary")
	rl := Init(Config{
		CustomExporter: exporter,
		ServiceName:    "checkout",
		Environment:    "test",
		SampleRate:     1,
		Logs:           true,
		TimeoutMs:      250,
	})
	ctx := rl.Tracer().Start(context.Background(), "GET /checkout", "")
	if root := RootSpan(ctx); root != nil {
		root.SetString(AttrURLFull, "https://alice:secret@example.test/pay?token=raw-secret#fragment")
	}
	rl.Tracer().Finish(ctx)
	record := slog.NewRecord(time.Now(), slog.LevelError, "failed email=alice@example.test password=hunter2", 0)
	if err := rl.SlogHandler(nil).Handle(ctx, record); err != nil {
		t.Fatalf("log handler: %v", err)
	}

	flushCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if !rl.Flush(flushCtx) {
		t.Fatal("custom exporter did not flush")
	}

	traces, logs, bounded := exporter.snapshot()
	if len(traces) != 1 || len(logs) != 1 {
		t.Fatalf("signals delivered traces=%d logs=%d", len(traces), len(logs))
	}
	if !bounded {
		t.Fatal("export calls did not receive a timeout context")
	}
	encoded, err := json.Marshal(struct {
		Traces []ExportTraceServiceRequest
		Logs   []ExportLogsServiceRequest
	}{traces, logs})
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"alice:", "raw-secret", "fragment", "alice@example.test", "hunter2", "X-Restlytics-Key", "rl_tenant_a_canary"} {
		if strings.Contains(strings.ToLower(string(encoded)), strings.ToLower(secret)) {
			t.Fatalf("exporter received credential or unredacted source value %q: %s", secret, encoded)
		}
	}
	if !strings.Contains(string(encoded), `"service.name"`) || !strings.Contains(string(encoded), `"deployment.environment"`) {
		t.Fatalf("exporter did not receive production OTLP resources: %s", encoded)
	}

	diagnostics, ok := rl.TransportDiagnostics()
	if !ok || diagnostics.DeliveredBatches != 2 || diagnostics.FailedBatches != 0 {
		t.Fatalf("unsafe or incomplete diagnostics: available=%t snapshot=%+v", ok, diagnostics)
	}
	if !rl.Shutdown(flushCtx) || !rl.Shutdown(flushCtx) {
		t.Fatal("custom exporter shutdown was not successful and idempotent")
	}
	if exporter.flushes.Load() != 2 || exporter.shutdowns.Load() != 1 {
		t.Fatalf("unexpected lifecycle calls flush=%d shutdown=%d", exporter.flushes.Load(), exporter.shutdowns.Load())
	}
}

type failingExporter struct{}

func (failingExporter) ExportTraces(context.Context, ExportTraceServiceRequest) error {
	return errors.New("provider unavailable")
}

func (failingExporter) ExportLogs(context.Context, ExportLogsServiceRequest) error {
	panic("provider bug")
}

func TestCustomExporterErrorsAndPanicsNeverEscapeHostWork(t *testing.T) {
	rl := Init(Config{CustomExporter: failingExporter{}, SampleRate: 1, Logs: true})
	ctx := rl.Tracer().Start(context.Background(), "GET /safe", "")
	rl.Tracer().Finish(ctx)
	if err := rl.SlogHandler(nil).Handle(ctx, slog.NewRecord(time.Now(), slog.LevelError, "safe", 0)); err != nil {
		t.Fatalf("export failure reached host logger: %v", err)
	}

	flushCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if !rl.Flush(flushCtx) {
		t.Fatal("failed callbacks should settle without breaking queue flush")
	}
	diagnostics, ok := rl.TransportDiagnostics()
	if !ok || diagnostics.FailedBatches != 2 || diagnostics.DeliveredBatches != 0 {
		t.Fatalf("callback failures not safely accounted: available=%t snapshot=%+v", ok, diagnostics)
	}
	if !rl.Shutdown(flushCtx) {
		t.Fatal("callback failures prevented bounded shutdown")
	}
}

type blockingExporter struct {
	gate chan struct{}
}

func (e *blockingExporter) ExportTraces(context.Context, ExportTraceServiceRequest) error {
	<-e.gate
	return nil
}

func (e *blockingExporter) ExportLogs(context.Context, ExportLogsServiceRequest) error {
	<-e.gate
	return nil
}

func TestManagedExporterIsNonBlockingBoundedAndShutdownIsTimed(t *testing.T) {
	exporter := &blockingExporter{gate: make(chan struct{})}
	transport := newManagedExporter(exporter, time.Second, 2)
	started := time.Now()
	for i := 0; i < 10; i++ {
		transport.Send(ExportTraceServiceRequest{})
	}
	if time.Since(started) >= 100*time.Millisecond {
		t.Fatal("custom exporter executed on or blocked the host path")
	}
	diagnostics := transport.Diagnostics()
	if diagnostics.QueueCapacity != 2 || diagnostics.AcceptedBatches > 3 || diagnostics.DroppedBatches < 7 {
		t.Fatalf("custom exporter queue was not bounded: %+v", diagnostics)
	}

	shortCtx, shortCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer shortCancel()
	shutdownStarted := time.Now()
	if transport.Shutdown(shortCtx) {
		t.Fatal("shutdown unexpectedly completed while customer export was blocked")
	}
	if time.Since(shutdownStarted) > 250*time.Millisecond {
		t.Fatal("shutdown did not honor its deadline")
	}

	close(exporter.gate)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if !transport.Shutdown(shutdownCtx) {
		t.Fatal("shutdown did not recover after the customer exporter unblocked")
	}
}

type lifecycleFailureExporter struct {
	flushCalls    atomic.Int64
	shutdownCalls atomic.Int64
}

func (*lifecycleFailureExporter) ExportTraces(context.Context, ExportTraceServiceRequest) error {
	return nil
}

func (*lifecycleFailureExporter) ExportLogs(context.Context, ExportLogsServiceRequest) error {
	return nil
}

func (e *lifecycleFailureExporter) Flush(context.Context) error {
	if e.flushCalls.Add(1) == 1 {
		panic("flush bug")
	}
	return nil
}

func (e *lifecycleFailureExporter) Shutdown(context.Context) error {
	e.shutdownCalls.Add(1)
	return errors.New("close failed")
}

func TestCustomExporterLifecycleFailuresReturnFalseAndRemainContained(t *testing.T) {
	exporter := &lifecycleFailureExporter{}
	rl := Init(Config{CustomExporter: exporter})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if rl.Flush(ctx) {
		t.Fatal("panicking custom flush must report failure")
	}
	if rl.Shutdown(ctx) {
		t.Fatal("failing custom shutdown must report failure")
	}
	if rl.Shutdown(ctx) {
		t.Fatal("repeated shutdown must preserve the first failure result")
	}
	if exporter.flushCalls.Load() != 2 || exporter.shutdownCalls.Load() != 1 {
		t.Fatalf("lifecycle hooks were not bounded/idempotent flush=%d shutdown=%d", exporter.flushCalls.Load(), exporter.shutdownCalls.Load())
	}
}

type legacyCaptureTransport struct {
	traces atomic.Int64
	logs   atomic.Int64
}

func (t *legacyCaptureTransport) Send(ExportTraceServiceRequest) { t.traces.Add(1) }
func (t *legacyCaptureTransport) SendLogs(ExportLogsServiceRequest) {
	t.logs.Add(1)
}

func TestLegacyCustomTransportFieldsRemainCompatible(t *testing.T) {
	legacy := &legacyCaptureTransport{}
	rl := Init(Config{CustomTransport: legacy, Logs: true, SampleRate: 1})
	ctx := rl.Tracer().Start(context.Background(), "GET /legacy", "")
	rl.Tracer().Finish(ctx)
	_ = rl.SlogHandler(nil).Handle(ctx, slog.NewRecord(time.Now(), slog.LevelWarn, "legacy", 0))
	if legacy.traces.Load() != 1 || legacy.logs.Load() != 1 {
		t.Fatalf("legacy transports stopped working traces=%d logs=%d", legacy.traces.Load(), legacy.logs.Load())
	}
}

func TestCustomExporterTakesPrecedenceOverLegacyTransportFields(t *testing.T) {
	exporter := &recordingExporter{}
	legacy := &legacyCaptureTransport{}
	rl := Init(Config{
		CustomExporter:      exporter,
		CustomTransport:     legacy,
		CustomLogsTransport: legacy,
		Logs:                true,
		SampleRate:          1,
	})
	ctx := rl.Tracer().Start(context.Background(), "GET /preferred", "")
	rl.Tracer().Finish(ctx)
	_ = rl.SlogHandler(nil).Handle(ctx, slog.NewRecord(time.Now(), slog.LevelWarn, "preferred", 0))
	flushCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if !rl.Shutdown(flushCtx) {
		t.Fatal("custom exporter did not shut down")
	}
	traces, logs, _ := exporter.snapshot()
	if len(traces) != 1 || len(logs) != 1 || legacy.traces.Load() != 0 || legacy.logs.Load() != 0 {
		t.Fatalf("custom exporter precedence failed unified=(%d,%d) legacy=(%d,%d)", len(traces), len(logs), legacy.traces.Load(), legacy.logs.Load())
	}
}
