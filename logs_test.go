package restlytics

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type captureLogsTransport struct {
	mu       sync.Mutex
	payloads []ExportLogsServiceRequest
}

func (t *captureLogsTransport) SendLogs(payload ExportLogsServiceRequest) {
	t.mu.Lock()
	t.payloads = append(t.payloads, payload)
	t.mu.Unlock()
}

func (t *captureLogsTransport) records() []LogRecordData {
	t.mu.Lock()
	defer t.mu.Unlock()
	var records []LogRecordData
	for _, payload := range t.payloads {
		for _, resource := range payload.ResourceLogs {
			for _, scope := range resource.ScopeLogs {
				records = append(records, scope.LogRecords...)
			}
		}
	}
	return records
}

type panicLogsTransport struct{}

func (panicLogsTransport) SendLogs(ExportLogsServiceRequest) { panic("transport failed") }

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

type recordingHandler struct {
	mu      sync.Mutex
	records []slog.Record
	err     error
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *recordingHandler) Handle(_ context.Context, record slog.Record) error {
	h.mu.Lock()
	h.records = append(h.records, record)
	h.mu.Unlock()
	return h.err
}
func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordingHandler) WithGroup(string) slog.Handler      { return h }

func testLogRestlytics(transport LogsTransport, minimum int) *Restlytics {
	cfg := Config{
		Key:             "rl_test",
		ServiceName:     "checkout",
		Environment:     "test",
		SampleRate:      1,
		MaxSpans:        defaultMaxSpans,
		Logs:            true,
		LogsMinSeverity: minimum,
	}
	return &Restlytics{
		tracer:        NewTracer(cfg, NullTransport{}),
		logsTransport: transport,
		cfg:           cfg,
	}
}

func TestLogsConfigIsOptInAndDefaultsToWarn(t *testing.T) {
	t.Setenv("RESTLYTICS_LOGS", "")
	t.Setenv("RESTLYTICS_LOGS_MIN_SEVERITY", "")
	resolved := (Config{}).Resolve()
	if resolved.Logs || resolved.LogsMinSeverity != SeverityWarn {
		t.Fatalf("unexpected logs defaults: enabled=%t minimum=%d", resolved.Logs, resolved.LogsMinSeverity)
	}
	t.Setenv("RESTLYTICS_LOGS", "true")
	t.Setenv("RESTLYTICS_LOGS_MIN_SEVERITY", "17")
	resolved = (Config{}).Resolve()
	if !resolved.Logs || resolved.LogsMinSeverity != SeverityError {
		t.Fatalf("environment was not resolved: enabled=%t minimum=%d", resolved.Logs, resolved.LogsMinSeverity)
	}
}

func TestSlogHandlerDoesNotExportUntilLogsAreEnabled(t *testing.T) {
	capture := &captureLogsTransport{}
	rl := Init(Config{
		Key:                 "rl_test",
		CustomTransport:     NullTransport{},
		CustomLogsTransport: capture,
	})
	logger := slog.New(rl.SlogHandler(nil))
	logger.Error("must remain local")
	if records := capture.records(); len(records) != 0 {
		t.Fatalf("disabled log export captured %d records", len(records))
	}
}

func TestSlogHandlerExportsAndDelegates(t *testing.T) {
	capture := &captureLogsTransport{}
	next := &recordingHandler{}
	handler := testLogRestlytics(capture, SeverityWarn).SlogHandler(next)
	record := slog.NewRecord(time.Unix(1700000000, 123), slog.LevelWarn, "checkout slow", 0)
	record.AddAttrs(slog.String("order.id", "ord_123"))
	if err := handler.Handle(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	records := capture.records()
	if len(records) != 1 || records[0].SeverityNumber != SeverityWarn || records[0].SeverityText != "WARN" {
		t.Fatalf("unexpected exported records: %#v", records)
	}
	if records[0].TimeUnixNano != "1700000000000000123" {
		t.Fatalf("timeUnixNano must preserve event time as a string: %#v", records[0])
	}
	if len(next.records) != 1 || next.records[0].Message != "checkout slow" {
		t.Fatal("original record was not delegated unchanged")
	}
}

func TestSlogHandlerPreservesDownstreamAttrsAndGroups(t *testing.T) {
	var output bytes.Buffer
	next := slog.NewJSONHandler(&output, nil)
	logger := slog.New(
		testLogRestlytics(&captureLogsTransport{}, SeverityWarn).
			SlogHandler(next).
			WithGroup("request").
			WithAttrs([]slog.Attr{slog.String("route", "/checkout")}),
	)
	logger.Warn("slow", "status", 503)
	encoded := output.String()
	for _, want := range []string{`"request"`, `"route":"/checkout"`, `"status":503`} {
		if !strings.Contains(encoded, want) {
			t.Fatalf("downstream handler lost %s: %s", want, encoded)
		}
	}
}

func TestSlogHandlerSwallowsCaptureFailureButPreservesDelegateResult(t *testing.T) {
	wantErr := errors.New("application handler failure")
	next := &recordingHandler{err: wantErr}
	handler := testLogRestlytics(panicLogsTransport{}, SeverityWarn).SlogHandler(next)
	record := slog.NewRecord(time.Now(), slog.LevelError, "failed", 0)
	if got := handler.Handle(context.Background(), record); !errors.Is(got, wantErr) {
		t.Fatalf("capture failure changed delegate result: %v", got)
	}
	if len(next.records) != 1 {
		t.Fatal("capture panic prevented delegation")
	}
}

func TestSlogSeverityAndThresholdAreDeterministic(t *testing.T) {
	tests := []struct {
		level  slog.Level
		number int
		text   string
	}{
		{slog.LevelDebug, SeverityDebug, "DEBUG"},
		{slog.LevelInfo, SeverityInfo, "INFO"},
		{slog.LevelWarn, SeverityWarn, "WARN"},
		{slog.LevelError, SeverityError, "ERROR"},
		{slog.LevelError + 4, SeverityError2, "ERROR2"},
		{slog.LevelError + 8, SeverityFatal, "FATAL"},
	}
	for _, test := range tests {
		number, text := SlogSeverity(test.level)
		if number != test.number || text != test.text {
			t.Fatalf("level %v mapped to (%d, %q), want (%d, %q)", test.level, number, text, test.number, test.text)
		}
	}

	capture := &captureLogsTransport{}
	logger := slog.New(testLogRestlytics(capture, SeverityWarn).SlogHandler(nil))
	logger.Debug("debug")
	logger.Info("info")
	logger.Warn("warn")
	logger.Error("error")
	records := capture.records()
	if len(records) != 2 || records[0].SeverityNumber != SeverityWarn || records[1].SeverityNumber != SeverityError {
		t.Fatalf("threshold did not retain only WARN+: %#v", records)
	}
}

func TestSlogHandlerCorrelatesSampledAndUnsampledTraces(t *testing.T) {
	for _, sampled := range []bool{true, false} {
		t.Run(map[bool]string{true: "sampled", false: "unsampled"}[sampled], func(t *testing.T) {
			capture := &captureLogsTransport{}
			rl := testLogRestlytics(capture, SeverityWarn)
			if !sampled {
				rl.cfg.SampleRate = 0
				rl.tracer = NewTracer(rl.cfg, NullTransport{})
			}
			ctx := rl.Tracer().Start(context.Background(), "GET /checkout", "")
			record := slog.NewRecord(time.Now(), slog.LevelError, "correlated", 0)
			if err := rl.SlogHandler(nil).Handle(ctx, record); err != nil {
				t.Fatal(err)
			}
			records := capture.records()
			if len(records) != 1 {
				t.Fatalf("records: %d", len(records))
			}
			got := records[0]
			if got.TraceID != CurrentTraceID(ctx) || got.SpanID != CurrentSpanID(ctx) {
				t.Fatalf("missing ambient correlation: %#v", got)
			}
			wantFlags := 0
			if sampled {
				wantFlags = 1
			}
			if got.Flags == nil || *got.Flags != wantFlags {
				t.Fatalf("flags: %v, want %d", got.Flags, wantFlags)
			}
		})
	}

	capture := &captureLogsTransport{}
	rl := testLogRestlytics(capture, SeverityWarn)
	record := slog.NewRecord(time.Now(), slog.LevelWarn, "boot", 0)
	_ = rl.SlogHandler(nil).Handle(context.Background(), record)
	outside := capture.records()[0]
	if outside.TraceID != "" || outside.SpanID != "" || outside.Flags != nil {
		t.Fatalf("outside log must omit correlation ids and flags: %#v", outside)
	}
}

func TestSlogHandlerRedactsContentAndBoundsAttributes(t *testing.T) {
	capture := &captureLogsTransport{}
	handler := testLogRestlytics(capture, SeverityWarn).SlogHandler(nil).WithGroup("checkout").WithAttrs([]slog.Attr{
		slog.String("url", "https://alice:password@example.test/pay?unknown=customer-secret#raw"),
		slog.String("http.request.header.authorization", "Bearer attr.secret.token"),
		slog.Any("err", errors.New("card 4111111111111111")),
	})
	record := slog.NewRecord(time.Now(), slog.LevelError, strings.Join([]string{
		"email=alice@example.test",
		"Authorization: Bearer abc.def.ghi",
		"password=hunter2",
		"https://example.test/reset?unknown=customer-secret#raw",
		"request body=card=4111111111111111",
		"bindings=[alice@example.test,hunter2]",
		"-----BEGIN RSA PRIVATE KEY-----private-canary-----END RSA PRIVATE KEY-----",
	}, "\n"), 0)
	record.AddAttrs(
		slog.String("customer.note", "alice@example.test password=hunter2"),
		slog.String("request.body", "card=4111111111111111"),
	)
	if err := handler.Handle(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(capture.payloads[0])
	if err != nil {
		t.Fatal(err)
	}
	encoded := string(raw)
	for _, canary := range []string{
		"alice@example.test", "hunter2", "abc.def.ghi", "customer-secret",
		"4111111111111111", "private-canary", "alice:",
		"http.request.header.authorization", "request.body",
	} {
		if strings.Contains(strings.ToLower(encoded), strings.ToLower(canary)) {
			t.Fatalf("log payload leaked %q: %s", canary, encoded)
		}
	}
	if !strings.Contains(encoded, `"checkout.customer.note"`) || !strings.Contains(encoded, "[REDACTED]") {
		t.Fatalf("safe structured attribute missing or not redacted: %s", encoded)
	}
}

func TestSlogHandlerBoundsPayloadContent(t *testing.T) {
	capture := &captureLogsTransport{}
	record := slog.NewRecord(time.Now(), slog.LevelWarn, strings.Repeat("m", maxLogBodyBytes+100), 0)
	for i := 0; i < maxLogAttributes+6; i++ {
		record.AddAttrs(slog.String("safe."+strings.Repeat("k", 3)+string(rune('a'+i%26)), strings.Repeat("v", maxLogAttrBytes+100)))
	}
	if err := testLogRestlytics(capture, SeverityWarn).SlogHandler(nil).Handle(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	got := capture.records()[0]
	if got.Body.StringValue == nil || len(*got.Body.StringValue) != maxLogBodyBytes {
		t.Fatalf("body was not bounded: %#v", got.Body)
	}
	if len(got.Attributes) != maxLogAttributes || got.DroppedAttributesCount != 6 {
		t.Fatalf("attributes were not bounded: kept=%d dropped=%d", len(got.Attributes), got.DroppedAttributesCount)
	}
	for _, attr := range got.Attributes {
		if attr.Value.StringValue == nil || len(*attr.Value.StringValue) != maxLogAttrBytes {
			t.Fatalf("attribute value was not bounded: %#v", attr)
		}
	}
}

func TestHTTPTransportExportsLogsAsGzippedOTLPJSON(t *testing.T) {
	requests := make(chan ExportLogsServiceRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/logs" {
			t.Errorf("path: %s", request.URL.Path)
		}
		if request.Header.Get("X-Restlytics-Key") != "rl_test" || request.Header.Get("Content-Encoding") != "gzip" {
			t.Errorf("headers: %#v", request.Header)
		}
		reader, err := gzip.NewReader(request.Body)
		if err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		raw, err := io.ReadAll(reader)
		_ = reader.Close()
		if err != nil {
			t.Error(err)
			return
		}
		var payload ExportLogsServiceRequest
		if err := json.Unmarshal(raw, &payload); err != nil {
			t.Error(err)
			return
		}
		requests <- payload
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	transport := NewHTTPTransport(server.URL, "rl_test", time.Second)
	payload := BuildLogsPayload("checkout", "test", []LogRecordData{{
		TimeUnixNano:   "1700000000000000000",
		SeverityNumber: SeverityWarn,
		Body:           StringValue("slow"),
	}})
	transport.SendLogs(payload)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if !transport.Shutdown(ctx) {
		t.Fatal("logs transport did not flush")
	}
	select {
	case got := <-requests:
		if len(got.ResourceLogs) != 1 || got.ResourceLogs[0].ScopeLogs[0].LogRecords[0].SeverityNumber != SeverityWarn {
			t.Fatalf("unexpected OTLP logs body: %#v", got)
		}
	default:
		t.Fatal("server did not receive log payload")
	}
}

func TestHTTPTransportLogFailureIsSwallowedAndCounted(t *testing.T) {
	transport := NewHTTPTransport("https://ingest.example.test", "rl_test", 20*time.Millisecond)
	transport.client.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("injected network failure")
	})
	start := time.Now()
	transport.SendLogs(ExportLogsServiceRequest{})
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("SendLogs blocked on network delivery")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if !transport.Flush(ctx) {
		t.Fatal("failed log delivery did not settle")
	}
	if diagnostics := transport.Diagnostics(); diagnostics.FailedBatches != 1 {
		t.Fatalf("transport failure not counted: %+v", diagnostics)
	}
	if !transport.Shutdown(ctx) {
		t.Fatal("shutdown failed after transport failure")
	}
}

func TestHTTPTransportLogBufferIsBounded(t *testing.T) {
	gate := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-gate
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	transport := newHTTPTransport(server.URL, "rl_test", time.Second, 2)

	for i := 0; i < 8; i++ {
		transport.SendLogs(ExportLogsServiceRequest{})
	}
	snapshot := transport.Diagnostics()
	if snapshot.QueueCapacity != 2 || snapshot.AcceptedBatches > 3 || snapshot.DroppedBatches < 5 {
		close(gate)
		t.Fatalf("unexpected bounded-queue diagnostics: %+v", snapshot)
	}
	close(gate)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if !transport.Shutdown(ctx) {
		t.Fatal("bounded log queue did not flush")
	}
}

func TestBuildLogsPayloadReusesTraceResource(t *testing.T) {
	logs := BuildLogsPayload("checkout", "test", nil)
	traces := BuildPayload("checkout", "test", nil)
	logResource, _ := json.Marshal(logs.ResourceLogs[0].Resource)
	traceResource, _ := json.Marshal(traces.ResourceSpans[0].Resource)
	if !bytes.Equal(logResource, traceResource) {
		t.Fatalf("resource blocks differ\nlogs: %s\ntraces: %s", logResource, traceResource)
	}
}
