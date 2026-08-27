package restlytics

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Transport sends an assembled OTLP payload to the ingestion service.
//
// Implementations MUST be fire-and-forget and MUST NOT block the caller or
// panic into the host app. Send is expected to return immediately.
type Transport interface {
	Send(payload ExportTraceServiceRequest)
}

// LogsTransport sends assembled OTLP log payloads. HTTPTransport implements
// both Transport and LogsTransport so both signals share one bounded queue and
// one fire-and-forget worker.
type LogsTransport interface {
	SendLogs(payload ExportLogsServiceRequest)
}

// Exporter is the stable provider-neutral custom export contract. Implement it
// to route the production-shaped OTLP/JSON payloads to an OpenTelemetry
// Collector, another observability backend, durable storage, or a test sink.
//
// Restlytics owns the fire-and-forget boundary: calls happen on one bounded
// worker queue, receive a context limited by Config.TimeoutMs, and never execute
// on the instrumented application path. Returned errors and panics are contained
// and counted as failed batches. An implementation should still honor ctx so a
// bounded Flush or Shutdown can complete promptly.
type Exporter interface {
	ExportTraces(ctx context.Context, payload ExportTraceServiceRequest) error
	ExportLogs(ctx context.Context, payload ExportLogsServiceRequest) error
}

// ExporterFlusher is an optional extension for exporters with their own buffer.
// It is called only after Restlytics has drained its accepted work. Flush errors,
// panics, and deadline overruns are contained and reported as a false result from
// Restlytics.Flush.
type ExporterFlusher interface {
	Flush(ctx context.Context) error
}

// ExporterShutdown is an optional extension for exporters that own resources.
// It is called at most once, after Restlytics has stopped accepting work and
// drained its queue. Errors and panics are contained.
type ExporterShutdown interface {
	Shutdown(ctx context.Context) error
}

// transportLifecycle is implemented by transports whose accepted work and
// resources can be bounded during process shutdown.
type transportLifecycle interface {
	Flush(context.Context) bool
	Shutdown(context.Context) bool
}

type diagnosticTransport interface {
	Diagnostics() TransportDiagnostics
}

type outboundBatch struct {
	url     string
	payload any
}

type exportSignal uint8

const (
	exportSignalTraces exportSignal = iota + 1
	exportSignalLogs
)

type customExportBatch struct {
	signal exportSignal
	traces ExportTraceServiceRequest
	logs   ExportLogsServiceRequest
}

// TransportDiagnostics is a payload-free snapshot suitable for health checks and
// shutdown logs. Counts are process-local and monotonic.
type TransportDiagnostics struct {
	AcceptedBatches  int64
	DeliveredBatches int64
	DroppedBatches   int64
	FailedBatches    int64
	QueuedBatches    int
	InFlightBatches  int64
	QueueCapacity    int
	Closed           bool
}

// managedExporter adapts the public Exporter contract to the original internal
// signal transports. It deliberately mirrors HTTPTransport's single-worker,
// bounded, non-blocking delivery behavior.
type managedExporter struct {
	exporter  Exporter
	timeout   time.Duration
	queue     chan customExportBatch
	done      chan struct{}
	stopped   chan struct{}
	acceptMu  sync.Mutex
	stopOnce  sync.Once
	closeOnce sync.Once
	closeDone chan struct{}
	closed    atomic.Bool
	closeOK   atomic.Bool
	pending   atomic.Int64
	inFlight  atomic.Int64
	accepted  atomic.Int64
	delivered atomic.Int64
	dropped   atomic.Int64
	failed    atomic.Int64
}

func newManagedExporter(exporter Exporter, timeout time.Duration, queueCapacity int) *managedExporter {
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	if queueCapacity <= 0 {
		queueCapacity = 64
	}
	t := &managedExporter{
		exporter:  exporter,
		timeout:   timeout,
		queue:     make(chan customExportBatch, queueCapacity),
		done:      make(chan struct{}),
		stopped:   make(chan struct{}),
		closeDone: make(chan struct{}),
	}
	t.closeOK.Store(true)
	go t.run()
	return t
}

// Send performs only a bounded enqueue; customer code never runs here.
func (t *managedExporter) Send(payload ExportTraceServiceRequest) {
	t.enqueue(customExportBatch{signal: exportSignalTraces, traces: payload})
}

// SendLogs performs only a bounded enqueue; customer code never runs here.
func (t *managedExporter) SendLogs(payload ExportLogsServiceRequest) {
	t.enqueue(customExportBatch{signal: exportSignalLogs, logs: payload})
}

func (t *managedExporter) enqueue(batch customExportBatch) {
	t.acceptMu.Lock()
	defer t.acceptMu.Unlock()
	if t.closed.Load() || t.exporter == nil {
		t.dropped.Add(1)
		return
	}
	t.pending.Add(1)
	select {
	case t.queue <- batch:
		t.accepted.Add(1)
	default:
		t.pending.Add(-1)
		t.dropped.Add(1)
	}
}

// Diagnostics returns payload-free delivery counters for the custom exporter.
func (t *managedExporter) Diagnostics() TransportDiagnostics {
	return TransportDiagnostics{
		AcceptedBatches:  t.accepted.Load(),
		DeliveredBatches: t.delivered.Load(),
		DroppedBatches:   t.dropped.Load(),
		FailedBatches:    t.failed.Load(),
		QueuedBatches:    len(t.queue),
		InFlightBatches:  t.inFlight.Load(),
		QueueCapacity:    cap(t.queue),
		Closed:           t.closed.Load(),
	}
}

// Flush drains accepted Restlytics work and then invokes the optional exporter
// flush hook. Both phases obey ctx.
func (t *managedExporter) Flush(ctx context.Context) bool {
	if ctx == nil {
		ctx = context.Background()
	}
	if !waitForPending(ctx, &t.pending) {
		return false
	}
	flusher, ok := t.exporter.(ExporterFlusher)
	if !ok {
		return true
	}
	return callExporterHook(ctx, flusher.Flush)
}

// Shutdown stops acceptance, drains queued work, stops the Restlytics worker,
// and invokes the optional exporter shutdown hook. It is idempotent.
func (t *managedExporter) Shutdown(ctx context.Context) bool {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-t.stopped:
		return t.shutdownExporter(ctx)
	default:
	}
	t.acceptMu.Lock()
	t.closed.Store(true)
	t.acceptMu.Unlock()
	if !t.Flush(ctx) {
		return false
	}
	t.stopOnce.Do(func() { close(t.done) })
	select {
	case <-t.stopped:
	case <-ctx.Done():
		return false
	}
	return t.shutdownExporter(ctx)
}

func (t *managedExporter) shutdownExporter(ctx context.Context) bool {
	shutdown, ok := t.exporter.(ExporterShutdown)
	if !ok {
		return true
	}
	t.closeOnce.Do(func() {
		t.closeOK.Store(callExporterHook(ctx, shutdown.Shutdown))
		close(t.closeDone)
	})
	select {
	case <-t.closeDone:
		return t.closeOK.Load()
	case <-ctx.Done():
		return false
	}
}

func (t *managedExporter) run() {
	defer close(t.stopped)
	for {
		select {
		case batch := <-t.queue:
			t.inFlight.Store(1)
			if t.export(batch) {
				t.delivered.Add(1)
			} else {
				t.failed.Add(1)
			}
			t.inFlight.Store(0)
			t.pending.Add(-1)
		case <-t.done:
			return
		}
	}
}

func (t *managedExporter) export(batch customExportBatch) (delivered bool) {
	defer func() {
		if recover() != nil {
			delivered = false
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), t.timeout)
	defer cancel()
	switch batch.signal {
	case exportSignalTraces:
		return t.exporter.ExportTraces(ctx, batch.traces) == nil
	case exportSignalLogs:
		return t.exporter.ExportLogs(ctx, batch.logs) == nil
	default:
		return false
	}
}

func waitForPending(ctx context.Context, pending *atomic.Int64) bool {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if pending.Load() == 0 {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
		}
	}
}

func callExporterHook(ctx context.Context, hook func(context.Context) error) bool {
	result := make(chan bool, 1)
	go func() {
		ok := false
		defer func() {
			_ = recover()
			result <- ok
		}()
		ok = hook(ctx) == nil
	}()
	select {
	case ok := <-result:
		return ok
	case <-ctx.Done():
		return false
	}
}

// HTTPTransport gzips the JSON body and POSTs it with net/http using one worker
// goroutine and a bounded queue off the request path.
//
// Wire format (must match the ingestion contract exactly):
//
//	POST {ingestURL}/v1/traces or /v1/logs
//	X-Restlytics-Key: {key}
//	Content-Type: application/json
//	Content-Encoding: gzip
//	body = gzip(json)
//
// Every error path is swallowed — telemetry must never hurt the host app.
type HTTPTransport struct {
	tracesURL string
	logsURL   string
	key       string
	client    *http.Client
	queue     chan outboundBatch
	done      chan struct{}
	stopped   chan struct{}
	acceptMu  sync.Mutex
	stopOnce  sync.Once
	closed    atomic.Bool
	pending   atomic.Int64
	inFlight  atomic.Int64
	accepted  atomic.Int64
	delivered atomic.Int64
	dropped   atomic.Int64
	failed    atomic.Int64
}

// NewHTTPTransport builds an HTTPTransport. timeout bounds the whole send
// (~2s by the contract); a zero/negative timeout falls back to 2s.
func NewHTTPTransport(ingestURL, key string, timeout time.Duration) *HTTPTransport {
	return newHTTPTransport(ingestURL, key, timeout, 64)
}

func newHTTPTransport(ingestURL, key string, timeout time.Duration, queueCapacity int) *HTTPTransport {
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	if queueCapacity <= 0 {
		queueCapacity = 64
	}
	baseURL := strings.TrimRight(ingestURL, "/")
	t := &HTTPTransport{
		tracesURL: baseURL + "/v1/traces",
		logsURL:   baseURL + "/v1/logs",
		key:       key,
		client: &http.Client{
			Timeout: timeout,
		},
		queue:   make(chan outboundBatch, queueCapacity),
		done:    make(chan struct{}),
		stopped: make(chan struct{}),
	}
	go t.run()
	return t
}

// Send performs only a bounded non-blocking enqueue and returns immediately.
func (t *HTTPTransport) Send(payload ExportTraceServiceRequest) {
	t.enqueue(outboundBatch{url: t.tracesURL, payload: payload})
}

// SendLogs performs the same bounded non-blocking enqueue as Send, targeting
// the OTLP/HTTP logs endpoint.
func (t *HTTPTransport) SendLogs(payload ExportLogsServiceRequest) {
	t.enqueue(outboundBatch{url: t.logsURL, payload: payload})
}

func (t *HTTPTransport) enqueue(batch outboundBatch) {
	t.acceptMu.Lock()
	defer t.acceptMu.Unlock()
	if t.closed.Load() || batch.url == "" || t.key == "" {
		t.dropped.Add(1)
		return
	}

	t.pending.Add(1)
	select {
	case t.queue <- batch:
		t.accepted.Add(1)
	default:
		t.pending.Add(-1)
		t.dropped.Add(1)
	}
}

// Diagnostics returns an atomic delivery-health snapshot without payload data.
func (t *HTTPTransport) Diagnostics() TransportDiagnostics {
	return TransportDiagnostics{
		AcceptedBatches:  t.accepted.Load(),
		DeliveredBatches: t.delivered.Load(),
		DroppedBatches:   t.dropped.Load(),
		FailedBatches:    t.failed.Load(),
		QueuedBatches:    len(t.queue),
		InFlightBatches:  t.inFlight.Load(),
		QueueCapacity:    cap(t.queue),
		Closed:           t.closed.Load(),
	}
}

// Flush waits for already-accepted batches without accepting unbounded work.
func (t *HTTPTransport) Flush(ctx context.Context) bool {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if t.pending.Load() == 0 {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
		}
	}
}

// Shutdown stops new work, flushes accepted batches, and releases the worker.
func (t *HTTPTransport) Shutdown(ctx context.Context) bool {
	t.acceptMu.Lock()
	t.closed.Store(true)
	t.acceptMu.Unlock()
	if !t.Flush(ctx) {
		return false
	}
	t.stopOnce.Do(func() { close(t.done) })
	select {
	case <-t.stopped:
		return true
	case <-ctx.Done():
		return false
	}
}

func (t *HTTPTransport) run() {
	defer close(t.stopped)
	for {
		select {
		case batch := <-t.queue:
			t.inFlight.Store(1)
			if t.post(batch) {
				t.delivered.Add(1)
			} else {
				t.failed.Add(1)
			}
			t.inFlight.Store(0)
			t.pending.Add(-1)
		case <-t.done:
			return
		}
	}
}

func (t *HTTPTransport) post(batch outboundBatch) (delivered bool) {
	defer func() {
		if recover() != nil {
			delivered = false
		}
	}()
	body, err := gzipJSON(batch.payload)
	if err != nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), t.client.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, batch.url, bytes.NewReader(body))
	if err != nil {
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")
	req.Header.Set("X-Restlytics-Key", t.key)
	resp, err := t.client.Do(req)
	if err != nil {
		return false
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return true
}

func gzipJSON(payload any) ([]byte, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	gw, err := gzip.NewWriterLevel(&buf, gzip.DefaultCompression)
	if err != nil {
		return nil, err
	}
	if _, err := gw.Write(raw); err != nil {
		_ = gw.Close()
		return nil, err
	}
	if err := gw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// NullTransport discards everything. Useful for tests or to disable delivery
// while keeping instrumentation.
type NullTransport struct{}

// Send does nothing.
func (NullTransport) Send(ExportTraceServiceRequest) {}

// SendLogs does nothing.
func (NullTransport) SendLogs(ExportLogsServiceRequest) {}

// LogTransport writes the JSON payload to a logger (local debugging). It marshals
// synchronously but swallows errors and never panics.
type LogTransport struct {
	Logger *log.Logger
}

// Send logs the payload as pretty JSON.
func (t LogTransport) Send(payload ExportTraceServiceRequest) {
	t.logPayload(payload)
}

// SendLogs logs a logs payload for local debugging.
func (t LogTransport) SendLogs(payload ExportLogsServiceRequest) {
	t.logPayload(payload)
}

func (t LogTransport) logPayload(payload any) {
	defer func() { _ = recover() }()
	raw, err := json.Marshal(payload)
	if err != nil {
		return
	}
	if t.Logger != nil {
		t.Logger.Printf("restlytics: %s", raw)
		return
	}
	log.Printf("restlytics: %s", raw)
}

// TelemetryPreview is a local-only description of the production-shaped batch.
type TelemetryPreview struct {
	Mode                   string                    `json:"mode"`
	NetworkRequestMade     bool                      `json:"networkRequestMade"`
	Signal                 string                    `json:"signal"`
	ConfiguredSampleRate   float64                   `json:"configuredSampleRate"`
	Sampled                bool                      `json:"sampled"`
	SpanCount              int                       `json:"spanCount"`
	JSONBytes              int                       `json:"jsonBytes"`
	GzipBytes              int                       `json:"gzipBytes"`
	RedactionPolicyApplied []string                  `json:"redactionPolicyApplied"`
	Payload                ExportTraceServiceRequest `json:"payload"`
}

// PreviewTransport emits a structured report locally and never opens a socket.
// It is safe for concurrent requests and is selected with
// RESTLYTICS_TRANSPORT=preview.
type PreviewTransport struct {
	SampleRate float64
	Writer     io.Writer
	mu         sync.Mutex
	Reports    []TelemetryPreview
	LogReports []LogTelemetryPreview
}

// LogTelemetryPreview is the local-only counterpart to TelemetryPreview for
// production-shaped OTLP log batches.
type LogTelemetryPreview struct {
	Mode                   string                   `json:"mode"`
	NetworkRequestMade     bool                     `json:"networkRequestMade"`
	Signal                 string                   `json:"signal"`
	RecordCount            int                      `json:"recordCount"`
	JSONBytes              int                      `json:"jsonBytes"`
	GzipBytes              int                      `json:"gzipBytes"`
	RedactionPolicyApplied []string                 `json:"redactionPolicyApplied"`
	Payload                ExportLogsServiceRequest `json:"payload"`
}

// NewPreviewTransport creates a local-only preview transport.
func NewPreviewTransport(sampleRate float64, writer io.Writer) *PreviewTransport {
	return &PreviewTransport{SampleRate: sampleRate, Writer: writer}
}

// Send creates the report synchronously, swallowing every failure.
func (t *PreviewTransport) Send(payload ExportTraceServiceRequest) {
	defer func() { _ = recover() }()
	raw, err := json.Marshal(payload)
	if err != nil {
		return
	}
	gzipped, err := gzipJSON(payload)
	if err != nil {
		return
	}
	spanCount := 0
	for _, resource := range payload.ResourceSpans {
		for _, scope := range resource.ScopeSpans {
			spanCount += len(scope.Spans)
		}
	}
	report := TelemetryPreview{
		Mode:                 "preview",
		NetworkRequestMade:   false,
		Signal:               "traces",
		ConfiguredSampleRate: t.SampleRate,
		Sampled:              true,
		SpanCount:            spanCount,
		JSONBytes:            len(raw),
		GzipBytes:            len(gzipped),
		RedactionPolicyApplied: []string{
			"url query values and URL credentials",
			"sensitive headers and credentials",
			"request and response bodies",
			"exception messages and stack traces",
			"SQL binding values",
		},
		Payload: payload,
	}
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return
	}
	t.mu.Lock()
	t.Reports = append(t.Reports, report)
	if len(t.Reports) > 16 {
		t.Reports = append([]TelemetryPreview(nil), t.Reports[len(t.Reports)-16:]...)
	}
	writer := t.Writer
	if writer != nil {
		_, _ = fmt.Fprintln(writer, string(encoded))
	} else {
		log.Printf("restlytics preview: %s", encoded)
	}
	t.mu.Unlock()
}

// SendLogs records a bounded local preview without opening a socket.
func (t *PreviewTransport) SendLogs(payload ExportLogsServiceRequest) {
	defer func() { _ = recover() }()
	raw, err := json.Marshal(payload)
	if err != nil {
		return
	}
	gzipped, err := gzipJSON(payload)
	if err != nil {
		return
	}
	recordCount := 0
	for _, resource := range payload.ResourceLogs {
		for _, scope := range resource.ScopeLogs {
			recordCount += len(scope.LogRecords)
		}
	}
	report := LogTelemetryPreview{
		Mode:               "preview",
		NetworkRequestMade: false,
		Signal:             "logs",
		RecordCount:        recordCount,
		JSONBytes:          len(raw),
		GzipBytes:          len(gzipped),
		RedactionPolicyApplied: []string{
			"credentials and personal data in log text",
			"URL credentials, fragments, and query values",
			"sensitive structured attributes",
			"bounded body and attribute content",
		},
		Payload: payload,
	}
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return
	}
	t.mu.Lock()
	t.LogReports = append(t.LogReports, report)
	if len(t.LogReports) > 16 {
		t.LogReports = append([]LogTelemetryPreview(nil), t.LogReports[len(t.LogReports)-16:]...)
	}
	writer := t.Writer
	if writer != nil {
		_, _ = fmt.Fprintln(writer, string(encoded))
	} else {
		log.Printf("restlytics preview: %s", encoded)
	}
	t.mu.Unlock()
}

// transportFromConfig picks a Transport from a resolved Config.
func transportFromConfig(c Config) Transport {
	if c.CustomExporter != nil {
		return newManagedExporter(c.CustomExporter, time.Duration(c.TimeoutMs)*time.Millisecond, 64)
	}
	if c.CustomTransport != nil {
		return c.CustomTransport
	}
	switch strings.ToLower(c.Transport) {
	case "null":
		return NullTransport{}
	case "log":
		return LogTransport{}
	case "preview":
		return NewPreviewTransport(c.SampleRate, nil)
	default: // "http", "curl", anything else
		return NewHTTPTransport(c.IngestURL, c.Key, time.Duration(c.TimeoutMs)*time.Millisecond)
	}
}

func logsTransportFromConfig(c Config, traceTransport Transport) LogsTransport {
	if c.CustomExporter != nil {
		if transport, ok := traceTransport.(LogsTransport); ok {
			return transport
		}
		return NullTransport{}
	}
	if c.CustomLogsTransport != nil {
		return c.CustomLogsTransport
	}
	if transport, ok := traceTransport.(LogsTransport); ok {
		return transport
	}
	return NullTransport{}
}
