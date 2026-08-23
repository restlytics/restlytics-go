package restlytics

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
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

// HTTPTransport gzips the JSON body and POSTs it with net/http using one worker
// goroutine and a bounded queue off the request path.
//
// Wire format (must match the ingestion contract exactly):
//
//	POST {ingestURL}/v1/traces
//	X-Restlytics-Key: {key}
//	Content-Type: application/json
//	Content-Encoding: gzip
//	body = gzip(json)
//
// Every error path is swallowed — telemetry must never hurt the host app.
type HTTPTransport struct {
	url       string
	key       string
	client    *http.Client
	queue     chan ExportTraceServiceRequest
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
	url := strings.TrimRight(ingestURL, "/") + "/v1/traces"
	t := &HTTPTransport{
		url: url,
		key: key,
		client: &http.Client{
			Timeout: timeout,
		},
		queue:   make(chan ExportTraceServiceRequest, queueCapacity),
		done:    make(chan struct{}),
		stopped: make(chan struct{}),
	}
	go t.run()
	return t
}

// Send performs only a bounded non-blocking enqueue and returns immediately.
func (t *HTTPTransport) Send(payload ExportTraceServiceRequest) {
	t.acceptMu.Lock()
	defer t.acceptMu.Unlock()
	if t.closed.Load() || t.url == "" || t.key == "" {
		t.dropped.Add(1)
		return
	}

	t.pending.Add(1)
	select {
	case t.queue <- payload:
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
		case payload := <-t.queue:
			t.inFlight.Store(1)
			if t.post(payload) {
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

func (t *HTTPTransport) post(payload ExportTraceServiceRequest) (delivered bool) {
	defer func() {
		if recover() != nil {
			delivered = false
		}
	}()
	body, err := gzipJSON(payload)
	if err != nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), t.client.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.url, bytes.NewReader(body))
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

func gzipJSON(payload ExportTraceServiceRequest) ([]byte, error) {
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

// LogTransport writes the JSON payload to a logger (local debugging). It marshals
// synchronously but swallows errors and never panics.
type LogTransport struct {
	Logger *log.Logger
}

// Send logs the payload as pretty JSON.
func (t LogTransport) Send(payload ExportTraceServiceRequest) {
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

// transportFromConfig picks a Transport from a resolved Config.
func transportFromConfig(c Config) Transport {
	if c.CustomTransport != nil {
		return c.CustomTransport
	}
	switch strings.ToLower(c.Transport) {
	case "null":
		return NullTransport{}
	case "log":
		return LogTransport{}
	default: // "http", "curl", anything else
		return NewHTTPTransport(c.IngestURL, c.Key, time.Duration(c.TimeoutMs)*time.Millisecond)
	}
}
