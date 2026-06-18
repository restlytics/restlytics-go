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
	"time"
)

// Transport sends an assembled OTLP payload to the ingestion service.
//
// Implementations MUST be fire-and-forget and MUST NOT block the caller or
// panic into the host app. Send is expected to return immediately.
type Transport interface {
	Send(payload ExportTraceServiceRequest)
}

// HTTPTransport gzips the JSON body and POSTs it with net/http, off the request
// path in a background goroutine.
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
	url    string
	key    string
	client *http.Client
}

// NewHTTPTransport builds an HTTPTransport. timeout bounds the whole send
// (~2s by the contract); a zero/negative timeout falls back to 2s.
func NewHTTPTransport(ingestURL, key string, timeout time.Duration) *HTTPTransport {
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	url := strings.TrimRight(ingestURL, "/") + "/v1/traces"
	return &HTTPTransport{
		url: url,
		key: key,
		client: &http.Client{
			Timeout: timeout,
		},
	}
}

// Send fires the payload off in a background goroutine and returns immediately.
func (t *HTTPTransport) Send(payload ExportTraceServiceRequest) {
	if t.url == "" || t.key == "" {
		return
	}

	go func() {
		// Absolute backstop: nothing in the flush goroutine may ever propagate a
		// panic and crash the host process.
		defer func() {
			_ = recover()
		}()

		body, err := gzipJSON(payload)
		if err != nil {
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), t.client.Timeout)
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.url, bytes.NewReader(body))
		if err != nil {
			return
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Content-Encoding", "gzip")
		req.Header.Set("X-Restlytics-Key", t.key)

		resp, err := t.client.Do(req)
		if err != nil {
			// Degrade silently on timeout/connection errors — drop the batch.
			return
		}
		// Drain + close so the connection can be reused; response body is ignored
		// (any/no response is treated as success per the contract).
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
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
