package restlytics

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

type captureRoundTripper struct {
	traceparent string
}

func (c *captureRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	c.traceparent = request.Header.Get("traceparent")
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader("ok")),
		Header:     make(http.Header),
		Request:    request,
	}, nil
}

func TestHTTPRoundTripperInjectsRecordedClientContext(t *testing.T) {
	capture := &captureTransport{}
	cfg := Config{
		Key:             "rk_test",
		ServiceName:     "go-test",
		Environment:     "test",
		SampleRate:      1,
		MaxSpans:        100,
		InstrumentHTTP:  boolPtr(true),
		CustomTransport: capture,
	}.Resolve()
	rl := &Restlytics{tracer: NewTracer(cfg, capture), cfg: cfg}
	ctx := rl.tracer.Start(context.Background(), "GET /proxy", sampledParent)
	downstream := &captureRoundTripper{}
	client := &http.Client{Transport: rl.HTTPRoundTripper(downstream)}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.example.test/orders?token=secret", nil)
	if err != nil {
		t.Fatal(err)
	}

	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	rl.tracer.Finish(ctx)

	if !strings.HasPrefix(downstream.traceparent, "00-4bf92f3577b34da6a3ce929d0e0e4736-") ||
		!strings.HasSuffix(downstream.traceparent, "-01") {
		t.Fatalf("traceparent = %q", downstream.traceparent)
	}
	spans := capture.payloads[0].ResourceSpans[0].ScopeSpans[0].Spans
	if len(spans) != 2 || spans[1].SpanID != strings.Split(downstream.traceparent, "-")[2] {
		t.Fatalf("client span does not match propagated context: %#v", spans)
	}
	if spans[1].ParentSpanID != spans[0].SpanID {
		t.Fatalf("client parent = %q, root = %q", spans[1].ParentSpanID, spans[0].SpanID)
	}
}

func TestHTTPRoundTripperPropagatesUnsampledContextWithoutRecording(t *testing.T) {
	capture := &captureTransport{}
	cfg := Config{
		Key:             "rk_test",
		ServiceName:     "go-test",
		Environment:     "test",
		SampleRate:      1,
		MaxSpans:        100,
		InstrumentHTTP:  boolPtr(true),
		CustomTransport: capture,
	}.Resolve()
	rl := &Restlytics{tracer: NewTracer(cfg, capture), cfg: cfg}
	ctx := rl.tracer.Start(context.Background(), "GET /proxy", unsampledParent)
	downstream := &captureRoundTripper{}
	client := &http.Client{Transport: rl.HTTPRoundTripper(downstream)}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.example.test/orders", nil)
	if err != nil {
		t.Fatal(err)
	}

	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	rl.tracer.Finish(ctx)

	if !strings.HasSuffix(downstream.traceparent, "-00") {
		t.Fatalf("unsampled traceparent = %q", downstream.traceparent)
	}
	if len(capture.payloads) != 0 {
		t.Fatalf("unsampled request exported %d payloads", len(capture.payloads))
	}
}

func TestHTTPRoundTripperRecordingDoesNotRaceFinish(t *testing.T) {
	// An outbound call can outlive its handler (for example with
	// context.WithoutCancel). Its CLIENT span must be complete before Finish can
	// observe it; the race detector fails this test otherwise.
	for i := 0; i < 100; i++ {
		capture := &captureTransport{}
		cfg := Config{
			Key:             "rk_test",
			ServiceName:     "go-test",
			Environment:     "test",
			SampleRate:      1,
			MaxSpans:        100,
			InstrumentHTTP:  boolPtr(true),
			CustomTransport: capture,
		}.Resolve()
		rl := &Restlytics{tracer: NewTracer(cfg, capture), cfg: cfg}
		ctx := rl.tracer.Start(context.Background(), "GET /proxy", sampledParent)
		returned := make(chan struct{})
		downstream := roundTripFunc(func(request *http.Request) (*http.Response, error) {
			defer close(returned)
			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody, Header: make(http.Header), Request: request}, nil
		})
		client := &http.Client{Transport: rl.HTTPRoundTripper(downstream)}
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.example.test/orders", nil)
		if err != nil {
			t.Fatal(err)
		}

		done := make(chan struct{})
		go func() {
			defer close(done)
			if response, err := client.Do(request); err == nil {
				_ = response.Body.Close()
			}
		}()
		<-returned
		rl.tracer.Finish(ctx)
		<-done
	}
}
