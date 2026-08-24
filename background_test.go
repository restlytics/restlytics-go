package restlytics

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
)

type captureTransport struct {
	mu       sync.Mutex
	payloads []ExportTraceServiceRequest
}

func (c *captureTransport) Send(payload ExportTraceServiceRequest) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.payloads = append(c.payloads, payload)
}

func backgroundTracer() (*Tracer, *captureTransport) {
	transport := &captureTransport{}
	return NewTracer(Config{Key: "rk_test", ServiceName: "worker", Environment: "test", SampleRate: 1, MaxSpans: 100}, transport), transport
}

func TestBackgroundJobPropagationSuccessAndRedaction(t *testing.T) {
	tracer, transport := backgroundTracer()
	carrier := QueueCarrier{"customer": "not-exported"}
	err := tracer.RunJob(context.Background(), JobOptions{Name: "billing.reconcile", System: "redis", Destination: "billing", Attempt: 2}, func(ctx context.Context) error {
		return Enqueue(ctx, EnqueueOptions{System: "redis", Destination: "emails"}, carrier, func(QueueCarrier) error { return nil })
	})
	if err != nil {
		t.Fatal(err)
	}
	spans := transport.payloads[0].ResourceSpans[0].ScopeSpans[0].Spans
	if spans[0].Kind != KindConsumer || spans[0].Status.Code != StatusOK {
		t.Fatalf("unexpected job root: %#v", spans[0])
	}
	envelope := carrier["__restlytics"].(map[string]string)
	if !strings.Contains(envelope["traceparent"], "-"+spans[1].SpanID+"-01") {
		t.Fatalf("bad traceparent: %s", envelope["traceparent"])
	}
	encoded, _ := json.Marshal(transport.payloads[0])
	if strings.Contains(string(encoded), "not-exported") {
		t.Fatal("carrier payload leaked into telemetry")
	}
}

func TestBackgroundJobContinuesAndLinksFailureWithoutErrorContent(t *testing.T) {
	tracer, transport := backgroundTracer()
	parent := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	err := tracer.RunJob(context.Background(), JobOptions{Name: "send.email", System: "redis", Destination: "emails", Traceparent: parent}, func(context.Context) error {
		return errors.New("customer secret")
	})
	if err == nil {
		t.Fatal("expected operation error")
	}
	root := transport.payloads[0].ResourceSpans[0].ScopeSpans[0].Spans[0]
	if root.TraceID != "4bf92f3577b34da6a3ce929d0e0e4736" || len(root.Links) != 1 || root.Status.Code != StatusError {
		t.Fatalf("unexpected root: %#v", root)
	}
	encoded, _ := json.Marshal(root)
	if strings.Contains(string(encoded), "customer secret") {
		t.Fatal("error content leaked")
	}
}

func TestBackgroundContextsAreTenantSafeAndCommandExitFails(t *testing.T) {
	tracer, transport := backgroundTracer()
	var wait sync.WaitGroup
	wait.Add(2)
	go func() {
		defer wait.Done()
		_ = tracer.RunSchedule(context.Background(), ScheduleOptions{Name: "nightly-digest", Cron: "0 3 * * *"}, func(context.Context) error { return nil })
	}()
	go func() {
		defer wait.Done()
		_, _ = tracer.RunCommand(context.Background(), CommandOptions{Name: "reports:generate"}, func(context.Context) (int, error) { return 2, nil })
	}()
	wait.Wait()
	if len(transport.payloads) != 2 {
		t.Fatalf("payload count: %d", len(transport.payloads))
	}
	first := transport.payloads[0].ResourceSpans[0].ScopeSpans[0].Spans[0]
	second := transport.payloads[1].ResourceSpans[0].ScopeSpans[0].Spans[0]
	if first.TraceID == second.TraceID {
		t.Fatal("concurrent work shared a trace")
	}
	for _, root := range []SpanData{first, second} {
		if root.Name == "reports:generate" && root.Status.Code != StatusError {
			t.Fatal("non-zero command exit was not failed")
		}
	}
}

func TestBackgroundUnsampledContextPropagatesWithoutExporting(t *testing.T) {
	transport := &captureTransport{}
	tracer := NewTracer(Config{Key: "rk_test", ServiceName: "worker", Environment: "test", SampleRate: 0, MaxSpans: 100}, transport)
	carrier := QueueCarrier{}
	err := tracer.RunJob(context.Background(), JobOptions{Name: "billing.reconcile", System: "redis", Destination: "billing"}, func(ctx context.Context) error {
		return Enqueue(ctx, EnqueueOptions{System: "redis", Destination: "emails"}, carrier, func(QueueCarrier) error { return nil })
	})
	if err != nil {
		t.Fatal(err)
	}
	envelope := carrier["__restlytics"].(map[string]string)
	if !strings.HasSuffix(envelope["traceparent"], "-00") {
		t.Fatalf("unsampled traceparent: %s", envelope["traceparent"])
	}
	if len(transport.payloads) != 0 {
		t.Fatalf("unsampled work exported %d payloads", len(transport.payloads))
	}
}
