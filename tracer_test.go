package restlytics

import (
	"context"
	"testing"
)

// The sampling decision is made ONCE per trace (SPEC §3): a continued trace
// inherits the upstream flag verbatim, a root trace rolls locally. Rate 0.0 makes
// any stray local re-roll return false, so a sampled upstream surviving it is
// proof the upstream bit was honored rather than re-decided.

const (
	sampledParent   = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	unsampledParent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00"
)

func newTestTracer(sampleRate float64) *Tracer {
	// Built directly (not via Config.Resolve) because Resolve would rewrite a
	// zero SampleRate to the 1.0 default.
	return NewTracer(Config{
		Key:         "rk_test_key",
		ServiceName: "svc",
		Environment: "test",
		SampleRate:  sampleRate,
		MaxSpans:    defaultMaxSpans,
	}, NullTransport{})
}

func TestStart_ContinuedTraceInheritsUpstreamSampled(t *testing.T) {
	tr := newTestTracer(0.0)
	ctx := tr.Start(context.Background(), "GET /users/{id}", sampledParent)

	if !IsSampled(ctx) {
		t.Fatal("continued trace with upstream sampled=1 must be sampled even at rate 0.0")
	}
	if got := TraceID(ctx); got != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatalf("trace id: %q", got)
	}
}

func TestStart_ContinuedTraceHonorsUpstreamNotSampled(t *testing.T) {
	// Rate 1.0: even a local roll that would keep the trace must not override an
	// upstream "not sampled".
	tr := newTestTracer(1.0)
	ctx := tr.Start(context.Background(), "GET /users/{id}", unsampledParent)

	if IsSampled(ctx) {
		t.Fatal("continued trace with upstream sampled=0 must not be sampled")
	}
}

func TestStart_RootTraceStillRollsLocally(t *testing.T) {
	tr := newTestTracer(0.0)
	ctx := tr.Start(context.Background(), "GET /users/{id}", "")

	if IsSampled(ctx) {
		t.Fatal("root trace at rate 0.0 must not be sampled")
	}
}
