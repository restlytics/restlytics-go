package restlytics

import (
	"context"
	"strconv"
	"sync"
	"time"
)

// Per-request tracer state.
//
// Unlike the Laravel reference (a reused singleton that must reset() between
// requests), the Go SDK isolates per-request state with context.Context: each
// request gets its own *requestState carried in the context, so there is no
// cross-request leakage and no shared mutable singleton. A mutex guards the
// child buffer because DB/HTTP/cache instrumentation may run on goroutines
// spawned within the request.
//
// Timing model: time.Now() (with monotonic reading on Go) is read for both the
// absolute epoch start and subsequent durations. Go's time.Now carries a
// monotonic clock so subtractions are immune to wall-clock jumps.

type ctxKey struct{}

// requestState holds everything for one in-flight request.
type requestState struct {
	mu sync.Mutex

	enabled bool
	sampled bool

	traceID  string
	rootSpan *Span

	rawSpans []*Span // in-request child spans, serialized + self-timed at finish

	dbQueryCount int64
	maxSpans     int

	cfg       *Config
	transport Transport
}

// Tracer owns config + transport and starts per-request traces. It is safe for
// concurrent use across requests; per-request mutable state lives in the context.
type Tracer struct {
	cfg       Config
	transport Transport
}

// NewTracer builds a Tracer from a resolved Config and a Transport.
func NewTracer(cfg Config, transport Transport) *Tracer {
	return &Tracer{cfg: cfg, transport: transport}
}

// Config returns the resolved config.
func (t *Tracer) Config() Config { return t.cfg }

// Start opens the root SERVER span and returns a context carrying the request
// state. traceparent (may be "") continues an incoming distributed trace.
//
// The sampling decision is HEAD-BASED and made exactly once here, keyed off the
// trace id, so all spans in a trace share the same fate.
func (t *Tracer) Start(ctx context.Context, name, traceparent string) context.Context {
	st := &requestState{
		enabled:   t.cfg.Enabled(),
		maxSpans:  t.cfg.MaxSpans,
		cfg:       &t.cfg,
		transport: t.transport,
	}

	var rootParent string
	if tp, ok := ParseTraceparent(traceparent); ok {
		st.traceID = tp.TraceID
		rootParent = tp.ParentSpanID
		// Respect an upstream "not sampled" decision; only re-roll if it was sampled.
		st.sampled = st.enabled && tp.Sampled && sampleDecision(st.traceID, t.cfg.SampleRate)
	} else {
		st.traceID = NewTraceID()
		st.sampled = st.enabled && sampleDecision(st.traceID, t.cfg.SampleRate)
	}

	if st.sampled {
		now := nowNs()
		st.rootSpan = newSpan(st.traceID, NewSpanID(), rootParent, name, KindServer, now, now)
	}

	return context.WithValue(ctx, ctxKey{}, st)
}

// fromContext extracts the request state, if any.
func fromContext(ctx context.Context) *requestState {
	if ctx == nil {
		return nil
	}
	st, _ := ctx.Value(ctxKey{}).(*requestState)
	return st
}

// IsSampled reports whether the context's trace is being recorded.
func IsSampled(ctx context.Context) bool {
	st := fromContext(ctx)
	return st != nil && st.enabled && st.sampled && st.rootSpan != nil
}

// RootSpan returns the root SERVER span for this request, or nil.
func RootSpan(ctx context.Context) *Span {
	st := fromContext(ctx)
	if st == nil {
		return nil
	}
	return st.rootSpan
}

// TraceID returns the active trace id, or "".
func TraceID(ctx context.Context) string {
	st := fromContext(ctx)
	if st == nil {
		return ""
	}
	return st.traceID
}

// AddChildSpan creates and records a CLIENT child span over an absolute
// [startNs, endNs] window. DB/HTTP/cache instrumentation often only learns of a
// span AFTER it finished, so callers back-date the start. Returns nil when not
// sampled or when the buffer cap is hit.
//
// The returned span may be mutated (attributes/status) until Finish is called;
// after Finish the span is frozen into the outgoing buffer.
func AddChildSpan(ctx context.Context, name string, startNs, endNs int64) *Span {
	st := fromContext(ctx)
	if st == nil || !st.sampled || st.rootSpan == nil {
		return nil
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	if len(st.rawSpans) >= st.maxSpans {
		return nil
	}
	sp := newSpan(st.traceID, NewSpanID(), st.rootSpan.SpanID, name, KindClient, startNs, endNs)
	st.rawSpans = append(st.rawSpans, sp)
	return sp
}

// IncrementDBQueryCount bumps the per-request DB query counter.
func IncrementDBQueryCount(ctx context.Context) {
	st := fromContext(ctx)
	if st == nil {
		return
	}
	st.mu.Lock()
	st.dbQueryCount++
	st.mu.Unlock()
}

// Finish closes the root span, computes self-time rollups, and flushes the OTLP
// batch (fire-and-forget). Safe to call once per request; resilient to panics.
func (t *Tracer) Finish(ctx context.Context) {
	st := fromContext(ctx)
	if st == nil {
		return
	}

	defer func() { _ = recover() }()

	st.mu.Lock()
	defer st.mu.Unlock()

	if !st.sampled || st.rootSpan == nil {
		return
	}

	st.rootSpan.SetEnd(nowNs())
	st.attachSelfTime()
	st.rootSpan.SetInt(AttrDBQueryCount, st.dbQueryCount)
	st.rootSpan.SetString(AttrCategory, CategoryApp)

	all := make([]SpanData, 0, len(st.rawSpans)+1)
	all = append(all, st.rootSpan.toData())
	for _, sp := range st.rawSpans {
		all = append(all, sp.toData())
	}

	payload := BuildPayload(st.cfg.ServiceName, st.cfg.Environment, all)
	st.transport.Send(payload)
}

// attachSelfTime computes and stamps restlytics.self_ns.{db,http,cache,app} on
// the root span. Caller holds st.mu.
func (st *requestState) attachSelfTime() {
	root := st.rootSpan
	if root == nil {
		return
	}
	rootStart := root.StartUnixNano
	rootDur := root.DurationNs()

	byCat := map[string][]Interval{
		CategoryDB:    nil,
		CategoryHTTP:  nil,
		CategoryCache: nil,
		CategoryApp:   nil,
	}
	var all []Interval

	for _, sp := range st.rawSpans {
		start := sp.StartUnixNano - rootStart
		end := sp.EndUnixNano - rootStart
		if end < start {
			end = start
		}
		iv := Interval{Start: start, End: end}
		all = append(all, iv)
		byCat[sp.category()] = append(byCat[sp.category()], iv)
	}

	selfDB := UnionLength(byCat[CategoryDB])
	selfHTTP := UnionLength(byCat[CategoryHTTP])
	selfCache := UnionLength(byCat[CategoryCache])
	// app self-time = explicit app-category child time + the root's own exclusive
	// (uncovered) time. Mirrors the ingestion service's computation.
	appExclusive := rootDur - UnionLength(all)
	if appExclusive < 0 {
		appExclusive = 0
	}
	selfApp := UnionLength(byCat[CategoryApp]) + appExclusive

	root.SetInt(AttrSelfNsDB, selfDB)
	root.SetInt(AttrSelfNsHTTP, selfHTTP)
	root.SetInt(AttrSelfNsCache, selfCache)
	root.SetInt(AttrSelfNsApp, selfApp)
}

// nowNs returns the current epoch time in nanoseconds.
func nowNs() int64 {
	return time.Now().UnixNano()
}

// sampleDecision is deterministic head-based trace-id-ratio sampling: hash the
// last 32 bits of the trace id into [0,1) and keep it if it falls under rate.
func sampleDecision(traceID string, rate float64) bool {
	if rate >= 1.0 {
		return true
	}
	if rate <= 0.0 {
		return false
	}
	tail := traceID
	if len(tail) > 8 {
		tail = tail[len(tail)-8:]
	}
	bucket, err := strconv.ParseUint(tail, 16, 64)
	if err != nil {
		return false
	}
	ratio := float64(bucket) / float64(0xFFFFFFFF)
	return ratio < rate
}
