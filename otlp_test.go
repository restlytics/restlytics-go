package restlytics

import (
	"encoding/json"
	"regexp"
	"strings"
	"testing"
)

// Lock down the three classic OTLP/JSON footguns on the serialized wire format.

func TestWire_IntValueIsString(t *testing.T) {
	sp := newSpan("a", "b", "", "n", KindServer, 1, 2)
	sp.SetInt(AttrHTTPResponseStatusCode, 200)
	raw, _ := json.Marshal(sp.toData())
	if !strings.Contains(string(raw), `"intValue":"200"`) {
		t.Fatalf("intValue must be a string, got: %s", raw)
	}
	if strings.Contains(string(raw), `"intValue":200`) {
		t.Fatalf("intValue must NOT be a JSON number, got: %s", raw)
	}
}

func TestWire_UnixNanoAreStrings(t *testing.T) {
	sp := newSpan("a", "b", "", "n", KindServer, 1700000000000000000, 1700000000500000000)
	raw, _ := json.Marshal(sp.toData())
	s := string(raw)
	if !strings.Contains(s, `"startTimeUnixNano":"1700000000000000000"`) {
		t.Fatalf("startTimeUnixNano must be a string: %s", s)
	}
	if !strings.Contains(s, `"endTimeUnixNano":"1700000000500000000"`) {
		t.Fatalf("endTimeUnixNano must be a string: %s", s)
	}
}

var (
	traceIDRe = regexp.MustCompile(`^[0-9a-f]{32}$`)
	spanIDRe  = regexp.MustCompile(`^[0-9a-f]{16}$`)
)

func TestIDs_ShapeAndNotAllZero(t *testing.T) {
	for i := 0; i < 200; i++ {
		tid := NewTraceID()
		sid := NewSpanID()
		if !traceIDRe.MatchString(tid) {
			t.Fatalf("bad trace id: %q", tid)
		}
		if !spanIDRe.MatchString(sid) {
			t.Fatalf("bad span id: %q", sid)
		}
		if isAllZeroHex(tid) || isAllZeroHex(sid) {
			t.Fatalf("ids must not be all-zero: %q %q", tid, sid)
		}
	}
}

func TestWire_RootOmitsParentSpanID(t *testing.T) {
	root := newSpan("aa", "bb", "", "root", KindServer, 1, 2)
	raw, _ := json.Marshal(root.toData())
	if strings.Contains(string(raw), "parentSpanId") {
		t.Fatalf("root span must omit parentSpanId: %s", raw)
	}

	child := newSpan("aa", "cc", "bb", "child", KindClient, 1, 2)
	raw2, _ := json.Marshal(child.toData())
	if !strings.Contains(string(raw2), `"parentSpanId":"bb"`) {
		t.Fatalf("child must carry parentSpanId: %s", raw2)
	}
}

func TestParseTraceparent(t *testing.T) {
	tp, ok := ParseTraceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	if !ok {
		t.Fatal("expected valid traceparent")
	}
	if tp.TraceID != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatalf("trace id: %q", tp.TraceID)
	}
	if tp.ParentSpanID != "00f067aa0ba902b7" {
		t.Fatalf("parent span id: %q", tp.ParentSpanID)
	}
	if !tp.Sampled {
		t.Fatal("expected sampled=true")
	}

	if _, ok := ParseTraceparent(""); ok {
		t.Fatal("empty must be invalid")
	}
	if _, ok := ParseTraceparent("00-00000000000000000000000000000000-00f067aa0ba902b7-01"); ok {
		t.Fatal("all-zero trace id must be rejected")
	}
	if _, ok := ParseTraceparent("garbage"); ok {
		t.Fatal("garbage must be rejected")
	}
}

func TestSampleDecision_Bounds(t *testing.T) {
	if !sampleDecision("ffffffffffffffffffffffffffffffff", 1.0) {
		t.Fatal("rate 1.0 must always sample")
	}
	if sampleDecision("ffffffffffffffffffffffffffffffff", 0.0) {
		t.Fatal("rate 0.0 must never sample")
	}
}

func TestBuildPayload_Shape(t *testing.T) {
	root := newSpan(NewTraceID(), NewSpanID(), "", "GET /users/{id}", KindServer, 1, 2)
	root.SetString(AttrHTTPRoute, "/users/{id}")
	payload := BuildPayload("svc", "production", []SpanData{root.toData()})
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	s := string(raw)
	for _, want := range []string{
		`"resourceSpans"`, `"scopeSpans"`, `"restlytics-go"`,
		`"service.name"`, `"deployment.environment"`, `"telemetry.sdk.version"`,
		`"http.route"`,
	} {
		if !strings.Contains(s, want) {
			t.Fatalf("payload missing %s: %s", want, s)
		}
	}
}
