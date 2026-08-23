package restlytics

import (
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"
)

func loadConformanceProperties(t *testing.T) map[string]string {
	t.Helper()
	raw, err := os.ReadFile("testdata/conformance/v1/vectors.properties")
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		parts := strings.SplitN(strings.TrimSpace(line), "=", 2)
		if len(parts) != 2 {
			t.Fatalf("invalid conformance property: %q", line)
		}
		values[parts[0]] = parts[1]
	}
	return values
}

func TestSharedConformanceFixture(t *testing.T) {
	fixture := loadConformanceProperties(t)
	kind, _ := strconv.Atoi(fixture["span.kind"])
	start, _ := strconv.ParseInt(fixture["span.start_ns"], 10, 64)
	end, _ := strconv.ParseInt(fixture["span.end_ns"], 10, 64)
	intValue, _ := strconv.ParseInt(fixture["attribute.int.value"], 10, 64)
	status, _ := strconv.Atoi(fixture["error.status_code"])
	boolValue, _ := strconv.ParseBool(fixture["attribute.bool.value"])

	span := newSpan(
		fixture["trace.id"], fixture["span.id"], fixture["span.parent_id"],
		fixture["span.name"], kind, start, end,
	)
	span.SetString(fixture["attribute.string.key"], fixture["attribute.string.value"])
	span.SetInt(fixture["attribute.int.key"], intValue)
	span.SetBool(fixture["attribute.bool.key"], boolValue)
	span.SetString(fixture["redaction.attribute_key"], fixture["redaction.attribute_value"])
	span.SetStatus(status, fixture["error.message"])

	actualJSON, err := json.Marshal(BuildPayload(
		fixture["service.name"], fixture["deployment.environment"], []SpanData{span.toData()},
	))
	if err != nil {
		t.Fatal(err)
	}
	expectedJSON, err := os.ReadFile("testdata/conformance/v1/otlp.expected.json")
	if err != nil {
		t.Fatal(err)
	}
	expectedText := strings.NewReplacer(
		"${SDK_NAME}", SDKName,
		"${SDK_LANGUAGE}", SDKLanguage,
		"${SDK_VERSION}", SDKVersion,
	).Replace(string(expectedJSON))
	var actual, expected any
	if err := json.Unmarshal(actualJSON, &actual); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(expectedText), &expected); err != nil {
		t.Fatal(err)
	}
	if !deepEqualJSON(actual, expected) {
		t.Fatalf("OTLP output does not match shared fixture\nactual: %s\nexpected: %s", actualJSON, expectedText)
	}

	sampled, ok := ParseTraceparent(fixture["propagation.sampled"])
	if !ok || !sampled.Sampled || sampled.TraceID != fixture["trace.id"] || sampled.ParentSpanID != fixture["span.id"] {
		t.Fatal("sampled traceparent does not match shared fixture")
	}
	unsampled, ok := ParseTraceparent(fixture["propagation.unsampled"])
	if !ok || unsampled.Sampled {
		t.Fatal("unsampled traceparent does not match shared fixture")
	}
	if _, ok := ParseTraceparent(fixture["propagation.invalid"]); ok {
		t.Fatal("invalid traceparent must be rejected")
	}
	zero, _ := strconv.ParseFloat(fixture["sampling.root_rate_zero"], 64)
	one, _ := strconv.ParseFloat(fixture["sampling.root_rate_one"], 64)
	if sampleDecision(fixture["trace.id"], zero) || !sampleDecision(fixture["trace.id"], one) {
		t.Fatal("root sampling rates do not match shared fixture")
	}
}

func deepEqualJSON(left, right any) bool {
	l, _ := json.Marshal(left)
	r, _ := json.Marshal(right)
	return string(l) == string(r)
}
