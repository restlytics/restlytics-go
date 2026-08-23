package restlytics

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestPreviewReportsRedactedPayloadAndSizesWithoutNetworking(t *testing.T) {
	var output bytes.Buffer
	transport := NewPreviewTransport(0.25, &output)
	span := newSpan(strings.Repeat("a", 32), strings.Repeat("b", 16), "", "GET /users/{id}", KindServer, 1, 2)
	span.SetString("url.full", "https://user:secret@example.test/users/1?token=secret")
	span.SetString("http.request.body", "do-not-export")
	transport.Send(BuildPayload("preview-app", "production", []SpanData{span.toData()}))

	if len(transport.Reports) != 1 {
		t.Fatalf("reports = %d, want 1", len(transport.Reports))
	}
	report := transport.Reports[0]
	if report.NetworkRequestMade || report.ConfiguredSampleRate != 0.25 || report.SpanCount != 1 {
		t.Fatalf("unexpected report: %+v", report)
	}
	if report.JSONBytes <= report.GzipBytes {
		t.Fatalf("json bytes %d should exceed gzip bytes %d", report.JSONBytes, report.GzipBytes)
	}
	encoded, _ := json.Marshal(report)
	for _, secret := range []string{"secret", "do-not-export"} {
		if strings.Contains(string(encoded), secret) {
			t.Fatalf("preview leaked %q", secret)
		}
	}
	if !strings.Contains(output.String(), "REDACTED") {
		t.Fatal("preview does not show URL redaction")
	}
}

func TestPreviewWorksWithoutIngestKey(t *testing.T) {
	rl := Init(Config{Transport: "preview"})
	if !rl.Enabled() {
		t.Fatal("preview should be enabled without an ingest key")
	}
	if _, ok := rl.tracer.transport.(*PreviewTransport); !ok {
		t.Fatalf("transport = %T, want *PreviewTransport", rl.tracer.transport)
	}
}
