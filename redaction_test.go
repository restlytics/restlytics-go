package restlytics

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestRedactionURLRemovesCredentialsFragmentAndEveryQueryValue(t *testing.T) {
	value := redactURL("https://alice:password@example.test/orders?token=abc&unknown=customer-secret#raw", []string{"token"})
	for _, secret := range []string{"alice", "password", "abc", "customer-secret", "raw"} {
		if strings.Contains(value, secret) {
			t.Fatalf("URL leaked %q: %s", secret, value)
		}
	}
}

func TestRedactionSpanBoundaryDropsContentBearingFields(t *testing.T) {
	span := newSpan(strings.Repeat("a", 32), strings.Repeat("b", 16), "", "GET /users/{id}", KindServer, 1, 2)
	span.SetString(AttrHTTPRequestMethod, "GET")
	span.SetString("http.request.header.authorization", "Bearer abc.def.ghi")
	span.SetString("gin.request.body", "password=hunter2")
	span.SetString("log.body", "alice@example.test")
	span.SetString(AttrURLFull, "https://example.test/?unknown=customer-secret")
	span.SetStatus(StatusError, "login failed for alice@example.test password=hunter2")

	raw, err := json.Marshal(span.toData())
	if err != nil {
		t.Fatal(err)
	}
	encoded := string(raw)
	for _, secret := range []string{"hunter2", "alice@example.test", "customer-secret", "authorization"} {
		if strings.Contains(encoded, secret) {
			t.Fatalf("span leaked %q: %s", secret, encoded)
		}
	}
	if span.toData().Status.Message != "" {
		t.Fatalf("exception message must be omitted: %#v", span.toData().Status)
	}
	if !IsSensitiveAttributeKey("fiber.request.payload") {
		t.Fatal("framework payload key must be sensitive")
	}
	if IsSensitiveAttributeKey(AttrBindingsCount) {
		t.Fatal("binding count is safe; only binding values are forbidden")
	}
}
