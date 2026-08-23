package restlytics_test

import (
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/restlytics/restlytics-go"
	restlyticsgin "github.com/restlytics/restlytics-go/adapters/gin"
)

const (
	projectKey = "rk_project_alpha"
	secret     = "customer-secret-must-not-leave-the-app"
)

type capture struct {
	key     string
	path    string
	payload restlytics.ExportTraceServiceRequest
}

func TestRealGinAppEmitsTenantSafeOTLPAndSurvivesIngestFailure(t *testing.T) {
	var mu sync.Mutex
	captures := make([]capture, 0, 3)
	ingestStatus := http.StatusAccepted
	ingest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		reader, err := gzip.NewReader(req.Body)
		if err != nil {
			t.Errorf("open gzip payload: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		body, err := io.ReadAll(reader)
		_ = reader.Close()
		if err != nil {
			t.Errorf("read payload: %v", err)
			return
		}
		var payload restlytics.ExportTraceServiceRequest
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Errorf("decode payload: %v", err)
			return
		}
		mu.Lock()
		captures = append(captures, capture{key: req.Header.Get("X-Restlytics-Key"), path: req.URL.Path, payload: payload})
		status := ingestStatus
		mu.Unlock()
		w.WriteHeader(status)
		_, _ = w.Write([]byte("{}"))
	}))
	defer ingest.Close()

	rl := restlytics.Init(restlytics.Config{
		Key:         projectKey,
		IngestURL:   ingest.URL,
		ServiceName: "gin-beta-app",
		Environment: "test",
		TimeoutMs:   300,
	})
	gin.SetMode(gin.TestMode)
	app := gin.New()
	app.Use(restlyticsgin.Middleware(rl))
	app.GET("/orders/:id", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"ok": true}) })
	app.GET("/fail/:id", func(c *gin.Context) { c.JSON(http.StatusServiceUnavailable, gin.H{"ok": false}) })

	request := httptest.NewRequest(http.MethodGet, "/orders/42?token="+secret, nil)
	request.Header.Set("Authorization", "Bearer "+secret)
	request.Header.Set("Cookie", "session="+secret)
	request.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	response := httptest.NewRecorder()
	app.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("customer response = %d, want 200", response.Code)
	}
	waitForCaptures(t, &mu, &captures, 1)

	mu.Lock()
	first := captures[0]
	mu.Unlock()
	root := rootSpan(t, first.payload)
	if first.path != "/v1/traces" || first.key != projectKey {
		t.Fatalf("ingest request path/key = %q/%q", first.path, first.key)
	}
	if root.TraceID != "4bf92f3577b34da6a3ce929d0e0e4736" || root.ParentSpanID != "00f067aa0ba902b7" {
		t.Fatalf("trace continuation drift: %#v", root)
	}
	if got := stringAttribute(root, restlytics.AttrHTTPRoute); got != "/orders/:id" {
		t.Fatalf("http.route = %q", got)
	}
	raw, _ := json.Marshal(first.payload)
	if strings.Contains(string(raw), projectKey) || strings.Contains(string(raw), secret) {
		t.Fatal("project key or request secret leaked into telemetry")
	}

	mu.Lock()
	ingestStatus = http.StatusServiceUnavailable
	mu.Unlock()
	response = httptest.NewRecorder()
	app.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/orders/43", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("ingest failure changed customer response to %d", response.Code)
	}
	waitForCaptures(t, &mu, &captures, 2)

	mu.Lock()
	ingestStatus = http.StatusAccepted
	mu.Unlock()
	response = httptest.NewRecorder()
	app.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/fail/44", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("failure response = %d", response.Code)
	}
	waitForCaptures(t, &mu, &captures, 3)
	mu.Lock()
	failed := captures[2]
	mu.Unlock()
	failedRoot := rootSpan(t, failed.payload)
	if failedRoot.Status == nil || failedRoot.Status.Code != restlytics.StatusError {
		t.Fatalf("failed route status = %#v", failedRoot.Status)
	}
	if got := stringAttribute(failedRoot, restlytics.AttrHTTPRoute); got != "/fail/:id" {
		t.Fatalf("failed http.route = %q", got)
	}
}

func waitForCaptures(t *testing.T, mu *sync.Mutex, captures *[]capture, count int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		got := len(*captures)
		mu.Unlock()
		if got >= count {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d ingest requests", count)
}

func rootSpan(t *testing.T, payload restlytics.ExportTraceServiceRequest) restlytics.SpanData {
	t.Helper()
	if len(payload.ResourceSpans) == 0 || len(payload.ResourceSpans[0].ScopeSpans) == 0 || len(payload.ResourceSpans[0].ScopeSpans[0].Spans) == 0 {
		t.Fatal("missing root span")
	}
	return payload.ResourceSpans[0].ScopeSpans[0].Spans[0]
}

func stringAttribute(span restlytics.SpanData, key string) string {
	for _, attribute := range span.Attributes {
		if attribute.Key == key && attribute.Value.StringValue != nil {
			return *attribute.Value.StringValue
		}
	}
	return ""
}
