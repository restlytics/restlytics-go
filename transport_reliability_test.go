package restlytics

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestTransportIsNonBlockingBoundedObservableAndFlushable(t *testing.T) {
	gate := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-gate
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	transport := newHTTPTransport(server.URL, "rl_test", 500*time.Millisecond, 4)

	started := time.Now()
	for i := 0; i < 10; i++ {
		transport.Send(ExportTraceServiceRequest{})
	}
	if time.Since(started) >= 250*time.Millisecond {
		t.Fatal("Send waited for network I/O")
	}
	snapshot := transport.Diagnostics()
	if snapshot.AcceptedBatches > 5 || snapshot.DroppedBatches < 5 || snapshot.QueueCapacity != 4 {
		t.Fatalf("unexpected saturation diagnostics: %+v", snapshot)
	}

	close(gate)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if !transport.Shutdown(ctx) {
		t.Fatal("graceful shutdown did not flush")
	}
	after := transport.Diagnostics()
	if after.DeliveredBatches != snapshot.AcceptedBatches || after.QueuedBatches != 0 {
		t.Fatalf("accepted work was not flushed: %+v", after)
	}
	transport.Send(ExportTraceServiceRequest{})
	if transport.Diagnostics().DroppedBatches != snapshot.DroppedBatches+1 {
		t.Fatal("send after shutdown was not counted as dropped")
	}
}

func TestTransportTimeoutIsCountedSwallowedAndNotRetried(t *testing.T) {
	var attempts atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	transport := NewHTTPTransport(server.URL, "rl_test", 20*time.Millisecond)
	transport.Send(ExportTraceServiceRequest{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if !transport.Flush(ctx) {
		t.Fatal("timed-out delivery did not settle")
	}
	if attempts.Load() != 1 || transport.Diagnostics().FailedBatches != 1 {
		t.Fatalf("timeout was retried or not counted: attempts=%d diagnostics=%+v", attempts.Load(), transport.Diagnostics())
	}
	if !transport.Shutdown(ctx) {
		t.Fatal("shutdown failed")
	}
}
