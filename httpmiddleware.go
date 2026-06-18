package restlytics

import (
	"context"
	"net/http"
	"strconv"
)

// Middleware returns a net/http middleware (func(http.Handler) http.Handler)
// that opens the root SERVER span at request start and flushes the OTLP batch
// via defer after the handler returns.
//
// http.route is set to the matched ServeMux pattern when available (Go 1.22+
// exposes it via Request.Pattern); for plain mux usage without patterns it falls
// back to the URL path. Framework adapters (Gin/Echo/Fiber) supply the real
// route template via their own routing info.
func (r *Restlytics) Middleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if !r.Enabled() || !r.ShouldTrace(req.URL.Path) {
				next.ServeHTTP(w, req)
				return
			}

			traceparent := req.Header.Get("traceparent")
			name := req.Method + " " + req.URL.Path
			ctx := r.tracer.Start(req.Context(), name, traceparent)
			req = req.WithContext(ctx)

			// Wrap the writer so we can capture the status code.
			sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}

			// Defer flush so it runs even if the handler panics. We mark the span as
			// errored, finish, then re-panic so the host's own recovery still sees it.
			defer func() {
				rec := recover()
				r.finishHTTP(ctx, req, sw, rec)
				if rec != nil {
					panic(rec)
				}
			}()

			next.ServeHTTP(sw, req)
		})
	}
}

// finishHTTP stamps the HTTP attributes on the root span and flushes the batch.
// route is resolved here (after the handler ran, so Request.Pattern is set).
func (r *Restlytics) finishHTTP(ctx context.Context, req *http.Request, sw *statusWriter, rec any) {
	root := RootSpan(ctx)
	if root == nil {
		return // not sampled / ignored
	}

	route := httpRoute(req)
	method := req.Method
	status := sw.status

	root.SetName(method + " " + route)
	root.SetString(AttrHTTPRequestMethod, method)
	root.SetString(AttrHTTPRoute, route)
	root.SetInt(AttrHTTPResponseStatusCode, int64(status))

	switch {
	case rec != nil:
		root.SetStatus(StatusError, "panic")
		root.SetInt(AttrHTTPResponseStatusCode, int64(http.StatusInternalServerError))
	case status >= 500:
		if root.StatusCode() != StatusError {
			root.SetStatus(StatusError, "HTTP "+strconv.Itoa(status))
		}
	case root.StatusCode() == StatusUnset:
		root.SetStatus(StatusOK, "")
	}

	r.tracer.Finish(ctx)
}

// httpRoute returns the route TEMPLATE for the request.
//
// The core net/http middleware has no portable access to the matched route
// template at the Go 1.21 baseline (Request.Pattern arrived later), so it falls
// back to the URL path. To emit a real low-cardinality template, either use a
// framework adapter (Gin/Echo/Fiber) or call WithRoute on the request context
// from your handler. http.route is the #1 correctness rule — a raw URL here is a
// last resort, not the intended steady state.
func httpRoute(req *http.Request) string {
	if rt := routeFromContext(req.Context()); rt != "" {
		return rt
	}
	return req.URL.Path
}

type statusWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (w *statusWriter) WriteHeader(code int) {
	if !w.wroteHeader {
		w.status = code
		w.wroteHeader = true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.wroteHeader = true
	}
	return w.ResponseWriter.Write(b)
}
