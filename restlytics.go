// Package restlytics is the Go SDK for restlytics — framework-native request
// tracing that emits the shared OTLP/JSON wire format (see sdks/SPEC.md).
//
// The core package depends only on the Go standard library so it compiles
// offline. Framework adapters (Gin, Echo, Fiber, GORM) live in subpackages and
// import their respective third-party libraries.
//
// Quick start (net/http):
//
//	rl := restlytics.Init(restlytics.Config{}) // reads RESTLYTICS_* env
//	mux := http.NewServeMux()
//	// ... register routes ...
//	handler := rl.Middleware()(mux)
//	http.ListenAndServe(":8080", handler)
package restlytics

import (
	"context"
	"log/slog"
	"net/url"
	"strings"
)

// Restlytics is the top-level SDK handle returned by Init. It owns the resolved
// config + transport and produces middleware / DB wrappers.
type Restlytics struct {
	tracer        *Tracer
	logsTransport LogsTransport
	lifecycle     transportLifecycle
	diagnostics   diagnosticTransport
	cfg           Config
}

// Init resolves the config (filling gaps from RESTLYTICS_* env vars), selects a
// transport, and returns an SDK handle. It never panics; a missing key simply
// yields a disabled default HTTP transport unless a custom exporter or local
// preview transport is configured.
func Init(cfg Config) *Restlytics {
	resolved := cfg.Resolve()
	transport := transportFromConfig(resolved)
	lifecycle, _ := transport.(transportLifecycle)
	diagnostics, _ := transport.(diagnosticTransport)
	return &Restlytics{
		tracer:        NewTracer(resolved, transport),
		logsTransport: logsTransportFromConfig(resolved, transport),
		lifecycle:     lifecycle,
		diagnostics:   diagnostics,
		cfg:           resolved,
	}
}

// Tracer exposes the underlying tracer for advanced/manual instrumentation.
func (r *Restlytics) Tracer() *Tracer { return r.tracer }

// Config returns the resolved config.
func (r *Restlytics) Config() Config { return r.cfg }

// Enabled reports whether the SDK has a configured export destination.
func (r *Restlytics) Enabled() bool { return r.cfg.Enabled() }

// Flush waits for telemetry already accepted by the configured transport. It
// returns false on deadline or lifecycle-hook failure/panic and never lets a
// customer exporter failure escape into the host application. Individual batch
// failures are reported by TransportDiagnostics. Transports with no lifecycle
// support are already synchronous/no-op and return true.
func (r *Restlytics) Flush(ctx context.Context) (ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	if r == nil || r.lifecycle == nil {
		return true
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return r.lifecycle.Flush(ctx)
}

// Shutdown stops new transport work, flushes accepted telemetry, and releases
// owned resources within ctx. It is safe to call more than once. A false result
// reports an incomplete/failed shutdown; failures never panic into the host.
func (r *Restlytics) Shutdown(ctx context.Context) (ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	if r == nil || r.lifecycle == nil {
		return true
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return r.lifecycle.Shutdown(ctx)
}

// TransportDiagnostics returns payload-free, process-local delivery counters
// when the configured transport supports them. The boolean is false for legacy
// transports without diagnostics. No payload, key, header, or tenant identity is
// included in the snapshot.
func (r *Restlytics) TransportDiagnostics() (snapshot TransportDiagnostics, available bool) {
	defer func() {
		if recover() != nil {
			snapshot = TransportDiagnostics{}
			available = false
		}
	}()
	if r == nil || r.diagnostics == nil {
		return TransportDiagnostics{}, false
	}
	return r.diagnostics.Diagnostics(), true
}

// SlogHandler returns a log/slog handler that exports qualifying records and
// delegates unchanged records to next. Pass nil for a capture-only handler.
// Use the context-aware slog methods (InfoContext, ErrorContext, and so on) to
// attach the active Restlytics trace and span identifiers.
func (r *Restlytics) SlogHandler(next slog.Handler) slog.Handler {
	return NewSlogHandler(r, next)
}

// ShouldTrace reports whether the given request path should be traced (i.e. is
// not in the ignore list). Trailing `*` acts as a prefix wildcard.
func (r *Restlytics) ShouldTrace(path string) bool {
	p := "/" + strings.TrimLeft(path, "/")
	for _, pattern := range r.cfg.IgnorePaths {
		pat := "/" + strings.TrimLeft(pattern, "/")
		if matchPath(pat, p) {
			return false
		}
	}
	return true
}

// matchPath supports exact matches and a single trailing `*` prefix wildcard.
func matchPath(pattern, path string) bool {
	if strings.HasSuffix(pattern, "*") {
		return strings.HasPrefix(path, strings.TrimSuffix(pattern, "*"))
	}
	return pattern == path
}

type routeKey struct{}

// WithRoute returns a context that carries an explicit route TEMPLATE (e.g.
// "/users/{id}"). Handlers using the plain net/http middleware can call this so
// the SERVER span reports a low-cardinality http.route instead of the raw path.
func WithRoute(ctx context.Context, template string) context.Context {
	return context.WithValue(ctx, routeKey{}, template)
}

// SetRoute sets the route template on the request's existing context-carried
// state directly. It is a convenience for adapters that learn the template after
// Start (e.g. Gin's c.FullPath()).
func SetRoute(ctx context.Context, template string) {
	if root := RootSpan(ctx); root != nil && template != "" {
		root.SetString(AttrHTTPRoute, template)
	}
}

func routeFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if v, ok := ctx.Value(routeKey{}).(string); ok {
		return v
	}
	return ""
}

// sensitiveHeaders are scrubbed from any header capture (SPEC §6). Lowercased.
var sensitiveHeaders = map[string]struct{}{
	"authorization":       {},
	"cookie":              {},
	"set-cookie":          {},
	"x-api-key":           {},
	"x-restlytics-key":    {},
	"proxy-authorization": {},
}

// IsSensitiveHeader reports whether a header name (case-insensitive) must be
// scrubbed and never captured.
func IsSensitiveHeader(name string) bool {
	_, ok := sensitiveHeaders[strings.ToLower(name)]
	return ok
}

var sensitiveAttributeSegments = map[string]struct{}{
	"authorization": {}, "auth": {}, "cookie": {}, "cookies": {},
	"setcookie": {}, "password": {}, "passwd": {}, "secret": {},
	"token": {}, "accesstoken": {}, "refreshtoken": {}, "apikey": {},
	"credential": {}, "credentials": {}, "body": {}, "payload": {},
	"form": {}, "stack": {}, "stacktrace": {}, "log": {},
}

// IsSensitiveAttributeKey is the fail-closed boundary for framework-specific
// attributes. Content-bearing headers, bodies, logs, credentials and exception
// fields never enter a span.
func IsSensitiveAttributeKey(key string) bool {
	normalized := strings.NewReplacer("-", ".", "_", ".").Replace(strings.ToLower(strings.TrimSpace(key)))
	switch normalized {
	case "http.request.method", "http.response.status.code", "restlytics.bindings.count":
		return false
	}
	for _, segment := range strings.Split(normalized, ".") {
		if _, sensitive := sensitiveAttributeSegments[segment]; sensitive {
			return true
		}
	}
	return false
}

// redactURL removes credentials/fragments and redacts every query value. keys is
// retained for API compatibility; safety does not depend on guessing key names.
func redactURL(raw string, _ []string) string {
	u, err := url.Parse(raw)
	if err != nil {
		clean := strings.SplitN(strings.SplitN(raw, "#", 2)[0], "?", 2)[0]
		if scheme := strings.Index(clean, "://"); scheme >= 0 {
			prefix := clean[:scheme+3]
			remainder := clean[scheme+3:]
			if at := strings.LastIndex(remainder, "@"); at >= 0 {
				return prefix + remainder[at+1:]
			}
		}
		return clean
	}
	u.User = nil
	u.Fragment = ""

	q := u.Query()
	for k := range q {
		q.Set(k, "REDACTED")
	}
	u.RawQuery = q.Encode()
	return u.String()
}
