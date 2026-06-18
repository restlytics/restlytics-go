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
	"net/url"
	"strings"
)

// Restlytics is the top-level SDK handle returned by Init. It owns the resolved
// config + transport and produces middleware / DB wrappers.
type Restlytics struct {
	tracer *Tracer
	cfg    Config
}

// Init resolves the config (filling gaps from RESTLYTICS_* env vars), selects a
// transport, and returns an SDK handle. It never panics; a missing key simply
// yields a disabled (no-op) SDK.
func Init(cfg Config) *Restlytics {
	resolved := cfg.Resolve()
	transport := transportFromConfig(resolved)
	return &Restlytics{
		tracer: NewTracer(resolved, transport),
		cfg:    resolved,
	}
}

// Tracer exposes the underlying tracer for advanced/manual instrumentation.
func (r *Restlytics) Tracer() *Tracer { return r.tracer }

// Config returns the resolved config.
func (r *Restlytics) Config() Config { return r.cfg }

// Enabled reports whether the SDK will emit traces (i.e. a key is configured).
func (r *Restlytics) Enabled() bool { return r.cfg.Enabled() }

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

// redactURL scrubs sensitive query-string keys from a raw URL, returning a
// best-effort safe value for url.full. Unparseable input is returned unchanged
// minus any query string, so we never leak values.
func redactURL(raw string, keys []string) string {
	u, err := url.Parse(raw)
	if err != nil {
		// Strip anything after `?` defensively rather than risk leaking values.
		if i := strings.IndexByte(raw, '?'); i >= 0 {
			return raw[:i]
		}
		return raw
	}
	if u.RawQuery == "" {
		return u.String()
	}

	deny := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		deny[strings.ToLower(k)] = struct{}{}
	}

	q := u.Query()
	for k := range q {
		if _, bad := deny[strings.ToLower(k)]; bad {
			q.Set(k, "REDACTED")
		}
	}
	u.RawQuery = q.Encode()
	return u.String()
}
