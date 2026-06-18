# restlytics Go SDK

Framework-native request tracing for Go. Emits the shared restlytics OTLP/JSON
wire format (see [`../SPEC.md`](../SPEC.md)) — one contract, every language.

- **One trace per request**: a root SERVER span plus CLIENT child spans for each
  DB query / outbound HTTP call / cache op.
- **Self-time** (`restlytics.self_ns.{db,http,cache,app}`) computed via
  interval-union so overlapping children don't over-count.
- **Fire-and-forget**: spans are buffered in-request and flushed *after* the
  response, gzipped, with a ~2s timeout and all errors swallowed. The host app is
  never blocked and never sees a panic from the SDK.
- **Dependency-free core**: the root package uses only the Go standard library,
  so it compiles offline. Framework adapters live in subpackages.

## Install

```sh
go get github.com/restlytics/restlytics-go
```

Go 1.21+.

## Configuration

Pass a `Config` to `Init`, or leave fields empty and configure via environment:

| Env var | Default | Meaning |
|---|---|---|
| `RESTLYTICS_KEY` | _(empty → SDK disabled)_ | Project ingest key (`X-Restlytics-Key`) |
| `RESTLYTICS_INGEST_URL` | `https://ingest.restlytics.com` | Base URL; POSTs to `{url}/v1/traces` |
| `RESTLYTICS_SERVICE_NAME` | `go-app` | `service.name` resource attribute |
| `RESTLYTICS_ENV` | `production` | `deployment.environment` |
| `RESTLYTICS_SAMPLE_RATE` | `1.0` | Head-based trace-id-ratio sample rate |
| `RESTLYTICS_TRANSPORT` | `http` | `http` / `null` / `log` |
| `RESTLYTICS_TIMEOUT_MS` | `2000` | Send timeout (ms) |
| `RESTLYTICS_CAPTURE_SQL` | `false` | Send raw `db.query.text` (capped 2048) |
| `RESTLYTICS_MAX_SPANS` | `2000` | Per-request child span cap |
| `RESTLYTICS_INSTRUMENT_DB` / `_HTTP` / `_CACHE` | `true` | Per-instrument toggles |

```go
rl := restlytics.Init(restlytics.Config{
    Key:         "rl_xxx",          // or RESTLYTICS_KEY
    ServiceName: "checkout",
    Environment: "production",
    SampleRate:  1.0,
})
```

A missing key yields a no-op SDK — safe to ship before a key is provisioned.

## net/http

```go
rl := restlytics.Init(restlytics.Config{})
mux := http.NewServeMux()
mux.HandleFunc("/users/", func(w http.ResponseWriter, r *http.Request) {
    // Provide the route TEMPLATE so http.route stays low-cardinality:
    restlytics.SetRoute(r.Context(), "/users/{id}")
    w.Write([]byte("ok"))
})
http.ListenAndServe(":8080", rl.Middleware()(mux))
```

> The plain net/http middleware can't see the matched route template at the
> Go 1.21 baseline, so call `restlytics.SetRoute(ctx, "/users/{id}")` from your
> handler (or use a framework adapter) to avoid raw URLs in `http.route`.

## Frameworks (adapter subpackages)

Each adapter imports its framework and supplies the real route template. They are
separate modules-worth of imports and are **not** part of the dependency-free core.

```go
// Gin — github.com/restlytics/restlytics-go/adapters/gin
r.Use(restlyticsgin.Middleware(rl)) // route from c.FullPath()

// Echo — github.com/restlytics/restlytics-go/adapters/echo
e.Use(restlyticsecho.Middleware(rl)) // route from c.Path()

// Fiber — github.com/restlytics/restlytics-go/adapters/fiber
app.Use(restlyticsfiber.Middleware(rl)) // route from c.Route().Path
```

## Database

### database/sql

Wrap a `driver.Connector` (or driver) so every query in a traced request becomes
a `db` child span. Only the query text + binding **count** are recorded — binding
values are never captured.

```go
connector, _ := pq.NewConnector(dsn)
db := restlytics.OpenDB(connector, "postgresql", rl)
// pass the request context through so spans attach to the trace:
db.QueryContext(r.Context(), "SELECT * FROM users WHERE id = $1", id)
```

### GORM

```go
import restlyticsgorm "github.com/restlytics/restlytics-go/adapters/gormr"

db.Use(restlyticsgorm.New(rl, "postgresql"))
// queries run with WithContext(r.Context()) attach to the active trace.
```

## Safety & redaction

- Bindings are **counted, never sent**. SQL is normalized to a literal-free
  `db.query.summary` (the N+1 grouping key).
- Outbound `url.full` query strings are scrubbed; sensitive headers
  (`authorization`, `cookie`, `x-api-key`, …) are never captured. No bodies.
- Per-request state is isolated via `context.Context` — no shared singleton, safe
  under concurrency.
- The in-request buffer is capped (default 2000 spans).

## License

MIT — see [`LICENSE`](./LICENSE).
