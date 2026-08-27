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
- **Opt-in native logs**: a composable `slog.Handler` exports WARN+ records as
  OTLP/JSON to `/v1/logs`, automatically correlated to the active trace.
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
| `RESTLYTICS_INGEST_URL` | `https://ingest.restlytics.com` | Base URL; POSTs to `{url}/v1/traces` and `{url}/v1/logs` |
| `RESTLYTICS_SERVICE_NAME` | `go-app` | `service.name` resource attribute |
| `RESTLYTICS_ENV` | `production` | `deployment.environment` |
| `RESTLYTICS_SAMPLE_RATE` | `1.0` | Head-based trace-id-ratio sample rate |
| `RESTLYTICS_TRANSPORT` | `http` | `http` / `preview` / `null` / `log` |
| `RESTLYTICS_TIMEOUT_MS` | `2000` | Send timeout (ms) |
| `RESTLYTICS_CAPTURE_SQL` | `false` | Send raw `db.query.text` (capped 2048) |
| `RESTLYTICS_MAX_SPANS` | `2000` | Per-request child span cap |
| `RESTLYTICS_LOGS` | `false` | Enable native `log/slog` export to `/v1/logs` |
| `RESTLYTICS_LOGS_MIN_SEVERITY` | `13` | Minimum OTel severity number (`13` = WARN, `17` = ERROR) |
| `RESTLYTICS_INSTRUMENT_DB` / `_HTTP` / `_CACHE` | `true` | Per-instrument toggles |

Before connecting production data, set `RESTLYTICS_TRANSPORT=preview` and drive
one representative request. Preview mode needs no ingest key, never opens a
socket, and writes a structured report with `networkRequestMade: false`, the
post-redaction production payload, configured sampling rate, span count, and
JSON/gzip byte sizes. Use `RESTLYTICS_SAMPLE_RATE=1` for a deterministic review.

## Delivery reliability and shutdown

`HTTPTransport` owns one worker goroutine and one fixed 64-batch channel shared
by traces and logs. `Send` and `SendLogs` only attempt a non-blocking enqueue;
saturation drops the new batch instead of blocking or spawning goroutines, and
delivery is never retried. Use
`TransportDiagnostics()` for payload-free accepted/delivered/dropped/failed
counters and shut down the SDK with a bounded context:

```go
health, available := rl.TransportDiagnostics()
if available {
    log.Printf("restlytics drops=%d failures=%d", health.DroppedBatches, health.FailedBatches)
}

ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
defer cancel()
if !rl.Shutdown(ctx) {
    log.Print("restlytics shutdown exceeded its deadline or exporter cleanup failed")
}
```

```go
rl := restlytics.Init(restlytics.Config{
    Key:         "rl_xxx",          // or RESTLYTICS_KEY
    ServiceName: "checkout",
    Environment: "production",
    SampleRate:  1.0,
})
```

A missing key yields a no-op SDK with the default HTTP transport — safe to ship
before a key is provisioned. Preview and custom exporters intentionally work
without a Restlytics ingest key.

## Custom exporter

Use `Config.CustomExporter` to route the same source-redacted production OTLP
trace and log payloads to a collector, another provider, or a durable customer
sink. The exporter is independent of Restlytics ingest: it does not require a
Restlytics key, and the SDK does not pass a key, header, or tenant identity to
callbacks.

```go
type collectorExporter struct {
    // customer-owned client or buffer
}

func (e *collectorExporter) ExportTraces(
    ctx context.Context,
    payload restlytics.ExportTraceServiceRequest,
) error {
    return e.send(ctx, "/v1/traces", payload)
}

func (e *collectorExporter) ExportLogs(
    ctx context.Context,
    payload restlytics.ExportLogsServiceRequest,
) error {
    return e.send(ctx, "/v1/logs", payload)
}

// Optional: implement restlytics.ExporterFlusher and
// restlytics.ExporterShutdown when the customer sink owns buffers/resources.
func (e *collectorExporter) Flush(ctx context.Context) error { return e.flush(ctx) }
func (e *collectorExporter) Shutdown(ctx context.Context) error { return e.close(ctx) }

exporter := &collectorExporter{}
rl := restlytics.Init(restlytics.Config{
    CustomExporter: exporter,
    ServiceName:    "checkout",
    Environment:    "production",
    Logs:           true,
})
```

`Exporter.ExportTraces` and `Exporter.ExportLogs` run on the SDK's single
fixed-size worker queue, never on the instrumented request/job/log path. Each
call receives a `Config.TimeoutMs`-bounded context. Returned errors and panics
are swallowed and reflected in `TransportDiagnostics`; saturation drops new
batches. `rl.Flush(ctx)` and `rl.Shutdown(ctx)` are bounded, return `false` for
deadline or optional lifecycle-hook failure, and never panic into the host.
Exporter implementations should honor cancellation; a callback that ignores its
context can occupy the one worker until it returns, while host work remains
non-blocking and the fixed queue safely drops overflow.

The older `Config.CustomTransport` and `Config.CustomLogsTransport` fields remain
source-compatible but are deprecated. `CustomExporter` is the unified contract
for new integrations and takes precedence when both are configured.

## Native `log/slog` export

Log export is disabled by default. Enable it with `RESTLYTICS_LOGS=true` (or
`Config.Logs`) and wrap the handler your application already uses:

```go
rl := restlytics.Init(restlytics.Config{})
appHandler := slog.NewJSONHandler(os.Stdout, nil)
logger := slog.New(rl.SlogHandler(appHandler))

// Context-aware methods attach traceId, spanId, and sampled flags.
logger.ErrorContext(r.Context(), "checkout failed", "order.id", orderID)
```

The original record is still delivered unchanged to `appHandler`; only the
exported copy is scrubbed. Standard slog levels map deterministically to OTel
`DEBUG=5`, `INFO=9`, `WARN=13`, and `ERROR=17`. Custom levels above ERROR map to
`ERROR2=18` and `FATAL=21`. Logs are not trace-sampled: a qualifying ERROR in an
unsampled request is still sent with that request's trace/span ids and flags.
Outside a Restlytics context, correlation fields are omitted.

Each exported record is bounded to an 8 KiB message and at most 64 structured
attributes (2 KiB per string value). The source scrubber removes recognizable
credentials, emails, private keys, URL credentials/fragments/query values, and
request/response or binding content; attributes with sensitive keys and
arbitrary objects/errors fail closed. The transport remains a fixed-size,
non-blocking queue, and every capture or delivery failure is swallowed.

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

## Background jobs, commands, and schedules

Pass the context returned to the operation through DB and HTTP calls. Names must
be stable handler/signature names—never job ids, arguments, or payload data.

```go
carrier := restlytics.QueueCarrier{"invoice_id": invoiceID}
_ = restlytics.Enqueue(ctx, restlytics.EnqueueOptions{
    System: "redis", Destination: "billing",
}, carrier, queue.Publish)

err := rl.RunJob(context.Background(), restlytics.JobOptions{
    Name: "billing.reconcile", System: "redis", Destination: "billing",
    Attempt: attempt,
    Traceparent: carrier["__restlytics"].(map[string]string)["traceparent"],
}, func(jobCtx context.Context) error {
    return reconcile(jobCtx, carrier)
})
```

`RunCommand` captures the returned exit code and `RunSchedule` captures the cron
expression. Jobs are `CONSUMER` roots; commands and schedules are `SERVER`
roots. The namespaced carrier preserves trace and sampling continuity, the job
links to its enqueue span, and enqueue I/O is isolated in
`restlytics.self_ns.queue`. Failures never export exception or payload content.

### GORM

```go
import restlyticsgorm "github.com/restlytics/restlytics-go/adapters/gormr"

db.Use(restlyticsgorm.New(rl, "postgresql"))
// queries run with WithContext(r.Context()) attach to the active trace.
```

## Safety & redaction

- Bindings are **counted, never sent**. SQL is normalized to a literal-free
  `db.query.summary` (the N+1 grouping key).
- Every outbound `url.full` query value is scrubbed; credentials/fragments, headers,
  bodies, and exception content are never exported.
- Native logs are opt-in and source-redacted before entering the bounded
  transport; use context-aware slog methods for automatic trace correlation.
- Per-request state is isolated via `context.Context` — no shared singleton, safe
  under concurrency.
- The in-request buffer is capped (default 2000 spans).

## Cross-language conformance

CI pins [`restlytics/sdk-conformance@v1.1.0`](https://github.com/restlytics/sdk-conformance)
and compares the vendored fixture before testing. The suite proves exact semantic OTLP output,
W3C propagation, root sampling, source redaction, and error-status behavior shared by all seven SDKs.
The release gate also boots a real Gin application and sends its request telemetry over gzip HTTP to a
deployed-compatible ingest server. It proves route templates, trace continuation, 202/503 handling, error
status, and that the project key plus request secrets stay out of the payload. Gin is beta-validated;
net/http, Echo, Fiber, and GORM remain preview until they pass the same real-app gate.

## License

MIT — see [`LICENSE`](./LICENSE).
