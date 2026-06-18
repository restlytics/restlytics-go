package restlytics

import "strconv"

// OTLP/JSON wire format (subset) — must match packages/contract/src/otlp.ts and
// the ingestion service exactly.
//
// The three classic footguns enforced here:
//   - trace/span ids are lowercase hex of the right length, never all-zero
//   - all *UnixNano fields are decimal STRINGS (64-bit safe in JSON)
//   - intValue is a STRING, not a JSON number

// AnyValue is the OTLP AnyValue wrapper. Exactly one field is set per value.
// All fields are pointers/omitempty so JSON emits only the populated one.
type AnyValue struct {
	StringValue *string  `json:"stringValue,omitempty"`
	BoolValue   *bool    `json:"boolValue,omitempty"`
	IntValue    *string  `json:"intValue,omitempty"` // int64 as a (signed) decimal STRING
	DoubleValue *float64 `json:"doubleValue,omitempty"`
}

// KeyValue is a single OTLP attribute.
type KeyValue struct {
	Key   string   `json:"key"`
	Value AnyValue `json:"value"`
}

// StringValue builds a string AnyValue.
func StringValue(v string) AnyValue {
	s := v
	return AnyValue{StringValue: &s}
}

// IntValue builds an int AnyValue. Per the contract the int is serialized as a
// decimal STRING.
func IntValue(v int64) AnyValue {
	s := strconv.FormatInt(v, 10)
	return AnyValue{IntValue: &s}
}

// BoolValue builds a boolean AnyValue.
func BoolValue(v bool) AnyValue {
	b := v
	return AnyValue{BoolValue: &b}
}

// DoubleValue builds a double AnyValue.
func DoubleValue(v float64) AnyValue {
	f := v
	return AnyValue{DoubleValue: &f}
}

// Status is the OTLP span status. Code: 0 UNSET, 1 OK, 2 ERROR.
type Status struct {
	Code    int    `json:"code"`
	Message string `json:"message,omitempty"`
}

// SpanData is the OTLP/JSON Span shape the ingestion contract validates.
type SpanData struct {
	TraceID           string     `json:"traceId"`
	SpanID            string     `json:"spanId"`
	ParentSpanID      string     `json:"parentSpanId,omitempty"`
	Name              string     `json:"name"`
	Kind              int        `json:"kind"`
	StartTimeUnixNano string     `json:"startTimeUnixNano"`
	EndTimeUnixNano   string     `json:"endTimeUnixNano"`
	Attributes        []KeyValue `json:"attributes,omitempty"`
	Status            *Status    `json:"status,omitempty"`
}

// InstrumentationScope identifies the SDK that produced the spans.
type InstrumentationScope struct {
	Name    string `json:"name,omitempty"`
	Version string `json:"version,omitempty"`
}

// ScopeSpans groups spans under one instrumentation scope.
type ScopeSpans struct {
	Scope InstrumentationScope `json:"scope"`
	Spans []SpanData           `json:"spans"`
}

// Resource carries service + SDK identity attributes.
type Resource struct {
	Attributes []KeyValue `json:"attributes,omitempty"`
}

// ResourceSpans groups scopeSpans under one resource.
type ResourceSpans struct {
	Resource   Resource     `json:"resource"`
	ScopeSpans []ScopeSpans `json:"scopeSpans"`
}

// ExportTraceServiceRequest is the top-level OTLP body for POST /v1/traces.
type ExportTraceServiceRequest struct {
	ResourceSpans []ResourceSpans `json:"resourceSpans"`
}

// SDK identity constants surfaced as resource attributes and the scope name.
const (
	SDKName     = "restlytics-go"
	SDKLanguage = "go"
	SDKVersion  = "0.1.0"
)

// BuildPayload assembles an ExportTraceServiceRequest from the request's spans.
func BuildPayload(serviceName, environment string, spans []SpanData) ExportTraceServiceRequest {
	return ExportTraceServiceRequest{
		ResourceSpans: []ResourceSpans{
			{
				Resource: Resource{
					Attributes: resourceAttributes(serviceName, environment),
				},
				ScopeSpans: []ScopeSpans{
					{
						Scope: InstrumentationScope{
							Name:    SDKName,
							Version: SDKVersion,
						},
						Spans: spans,
					},
				},
			},
		},
	}
}

func resourceAttributes(serviceName, environment string) []KeyValue {
	return []KeyValue{
		{Key: AttrServiceName, Value: StringValue(serviceName)},
		{Key: AttrDeploymentEnvironment, Value: StringValue(environment)},
		{Key: "telemetry.sdk.name", Value: StringValue(SDKName)},
		{Key: "telemetry.sdk.language", Value: StringValue(SDKLanguage)},
		{Key: "telemetry.sdk.version", Value: StringValue(SDKVersion)},
	}
}

// Semantic attribute keys (from packages/contract/src/attributes.ts).
const (
	// Resource
	AttrServiceName           = "service.name"
	AttrDeploymentEnvironment = "deployment.environment"

	// HTTP (OTel stable)
	AttrHTTPRequestMethod      = "http.request.method"
	AttrHTTPRoute              = "http.route" // TEMPLATE, e.g. /users/{id} — never the raw URL
	AttrHTTPResponseStatusCode = "http.response.status_code"
	AttrURLFull                = "url.full"
	AttrURLPath                = "url.path"
	AttrServerAddress          = "server.address"

	// DB (OTel stable, renamed set)
	AttrDBSystem        = "db.system.name"
	AttrDBNamespace     = "db.namespace"
	AttrDBQueryText     = "db.query.text"    // raw (capped, per-project toggle)
	AttrDBQuerySummary  = "db.query.summary" // normalized — the N+1 grouping key
	AttrDBOperationName = "db.operation.name"

	// restlytics extensions
	AttrBindingsCount = "restlytics.bindings_count"
	AttrDBQueryCount  = "restlytics.db_query_count"
	AttrCategory      = "restlytics.category"
	AttrSelfNsDB      = "restlytics.self_ns.db"
	AttrSelfNsHTTP    = "restlytics.self_ns.http"
	AttrSelfNsCache   = "restlytics.self_ns.cache"
	AttrSelfNsApp     = "restlytics.self_ns.app"
)

// Span kinds and status codes (OTLP enums).
const (
	KindInternal = 1
	KindServer   = 2
	KindClient   = 3
	KindProducer = 4
	KindConsumer = 5

	StatusUnset = 0
	StatusOK    = 1
	StatusError = 2
)

// restlytics.category values.
const (
	CategoryApp   = "app"
	CategoryDB    = "db"
	CategoryHTTP  = "http"
	CategoryCache = "cache"
	CategoryQueue = "queue"
	CategoryOther = "other"
)
