package restlytics

import (
	"context"
	"log/slog"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// OpenTelemetry log severity numbers used by the shared Restlytics contract.
const (
	SeverityDebug    = 5
	SeverityInfo     = 9
	SeverityInfo2    = 10
	SeverityWarn     = 13
	SeverityError    = 17
	SeverityError2   = 18
	SeverityFatal    = 21
	maxLogBodyBytes  = 8192
	maxLogAttrBytes  = 2048
	maxLogAttributes = 64
	maxLogGroupDepth = 8
)

// LogRecordData is the OTLP/JSON LogRecord subset accepted by /v1/logs.
type LogRecordData struct {
	TimeUnixNano           string     `json:"timeUnixNano"`
	ObservedTimeUnixNano   string     `json:"observedTimeUnixNano,omitempty"`
	SeverityNumber         int        `json:"severityNumber"`
	SeverityText           string     `json:"severityText,omitempty"`
	Body                   AnyValue   `json:"body"`
	Attributes             []KeyValue `json:"attributes,omitempty"`
	DroppedAttributesCount int        `json:"droppedAttributesCount,omitempty"`
	Flags                  *int       `json:"flags,omitempty"`
	TraceID                string     `json:"traceId,omitempty"`
	SpanID                 string     `json:"spanId,omitempty"`
}

// ScopeLogs groups log records under one instrumentation scope.
type ScopeLogs struct {
	Scope      InstrumentationScope `json:"scope"`
	LogRecords []LogRecordData      `json:"logRecords"`
}

// ResourceLogs groups scopeLogs under the same resource used by traces.
type ResourceLogs struct {
	Resource  Resource    `json:"resource"`
	ScopeLogs []ScopeLogs `json:"scopeLogs"`
}

// ExportLogsServiceRequest is the top-level OTLP body for POST /v1/logs.
type ExportLogsServiceRequest struct {
	ResourceLogs []ResourceLogs `json:"resourceLogs"`
}

// BuildLogsPayload assembles a logs request. It deliberately calls the same
// resourceAttributes builder and uses the same scope as BuildPayload.
func BuildLogsPayload(serviceName, environment string, records []LogRecordData) ExportLogsServiceRequest {
	return ExportLogsServiceRequest{
		ResourceLogs: []ResourceLogs{{
			Resource: Resource{Attributes: resourceAttributes(serviceName, environment)},
			ScopeLogs: []ScopeLogs{{
				Scope:      InstrumentationScope{Name: SDKName, Version: SDKVersion},
				LogRecords: records,
			}},
		}},
	}
}

// SlogSeverity maps log/slog levels into the deterministic severity buckets in
// the shared SDK contract. Standard slog levels map to DEBUG/INFO/WARN/ERROR;
// positive custom levels above ERROR map to ERROR2 and FATAL.
func SlogSeverity(level slog.Level) (number int, text string) {
	switch {
	case level < slog.LevelInfo:
		return SeverityDebug, "DEBUG"
	case level < slog.LevelWarn:
		return SeverityInfo, "INFO"
	case level < slog.LevelError:
		return SeverityWarn, "WARN"
	case level < slog.LevelError+4:
		return SeverityError, "ERROR"
	case level < slog.LevelError+8:
		return SeverityError2, "ERROR2"
	default:
		return SeverityFatal, "FATAL"
	}
}

// SlogHandler is a composable slog.Handler that sends a redacted copy to
// Restlytics and delegates the original record to the application's handler.
// Its capture path never returns an error or lets a panic escape.
type SlogHandler struct {
	next        slog.Handler
	transport   LogsTransport
	enabled     bool
	minSeverity int
	serviceName string
	environment string
	attrs       []slog.Attr
	groups      []string
}

// NewSlogHandler creates a Restlytics slog integration. A nil Restlytics handle
// is safe and simply leaves next unchanged.
func NewSlogHandler(rl *Restlytics, next slog.Handler) slog.Handler {
	h := &SlogHandler{next: next, transport: NullTransport{}, minSeverity: SeverityWarn}
	if rl == nil {
		return h
	}
	h.transport = rl.logsTransport
	if h.transport == nil {
		h.transport = NullTransport{}
	}
	h.enabled = rl.cfg.Logs && rl.cfg.Enabled()
	h.minSeverity = rl.cfg.LogsMinSeverity
	h.serviceName = rl.cfg.ServiceName
	h.environment = rl.cfg.Environment
	return h
}

// Enabled reports true when either the application handler or Restlytics wants
// the record. A low-severity record can therefore still reach next unchanged.
func (h *SlogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	number, _ := SlogSeverity(level)
	capture := h.enabled && number >= h.minSeverity
	if h.next == nil {
		return capture
	}
	return capture || h.next.Enabled(ctx, level)
}

// Handle delegates the original record and independently captures a redacted
// copy. Restlytics failures are swallowed; an error from next remains next's.
func (h *SlogHandler) Handle(ctx context.Context, record slog.Record) error {
	h.captureSafely(ctx, record)
	if h.next != nil && h.next.Enabled(ctx, record.Level) {
		return h.next.Handle(ctx, record)
	}
	return nil
}

// WithAttrs returns an immutable handler clone as required by slog.Handler.
func (h *SlogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	clone := h.clone()
	clone.attrs = append(clone.attrs, attrs...)
	if h.next != nil {
		clone.next = h.next.WithAttrs(attrs)
	}
	return clone
}

// WithGroup returns an immutable handler clone as required by slog.Handler.
func (h *SlogHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	clone := h.clone()
	clone.groups = append(clone.groups, name)
	if h.next != nil {
		clone.next = h.next.WithGroup(name)
	}
	return clone
}

func (h *SlogHandler) clone() *SlogHandler {
	clone := *h
	clone.attrs = append([]slog.Attr(nil), h.attrs...)
	clone.groups = append([]string(nil), h.groups...)
	return &clone
}

func (h *SlogHandler) captureSafely(ctx context.Context, record slog.Record) {
	defer func() { _ = recover() }()
	if !h.enabled || h.transport == nil {
		return
	}
	severityNumber, severityText := SlogSeverity(record.Level)
	if severityNumber < h.minSeverity {
		return
	}

	eventTime := record.Time
	observedTime := time.Now()
	if eventTime.IsZero() {
		eventTime = observedTime
	}
	attrs, dropped := h.recordAttributes(record)
	logRecord := LogRecordData{
		TimeUnixNano:           strconv.FormatInt(eventTime.UnixNano(), 10),
		ObservedTimeUnixNano:   strconv.FormatInt(observedTime.UnixNano(), 10),
		SeverityNumber:         severityNumber,
		SeverityText:           severityText,
		Body:                   StringValue(truncateLogText(redactLogText(record.Message), maxLogBodyBytes)),
		Attributes:             attrs,
		DroppedAttributesCount: dropped,
	}

	traceID := CurrentTraceID(ctx)
	spanID := CurrentSpanID(ctx)
	if validCorrelationID(traceID, 32) && validCorrelationID(spanID, 16) {
		logRecord.TraceID = traceID
		logRecord.SpanID = spanID
		flags, _ := CurrentTraceFlags(ctx)
		logRecord.Flags = &flags
	}

	h.transport.SendLogs(BuildLogsPayload(h.serviceName, h.environment, []LogRecordData{logRecord}))
}

func validCorrelationID(value string, length int) bool {
	return len(value) == length && isHex(value) && !isAllZeroHex(value)
}

func (h *SlogHandler) recordAttributes(record slog.Record) ([]KeyValue, int) {
	attrs := make([]KeyValue, 0, minInt(record.NumAttrs()+len(h.attrs), maxLogAttributes))
	dropped := 0
	for _, attr := range h.attrs {
		appendLogAttr(&attrs, &dropped, h.groups, attr, 0)
	}
	record.Attrs(func(attr slog.Attr) bool {
		appendLogAttr(&attrs, &dropped, h.groups, attr, 0)
		return true
	})
	return attrs, dropped
}

func appendLogAttr(out *[]KeyValue, dropped *int, groups []string, attr slog.Attr, depth int) {
	if depth > maxLogGroupDepth {
		*dropped = *dropped + 1
		return
	}
	value := attr.Value.Resolve()
	if value.Kind() == slog.KindGroup {
		nextGroups := groups
		if attr.Key != "" {
			nextGroups = append(append([]string(nil), groups...), attr.Key)
		}
		for _, child := range value.Group() {
			appendLogAttr(out, dropped, nextGroups, child, depth+1)
		}
		return
	}
	if attr.Key == "" || len(*out) >= maxLogAttributes {
		*dropped = *dropped + 1
		return
	}
	key := strings.Join(append(append([]string(nil), groups...), attr.Key), ".")
	if len(key) > 256 || isSensitiveLogAttributeKey(key) {
		*dropped = *dropped + 1
		return
	}
	converted, ok := logAnyValue(value)
	if !ok {
		*dropped = *dropped + 1
		return
	}
	*out = append(*out, KeyValue{Key: key, Value: converted})
}

func logAnyValue(value slog.Value) (AnyValue, bool) {
	switch value.Kind() {
	case slog.KindString:
		return StringValue(truncateLogText(redactLogText(value.String()), maxLogAttrBytes)), true
	case slog.KindBool:
		return BoolValue(value.Bool()), true
	case slog.KindInt64:
		return IntValue(value.Int64()), true
	case slog.KindUint64:
		value := value.Uint64()
		if value <= math.MaxInt64 {
			return IntValue(int64(value)), true
		}
		return StringValue(strconv.FormatUint(value, 10)), true
	case slog.KindFloat64:
		return DoubleValue(value.Float64()), true
	case slog.KindDuration:
		return IntValue(int64(value.Duration())), true
	case slog.KindTime:
		return StringValue(value.Time().UTC().Format(time.RFC3339Nano)), true
	case slog.KindAny:
		switch value := value.Any().(type) {
		case string:
			return StringValue(truncateLogText(redactLogText(value), maxLogAttrBytes)), true
		case time.Time:
			return StringValue(value.UTC().Format(time.RFC3339Nano)), true
		case time.Duration:
			return IntValue(int64(value)), true
		default:
			// Arbitrary objects, errors, byte buffers, and Stringers can contain
			// request bodies or exception content. Fail closed instead of calling
			// their potentially unsafe String methods.
			return AnyValue{}, false
		}
	default:
		return AnyValue{}, false
	}
}

func isSensitiveLogAttributeKey(key string) bool {
	if IsSensitiveAttributeKey(key) {
		return true
	}
	normalized := strings.NewReplacer("-", ".", "_", ".").Replace(strings.ToLower(strings.TrimSpace(key)))
	for _, segment := range strings.Split(normalized, ".") {
		switch segment {
		case "err", "error", "errors", "exception", "exceptions", "binding", "bindings":
			return true
		}
	}
	return false
}

var (
	privateKeyPattern  = regexp.MustCompile(`(?is)-----BEGIN [^-]*PRIVATE KEY-----.*?-----END [^-]*PRIVATE KEY-----`)
	authTokenPattern   = regexp.MustCompile(`(?i)\b(?:Bearer|Basic)\s+[A-Za-z0-9+/_=.-]+`)
	jwtPattern         = regexp.MustCompile(`\b[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b`)
	emailPattern       = regexp.MustCompile(`(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b`)
	secretPairPattern  = regexp.MustCompile(`(?i)\b(authorization|cookie|password|passwd|secret|token|access_token|refresh_token|api[_-]?key|credential)s?\b\s*[:=]\s*([^\s,;&]+)`)
	contentPattern     = regexp.MustCompile(`(?i)\b(request|response)[ _-]?(body|payload)\b\s*[:=]\s*[^\r\n]+`)
	exceptionPattern   = regexp.MustCompile(`(?i)\b(exception|stack(?:trace)?|bindings?)\b\s*[:=]\s*[^\r\n]+`)
	absoluteURLPattern = regexp.MustCompile(`(?i)https?://[^\s"'<>]+`)
)

func redactLogText(raw string) string {
	if raw == "" {
		return raw
	}
	redacted := privateKeyPattern.ReplaceAllString(raw, "[REDACTED]")
	// Parse complete URLs before generic email/credential patterns can make
	// their authority component syntactically invalid and strand a username.
	redacted = absoluteURLPattern.ReplaceAllStringFunc(redacted, func(value string) string {
		return redactURL(value, nil)
	})
	redacted = authTokenPattern.ReplaceAllString(redacted, "[REDACTED]")
	redacted = jwtPattern.ReplaceAllString(redacted, "[REDACTED]")
	redacted = emailPattern.ReplaceAllString(redacted, "[REDACTED]")
	redacted = secretPairPattern.ReplaceAllString(redacted, "$1=[REDACTED]")
	redacted = contentPattern.ReplaceAllString(redacted, "[REDACTED]")
	redacted = exceptionPattern.ReplaceAllString(redacted, "[REDACTED]")
	return redacted
}

func truncateLogText(value string, maxBytes int) string {
	if len(value) <= maxBytes {
		return value
	}
	value = value[:maxBytes]
	for !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return value
}

func minInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}
