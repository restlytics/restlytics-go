package restlytics

import "strconv"

// A single span, accumulated in-request and serialized to OTLP/JSON on flush.
//
// Timestamps are kept as int64 nanoseconds internally and only stringified at
// serialization time — the OTLP/JSON contract requires *UnixNano fields to be
// decimal STRINGS (to preserve 64-bit precision through JSON).
//
// Attributes are kept in insertion order so the wire output is deterministic.

// Span is a mutable in-request span. Not safe for concurrent mutation; each
// span is owned by the goroutine handling its request (children are added under
// the tracer's lock).
type Span struct {
	TraceID       string
	SpanID        string
	ParentSpanID  string
	Name          string
	Kind          int
	StartUnixNano int64
	EndUnixNano   int64

	// Ordered attribute storage. keys preserves insertion order; attrs maps a
	// key to its index in keys/values so a re-set updates in place.
	keys   []string
	values []AnyValue
	index  map[string]int

	statusCode    int
	statusMessage string
}

func newSpan(traceID, spanID, parentSpanID, name string, kind int, start, end int64) *Span {
	return &Span{
		TraceID:       traceID,
		SpanID:        spanID,
		ParentSpanID:  parentSpanID,
		Name:          name,
		Kind:          kind,
		StartUnixNano: start,
		EndUnixNano:   end,
		index:         map[string]int{},
		statusCode:    StatusUnset,
	}
}

// SetName updates the span name.
func (s *Span) SetName(name string) *Span {
	s.Name = name
	return s
}

// SetEnd sets the span end time (epoch nanoseconds).
func (s *Span) SetEnd(end int64) *Span {
	s.EndUnixNano = end
	return s
}

func (s *Span) setAttr(key string, v AnyValue) {
	if i, ok := s.index[key]; ok {
		s.values[i] = v
		return
	}
	s.index[key] = len(s.keys)
	s.keys = append(s.keys, key)
	s.values = append(s.values, v)
}

// SetString records a string attribute.
func (s *Span) SetString(key, value string) *Span {
	s.setAttr(key, StringValue(value))
	return s
}

// SetInt records an int attribute. Serialized as intValue (a STRING).
func (s *Span) SetInt(key string, value int64) *Span {
	s.setAttr(key, IntValue(value))
	return s
}

// SetDouble records a double attribute.
func (s *Span) SetDouble(key string, value float64) *Span {
	s.setAttr(key, DoubleValue(value))
	return s
}

// SetBool records a boolean attribute.
func (s *Span) SetBool(key string, value bool) *Span {
	s.setAttr(key, BoolValue(value))
	return s
}

// SetStatus sets the span status. The message is capped to keep payloads bounded.
func (s *Span) SetStatus(code int, message string) *Span {
	s.statusCode = code
	if message != "" {
		if len(message) > 1024 {
			message = message[:1024]
		}
		s.statusMessage = message
	}
	return s
}

// StatusCode returns the current status code.
func (s *Span) StatusCode() int {
	return s.statusCode
}

// category reads the span's restlytics.category attribute, defaulting to "app".
func (s *Span) category() string {
	if i, ok := s.index[AttrCategory]; ok {
		if v := s.values[i].StringValue; v != nil {
			switch *v {
			case CategoryDB, CategoryHTTP, CategoryCache, CategoryApp:
				return *v
			}
		}
	}
	return CategoryApp
}

// DurationNs returns the span duration in nanoseconds (clamped non-negative).
func (s *Span) DurationNs() int64 {
	d := s.EndUnixNano - s.StartUnixNano
	if d < 0 {
		return 0
	}
	return d
}

// toData serializes to the OTLP/JSON SpanData shape.
func (s *Span) toData() SpanData {
	d := SpanData{
		TraceID:           s.TraceID,
		SpanID:            s.SpanID,
		Name:              s.Name,
		Kind:              s.Kind,
		StartTimeUnixNano: strconv.FormatInt(s.StartUnixNano, 10),
		EndTimeUnixNano:   strconv.FormatInt(s.EndUnixNano, 10),
	}

	// parentSpanId is omitted/empty for the root SERVER span.
	if s.ParentSpanID != "" {
		d.ParentSpanID = s.ParentSpanID
	}

	if len(s.keys) > 0 {
		attrs := make([]KeyValue, len(s.keys))
		for i, k := range s.keys {
			attrs[i] = KeyValue{Key: k, Value: s.values[i]}
		}
		d.Attributes = attrs
	}

	// Only attach status when it carries signal (OK/ERROR); UNSET is the default.
	if s.statusCode != StatusUnset {
		d.Status = &Status{Code: s.statusCode, Message: s.statusMessage}
	}

	return d
}
