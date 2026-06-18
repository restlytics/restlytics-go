package restlytics

import (
	"crypto/rand"
	"encoding/hex"
	"strconv"
	"strings"
)

// Trace / span id generation and W3C traceparent handling.
//
// OTLP/JSON requires lowercase-hex ids: 32 chars (16 bytes) for a trace id,
// 16 chars (8 bytes) for a span id. The ingestion contract additionally rejects
// all-zero ids, so we make sure the random bytes are never empty.

// NewTraceID returns 32 lowercase hex chars (16 random bytes), never all-zero.
func NewTraceID() string {
	return randomHex(16)
}

// NewSpanID returns 16 lowercase hex chars (8 random bytes), never all-zero.
func NewSpanID() string {
	return randomHex(8)
}

func randomHex(n int) string {
	b := make([]byte, n)
	for {
		// crypto/rand.Read never returns short reads on supported platforms;
		// if it ever errored we'd still fall through to the all-zero guard.
		_, _ = rand.Read(b)
		if !allZero(b) {
			return hex.EncodeToString(b)
		}
	}
}

func allZero(b []byte) bool {
	for _, x := range b {
		if x != 0 {
			return false
		}
	}
	return true
}

func isAllZeroHex(s string) bool {
	if s == "" {
		return true
	}
	for i := 0; i < len(s); i++ {
		if s[i] != '0' {
			return false
		}
	}
	return true
}

// Traceparent holds a parsed W3C traceparent header.
type Traceparent struct {
	TraceID      string
	ParentSpanID string
	Sampled      bool
}

// ParseTraceparent parses a W3C `traceparent` header.
//
// Format: `version-traceid-spanid-flags`, e.g.
//
//	00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
//
// Returns ok=false when absent or malformed so the caller falls back to a fresh
// trace. Continuing an incoming traceparent stitches a single distributed trace
// together across services.
func ParseTraceparent(header string) (Traceparent, bool) {
	h := strings.ToLower(strings.TrimSpace(header))
	if h == "" {
		return Traceparent{}, false
	}

	parts := strings.Split(h, "-")
	if len(parts) != 4 {
		return Traceparent{}, false
	}
	version, traceID, spanID, flags := parts[0], parts[1], parts[2], parts[3]

	if len(version) != 2 || len(traceID) != 32 || len(spanID) != 16 || len(flags) != 2 {
		return Traceparent{}, false
	}
	if !isHex(version) || !isHex(traceID) || !isHex(spanID) || !isHex(flags) {
		return Traceparent{}, false
	}
	// Reject the invalid all-zero trace/parent ids per the W3C spec.
	if isAllZeroHex(traceID) || isAllZeroHex(spanID) {
		return Traceparent{}, false
	}

	flagsByte, err := strconv.ParseUint(flags, 16, 8)
	if err != nil {
		return Traceparent{}, false
	}

	return Traceparent{
		TraceID:      traceID,
		ParentSpanID: spanID,
		// Low bit of the flags byte is the "sampled" flag.
		Sampled: flagsByte&0x01 == 0x01,
	}, true
}

// FormatTraceparent builds a W3C `traceparent` value for outbound injection.
func FormatTraceparent(traceID, spanID string, sampled bool) string {
	flags := "00"
	if sampled {
		flags = "01"
	}
	return "00-" + traceID + "-" + spanID + "-" + flags
}

func isHex(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
