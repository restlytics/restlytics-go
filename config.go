package restlytics

import (
	"os"
	"strconv"
	"strings"
)

// Config is the native config surface. Resolve fills zero-value fields from the
// RESTLYTICS_* environment variables (SPEC §7) so callers can pass an empty
// Config and rely on the environment.
type Config struct {
	// Key is the project ingest key, sent as X-Restlytics-Key. When empty the
	// SDK quietly disables itself.
	Key string
	// IngestURL is the base URL; the SDK POSTs to {IngestURL}/v1/traces.
	IngestURL string
	// ServiceName / Environment become resource attributes.
	ServiceName string
	Environment string
	// SampleRate is the head-based trace-id-ratio sample rate (0.0–1.0).
	SampleRate float64
	// Transport selects the transport: "http"/"curl" (default), "null", "log".
	Transport string
	// TimeoutMs bounds the fire-and-forget send.
	TimeoutMs int
	// CaptureSQL controls whether raw db.query.text is sent (off by default).
	CaptureSQL bool
	// MaxSpans caps the in-request child buffer.
	MaxSpans int

	// Per-instrument toggles. nil pointer => default (on).
	InstrumentDB    *bool
	InstrumentHTTP  *bool
	InstrumentCache *bool

	// IgnorePaths are request paths to skip entirely (trailing `*` wildcard).
	IgnorePaths []string

	// RedactionQueryKeys are query-string keys scrubbed from url.full.
	RedactionQueryKeys []string

	// Transport lets callers inject a custom transport (e.g. in tests). When set
	// it overrides the Transport string selection.
	CustomTransport Transport
}

// Default values mirroring the Laravel reference config.
const (
	defaultIngestURL  = "https://ingest.restlytics.com"
	defaultService    = "go-app"
	defaultEnv        = "production"
	defaultSampleRate = 1.0
	defaultTransport  = "http"
	defaultTimeoutMs  = 2000
	defaultMaxSpans   = 2000
)

func defaultIgnorePaths() []string {
	return []string{"/up", "/health", "/healthz"}
}

func defaultRedactionKeys() []string {
	return []string{"token", "api_key", "apikey", "password", "secret", "access_token", "key", "signature"}
}

// Resolve returns a copy of c with empty fields filled from the environment and
// hardcoded defaults.
func (c Config) Resolve() Config {
	out := c

	if out.Key == "" {
		out.Key = os.Getenv("RESTLYTICS_KEY")
	}
	if out.IngestURL == "" {
		out.IngestURL = envOr("RESTLYTICS_INGEST_URL", defaultIngestURL)
	}
	if out.ServiceName == "" {
		out.ServiceName = envOr("RESTLYTICS_SERVICE_NAME", defaultService)
	}
	if out.Environment == "" {
		out.Environment = envOr("RESTLYTICS_ENV", defaultEnv)
	}
	if out.SampleRate == 0 {
		out.SampleRate = envFloat("RESTLYTICS_SAMPLE_RATE", defaultSampleRate)
	}
	if out.Transport == "" {
		out.Transport = envOr("RESTLYTICS_TRANSPORT", defaultTransport)
	}
	if out.TimeoutMs == 0 {
		out.TimeoutMs = envInt("RESTLYTICS_TIMEOUT_MS", defaultTimeoutMs)
	}
	if !out.CaptureSQL {
		out.CaptureSQL = envBool("RESTLYTICS_CAPTURE_SQL", false)
	}
	if out.MaxSpans == 0 {
		out.MaxSpans = envInt("RESTLYTICS_MAX_SPANS", defaultMaxSpans)
	}
	if out.InstrumentDB == nil {
		out.InstrumentDB = boolPtr(envBool("RESTLYTICS_INSTRUMENT_DB", true))
	}
	if out.InstrumentHTTP == nil {
		out.InstrumentHTTP = boolPtr(envBool("RESTLYTICS_INSTRUMENT_HTTP", true))
	}
	if out.InstrumentCache == nil {
		out.InstrumentCache = boolPtr(envBool("RESTLYTICS_INSTRUMENT_CACHE", true))
	}
	if out.IgnorePaths == nil {
		out.IgnorePaths = defaultIgnorePaths()
	}
	if out.RedactionQueryKeys == nil {
		out.RedactionQueryKeys = defaultRedactionKeys()
	}

	return out
}

// Enabled reports whether the SDK should actually emit traces.
func (c Config) Enabled() bool {
	return c.Key != ""
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
			return n
		}
	}
	return fallback
}

func envFloat(key string, fallback float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
			return f
		}
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	switch v {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
}

func boolPtr(b bool) *bool { return &b }
