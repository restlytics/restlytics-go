package restlytics

import (
	"context"
	"net/http"
	"strings"
)

// HTTPRoundTripper wraps a host transport with outbound CLIENT spans and W3C
// traceparent propagation. Use request contexts derived from the inbound
// request so the wrapper can find the active Restlytics trace.
func (r *Restlytics) HTTPRoundTripper(next http.RoundTripper) http.RoundTripper {
	if next == nil {
		next = http.DefaultTransport
	}
	return &restlyticsRoundTripper{next: next, cfg: r.cfg}
}

type restlyticsRoundTripper struct {
	next http.RoundTripper
	cfg  Config
}

func (r *restlyticsRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	if request == nil || r.cfg.InstrumentHTTP == nil || !*r.cfg.InstrumentHTTP {
		return r.next.RoundTrip(request)
	}

	traceparent, spanID, ok := outboundContext(request.Context())
	if !ok {
		return r.next.RoundTrip(request)
	}

	outgoing := request.Clone(request.Context())
	outgoing.Header.Set("traceparent", traceparent)
	startNs := nowNs()
	response, err := r.next.RoundTrip(outgoing)
	recordOutboundHTTP(request.Context(), outgoing, response, err, spanID, startNs)
	return response, err
}

func recordOutboundHTTP(
	ctx context.Context,
	request *http.Request,
	response *http.Response,
	err error,
	spanID string,
	startNs int64,
) {
	defer func() { _ = recover() }()

	method := strings.ToUpper(request.Method)
	if method == "" {
		method = http.MethodGet
	}
	host := ""
	urlFull := ""
	if request.URL != nil {
		host = request.URL.Host
		urlFull = request.URL.String()
	}
	nameHost := host
	if nameHost == "" {
		nameHost = "http"
	}
	addChildSpanWithID(ctx, method+" "+nameHost, startNs, nowNs(), spanID, func(span *Span) {
		span.SetString(AttrCategory, CategoryHTTP)
		span.SetString(AttrHTTPRequestMethod, method)
		if urlFull != "" {
			span.SetString(AttrURLFull, redactURL(urlFull, nil))
		}
		if host != "" {
			span.SetString("server.address", host)
		}
		if response != nil {
			span.SetInt(AttrHTTPResponseStatusCode, int64(response.StatusCode))
		}
		if err != nil || (response != nil && response.StatusCode >= http.StatusInternalServerError) {
			span.SetStatus(StatusError, "")
		}
	})
}
