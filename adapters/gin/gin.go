// Package restlyticsgin provides a Gin middleware for the restlytics Go SDK.
//
// It opens the root SERVER span at request start and flushes the OTLP batch
// after the handler chain returns. The route template comes from c.FullPath()
// (e.g. "/users/:id") — the low-cardinality template the contract requires,
// never the raw URL.
//
// This subpackage imports github.com/gin-gonic/gin and is NOT expected to build
// offline; the core restlytics package has no third-party dependencies.
package restlyticsgin

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/restlytics/restlytics-go"
)

// Middleware returns a gin.HandlerFunc that traces each request.
func Middleware(rl *restlytics.Restlytics) gin.HandlerFunc {
	tracer := rl.Tracer()

	return func(c *gin.Context) {
		if !rl.Enabled() || !rl.ShouldTrace(c.Request.URL.Path) {
			c.Next()
			return
		}

		traceparent := c.GetHeader("traceparent")
		name := c.Request.Method + " " + c.Request.URL.Path
		ctx := tracer.Start(c.Request.Context(), name, traceparent)
		c.Request = c.Request.WithContext(ctx)

		defer func() {
			finish(rl, c)
		}()

		c.Next()
	}
}

func finish(rl *restlytics.Restlytics, c *gin.Context) {
	root := restlytics.RootSpan(c.Request.Context())
	if root == nil {
		return
	}

	// c.FullPath() is the matched route TEMPLATE ("/users/:id"); fall back to the
	// path for unmatched routes (404).
	route := c.FullPath()
	if route == "" {
		route = c.Request.URL.Path
	}
	method := c.Request.Method
	status := c.Writer.Status()

	root.SetName(method + " " + route)
	root.SetString(restlytics.AttrHTTPRequestMethod, method)
	root.SetString(restlytics.AttrHTTPRoute, route)
	root.SetInt(restlytics.AttrHTTPResponseStatusCode, int64(status))

	switch {
	case status >= http.StatusInternalServerError:
		if root.StatusCode() != restlytics.StatusError {
			root.SetStatus(restlytics.StatusError, "HTTP "+strconv.Itoa(status))
		}
	case len(c.Errors) > 0:
		root.SetStatus(restlytics.StatusError, c.Errors.String())
	case root.StatusCode() == restlytics.StatusUnset:
		root.SetStatus(restlytics.StatusOK, "")
	}

	rl.Tracer().Finish(c.Request.Context())
}
