// Package restlyticsecho provides an Echo middleware for the restlytics Go SDK.
//
// The route template comes from c.Path() (e.g. "/users/:id") — the registered
// route pattern, never the raw URL.
//
// This subpackage imports github.com/labstack/echo/v4 and is NOT expected to
// build offline; the core restlytics package has no third-party dependencies.
package restlyticsecho

import (
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"
	"github.com/restlytics/restlytics-go"
)

// Middleware returns an echo.MiddlewareFunc that traces each request.
func Middleware(rl *restlytics.Restlytics) echo.MiddlewareFunc {
	tracer := rl.Tracer()

	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			req := c.Request()
			if !rl.Enabled() || !rl.ShouldTrace(req.URL.Path) {
				return next(c)
			}

			traceparent := req.Header.Get("traceparent")
			name := req.Method + " " + req.URL.Path
			ctx := tracer.Start(req.Context(), name, traceparent)
			c.SetRequest(req.WithContext(ctx))

			err := next(c)

			finish(rl, c, err)
			return err
		}
	}
}

func finish(rl *restlytics.Restlytics, c echo.Context, handlerErr error) {
	req := c.Request()
	root := restlytics.RootSpan(req.Context())
	if root == nil {
		return
	}

	// c.Path() is the registered route TEMPLATE ("/users/:id").
	route := c.Path()
	if route == "" {
		route = req.URL.Path
	}
	method := req.Method
	status := c.Response().Status

	// Echo may not have written the status yet when an error is returned; reflect
	// the error's HTTP code if present.
	if handlerErr != nil {
		if he, ok := handlerErr.(*echo.HTTPError); ok {
			status = he.Code
		} else if status < http.StatusBadRequest {
			status = http.StatusInternalServerError
		}
	}

	root.SetName(method + " " + route)
	root.SetString(restlytics.AttrHTTPRequestMethod, method)
	root.SetString(restlytics.AttrHTTPRoute, route)
	root.SetInt(restlytics.AttrHTTPResponseStatusCode, int64(status))

	switch {
	case status >= http.StatusInternalServerError:
		root.SetStatus(restlytics.StatusError, "HTTP "+strconv.Itoa(status))
	case root.StatusCode() == restlytics.StatusUnset:
		root.SetStatus(restlytics.StatusOK, "")
	}

	rl.Tracer().Finish(req.Context())
}
