// Package restlyticsfiber provides a Fiber middleware for the restlytics Go SDK.
//
// Fiber is built on fasthttp (not net/http), so the per-request restlytics
// context is carried via Fiber's UserContext. The route template comes from
// c.Route().Path (e.g. "/users/:id").
//
// This subpackage imports github.com/gofiber/fiber/v2 and is NOT expected to
// build offline; the core restlytics package has no third-party dependencies.
package restlyticsfiber

import (
	"net/http"
	"strconv"

	"github.com/gofiber/fiber/v2"
	"github.com/restlytics/restlytics-go"
)

// Middleware returns a fiber.Handler that traces each request.
func Middleware(rl *restlytics.Restlytics) fiber.Handler {
	tracer := rl.Tracer()

	return func(c *fiber.Ctx) error {
		path := c.Path()
		if !rl.Enabled() || !rl.ShouldTrace(path) {
			return c.Next()
		}

		method := c.Method()
		traceparent := c.Get("traceparent")
		name := method + " " + path

		// Carry the restlytics state on the user context so DB instrumentation
		// can find it; fasthttp has no net/http request context.
		ctx := tracer.Start(c.UserContext(), name, traceparent)
		c.SetUserContext(ctx)

		err := c.Next()

		finish(rl, c)
		return err
	}
}

func finish(rl *restlytics.Restlytics, c *fiber.Ctx) {
	root := restlytics.RootSpan(c.UserContext())
	if root == nil {
		return
	}

	route := c.Path()
	if r := c.Route(); r != nil && r.Path != "" {
		route = r.Path // registered TEMPLATE ("/users/:id")
	}
	method := c.Method()
	status := c.Response().StatusCode()

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

	rl.Tracer().Finish(c.UserContext())
}
