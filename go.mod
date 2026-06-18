module github.com/restlytics/restlytics-go

go 1.21

// The core package (root) uses the standard library only and builds offline.
//
// The following requirements are only needed by the framework adapter
// subpackages under ./adapters/*. They are declared here so `go build ./...`
// resolves them once modules are available; they are NOT fetched or used by the
// core. Versions are pinned but intentionally not vendored.
require (
	github.com/gin-gonic/gin v1.9.1
	github.com/labstack/echo/v4 v4.11.4
	github.com/gofiber/fiber/v2 v2.52.0
	gorm.io/gorm v1.25.7
)
