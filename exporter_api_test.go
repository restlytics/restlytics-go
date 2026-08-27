package restlytics_test

import (
	"context"

	"github.com/restlytics/restlytics-go"
)

// publicExporter is a compile-time customer example. Keeping this in the
// external test package proves the contract needs no unexported SDK details.
type publicExporter struct{}

func (publicExporter) ExportTraces(context.Context, restlytics.ExportTraceServiceRequest) error {
	return nil
}

func (publicExporter) ExportLogs(context.Context, restlytics.ExportLogsServiceRequest) error {
	return nil
}

var _ restlytics.Exporter = publicExporter{}
var _ = restlytics.Config{CustomExporter: publicExporter{}}
