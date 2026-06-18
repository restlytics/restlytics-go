// Package restlyticsgorm provides a GORM plugin for the restlytics Go SDK.
//
// It registers before/after callbacks on each GORM operation so every query
// emitted during a traced request becomes a CLIENT db span (kind=3,
// category="db"). Only the SQL text + binding COUNT are recorded; binding
// VALUES are never sent.
//
// This subpackage imports gorm.io/gorm and is NOT expected to build offline; the
// core restlytics package has no third-party dependencies.
//
// Usage:
//
//	db.Use(restlyticsgorm.New(rl, "postgresql"))
package restlyticsgorm

import (
	"time"

	"github.com/restlytics/restlytics-go"
	"gorm.io/gorm"
)

const startKey = "restlytics:start"

// Plugin implements gorm.Plugin.
type Plugin struct {
	rl     *restlytics.Restlytics
	system string
}

// New builds a GORM plugin. system is the db.system.name value
// (e.g. "postgresql", "mysql", "sqlite").
func New(rl *restlytics.Restlytics, system string) *Plugin {
	return &Plugin{rl: rl, system: system}
}

// Name implements gorm.Plugin.
func (p *Plugin) Name() string { return "restlytics" }

// Initialize registers the before/after callbacks on every operation.
func (p *Plugin) Initialize(db *gorm.DB) error {
	cb := db.Callback()

	ops := []struct {
		name   string
		before func(name string, fn func(*gorm.DB)) error
		after  func(name string, fn func(*gorm.DB)) error
	}{
		{"create", cb.Create().Before("gorm:create").Register, cb.Create().After("gorm:create").Register},
		{"query", cb.Query().Before("gorm:query").Register, cb.Query().After("gorm:query").Register},
		{"update", cb.Update().Before("gorm:update").Register, cb.Update().After("gorm:update").Register},
		{"delete", cb.Delete().Before("gorm:delete").Register, cb.Delete().After("gorm:delete").Register},
		{"row", cb.Row().Before("gorm:row").Register, cb.Row().After("gorm:row").Register},
		{"raw", cb.Raw().Before("gorm:raw").Register, cb.Raw().After("gorm:raw").Register},
	}

	for _, op := range ops {
		if err := op.before("restlytics:before_"+op.name, p.before); err != nil {
			return err
		}
		if err := op.after("restlytics:after_"+op.name, p.after); err != nil {
			return err
		}
	}
	return nil
}

// before stamps a monotonic start time onto the statement settings.
func (p *Plugin) before(db *gorm.DB) {
	db.Set(startKey, time.Now().UnixNano())
}

// after closes the span using the recorded start time and the rendered SQL.
func (p *Plugin) after(db *gorm.DB) {
	ctx := db.Statement.Context
	if !restlytics.IsSampled(ctx) {
		return
	}
	if p.rl.Config().InstrumentDB != nil && !*p.rl.Config().InstrumentDB {
		return
	}

	end := time.Now().UnixNano()
	start := end
	if v, ok := db.Get(startKey); ok {
		if n, ok := v.(int64); ok {
			start = n
		}
	}

	restlytics.IncrementDBQueryCount(ctx)
	sp := restlytics.AddChildSpan(ctx, "db.query", start, end)
	if sp == nil {
		return
	}

	// Render the SQL with placeholders; GORM exposes the explained statement.
	sql := db.Statement.SQL.String()
	summary := restlytics.Normalize(sql)

	sp.SetString(restlytics.AttrCategory, restlytics.CategoryDB)
	sp.SetString(restlytics.AttrDBSystem, p.system)
	sp.SetString(restlytics.AttrDBQuerySummary, summary)
	sp.SetName(op(summary))
	if name := op(summary); name != "" {
		sp.SetString(restlytics.AttrDBOperationName, name)
	}
	sp.SetInt(restlytics.AttrBindingsCount, int64(len(db.Statement.Vars)))
	if db.Statement.Table != "" {
		sp.SetString(restlytics.AttrDBNamespace, db.Statement.Table)
	}
	if p.rl.Config().CaptureSQL && sql != "" {
		sp.SetString(restlytics.AttrDBQueryText, capString(sql, 2048))
	}
	if db.Error != nil && db.Error != gorm.ErrRecordNotFound {
		sp.SetStatus(restlytics.StatusError, db.Error.Error())
	}
}

// op extracts the leading SQL verb from a normalized summary.
func op(summary string) string {
	i := 0
	for i < len(summary) && summary[i] == ' ' {
		i++
	}
	j := i
	for j < len(summary) && summary[j] != ' ' {
		j++
	}
	if j == i {
		return ""
	}
	return summary[i:j]
}

func capString(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

var _ gorm.Plugin = (*Plugin)(nil)
