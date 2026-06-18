package restlytics

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
)

// database/sql instrumentation.
//
// WrapConnector wraps a driver.Connector so every Query/Exec that carries a
// restlytics request context records a CLIENT db span (kind=3, category="db").
// We only ever see the query text + argument COUNT — binding VALUES are never
// available here, which is exactly the redaction posture we want.
//
// Usage:
//
//	connector, _ := pq.NewConnector(dsn)
//	db := sql.OpenDB(restlytics.WrapConnector(connector, "postgresql", rl))
//
// or for drivers without a Connector, register a wrapped driver:
//
//	sql.Register("postgres-rl", restlytics.WrapDriver(&pq.Driver{}, "postgresql", rl))
//	db, _ := sql.Open("postgres-rl", dsn)

// WrapConnector wraps an existing driver.Connector with restlytics tracing.
func WrapConnector(c driver.Connector, system string, rl *Restlytics) driver.Connector {
	return &tracedConnector{base: c, system: system, rl: rl}
}

// WrapDriver wraps a driver.Driver. The returned driver records query spans for
// connections opened through it. Note: spans are only recorded for calls that
// pass a restlytics request context (the context-aware driver interfaces).
func WrapDriver(d driver.Driver, system string, rl *Restlytics) driver.Driver {
	return &tracedDriver{base: d, system: system, rl: rl}
}

type tracedConnector struct {
	base   driver.Connector
	system string
	rl     *Restlytics
}

func (c *tracedConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.base.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &tracedConn{base: conn, system: c.system, rl: c.rl}, nil
}

func (c *tracedConnector) Driver() driver.Driver {
	return &tracedDriver{base: c.base.Driver(), system: c.system, rl: c.rl}
}

type tracedDriver struct {
	base   driver.Driver
	system string
	rl     *Restlytics
}

func (d *tracedDriver) Open(name string) (driver.Conn, error) {
	conn, err := d.base.Open(name)
	if err != nil {
		return nil, err
	}
	return &tracedConn{base: conn, system: d.system, rl: d.rl}, nil
}

// OpenConnector implements driver.DriverContext when the base driver does.
func (d *tracedDriver) OpenConnector(name string) (driver.Connector, error) {
	if dc, ok := d.base.(driver.DriverContext); ok {
		c, err := dc.OpenConnector(name)
		if err != nil {
			return nil, err
		}
		return &tracedConnector{base: c, system: d.system, rl: d.rl}, nil
	}
	// Fall back to a dsn-capturing connector around Open.
	return &dsnConnector{dsn: name, driver: d}, nil
}

type dsnConnector struct {
	dsn    string
	driver *tracedDriver
}

func (c *dsnConnector) Connect(_ context.Context) (driver.Conn, error) {
	return c.driver.Open(c.dsn)
}
func (c *dsnConnector) Driver() driver.Driver { return c.driver }

type tracedConn struct {
	base   driver.Conn
	system string
	rl     *Restlytics
}

// record opens, times, and closes a db child span if the context is sampled and
// DB instrumentation is enabled. fn does the real work.
func (c *tracedConn) record(ctx context.Context, query string, nargs int, fn func() error) error {
	if !c.dbEnabled() || !IsSampled(ctx) {
		return fn()
	}

	start := nowNs()
	err := fn()
	end := nowNs()

	IncrementDBQueryCount(ctx)
	if sp := AddChildSpan(ctx, "db.query", start, end); sp != nil {
		c.decorate(sp, query, nargs, err)
	}
	return err
}

func (c *tracedConn) decorate(sp *Span, query string, nargs int, err error) {
	sp.SetString(AttrCategory, CategoryDB)
	sp.SetString(AttrDBSystem, c.system)
	summary := Normalize(query)
	sp.SetString(AttrDBQuerySummary, summary)
	sp.SetName(opName(summary))
	if op := opName(summary); op != "" {
		sp.SetString(AttrDBOperationName, op)
	}
	sp.SetInt(AttrBindingsCount, int64(nargs))
	if c.rl.cfg.CaptureSQL {
		sp.SetString(AttrDBQueryText, capString(query, 2048))
	}
	if err != nil && !errors.Is(err, io.EOF) {
		sp.SetStatus(StatusError, err.Error())
	}
}

func (c *tracedConn) dbEnabled() bool {
	return c.rl != nil && c.rl.cfg.Enabled() &&
		(c.rl.cfg.InstrumentDB == nil || *c.rl.cfg.InstrumentDB)
}

// --- driver.Conn ---

func (c *tracedConn) Prepare(query string) (driver.Stmt, error) {
	st, err := c.base.Prepare(query)
	if err != nil {
		return nil, err
	}
	return &tracedStmt{base: st, conn: c, query: query}, nil
}

func (c *tracedConn) Close() error { return c.base.Close() }

func (c *tracedConn) Begin() (driver.Tx, error) { return c.base.Begin() } //nolint:staticcheck

// --- context-aware optional interfaces ---

func (c *tracedConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if cp, ok := c.base.(driver.ConnPrepareContext); ok {
		st, err := cp.PrepareContext(ctx, query)
		if err != nil {
			return nil, err
		}
		return &tracedStmt{base: st, conn: c, query: query}, nil
	}
	return c.Prepare(query)
}

func (c *tracedConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if cb, ok := c.base.(driver.ConnBeginTx); ok {
		return cb.BeginTx(ctx, opts)
	}
	return c.base.Begin() //nolint:staticcheck
}

func (c *tracedConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	q, ok := c.base.(driver.QueryerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	var rows driver.Rows
	err := c.record(ctx, query, len(args), func() error {
		var e error
		rows, e = q.QueryContext(ctx, query, args)
		return e
	})
	return rows, err
}

func (c *tracedConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	e, ok := c.base.(driver.ExecerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	var res driver.Result
	err := c.record(ctx, query, len(args), func() error {
		var ee error
		res, ee = e.ExecContext(ctx, query, args)
		return ee
	})
	return res, err
}

func (c *tracedConn) Ping(ctx context.Context) error {
	if p, ok := c.base.(driver.Pinger); ok {
		return p.Ping(ctx)
	}
	return nil
}

type tracedStmt struct {
	base  driver.Stmt
	conn  *tracedConn
	query string
}

func (s *tracedStmt) Close() error  { return s.base.Close() }
func (s *tracedStmt) NumInput() int { return s.base.NumInput() }

func (s *tracedStmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.base.Exec(args) //nolint:staticcheck
}

func (s *tracedStmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.base.Query(args) //nolint:staticcheck
}

func (s *tracedStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	ec, ok := s.base.(driver.StmtExecContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	var res driver.Result
	err := s.conn.record(ctx, s.query, len(args), func() error {
		var e error
		res, e = ec.ExecContext(ctx, args)
		return e
	})
	return res, err
}

func (s *tracedStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	qc, ok := s.base.(driver.StmtQueryContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	var rows driver.Rows
	err := s.conn.record(ctx, s.query, len(args), func() error {
		var e error
		rows, e = qc.QueryContext(ctx, args)
		return e
	})
	return rows, err
}

// Compile-time interface assertions for the optional context-aware interfaces.
var (
	_ driver.Connector          = (*tracedConnector)(nil)
	_ driver.Driver             = (*tracedDriver)(nil)
	_ driver.DriverContext      = (*tracedDriver)(nil)
	_ driver.Conn               = (*tracedConn)(nil)
	_ driver.ConnPrepareContext = (*tracedConn)(nil)
	_ driver.ConnBeginTx        = (*tracedConn)(nil)
	_ driver.QueryerContext     = (*tracedConn)(nil)
	_ driver.ExecerContext      = (*tracedConn)(nil)
	_ driver.Pinger             = (*tracedConn)(nil)
	_ driver.Stmt               = (*tracedStmt)(nil)
	_ driver.StmtExecContext    = (*tracedStmt)(nil)
	_ driver.StmtQueryContext   = (*tracedStmt)(nil)
)

// capString caps s to at most n bytes (UTF-8 safe-ish; we keep it simple).
func capString(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// opName extracts the leading SQL verb (select/insert/update/delete/...) from a
// normalized summary. Returns "" if none.
func opName(summary string) string {
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

// OpenDB is a convenience that wraps a driver.Connector with restlytics tracing
// and returns a ready *sql.DB.
func OpenDB(c driver.Connector, system string, rl *Restlytics) *sql.DB {
	return sql.OpenDB(WrapConnector(c, system, rl))
}
