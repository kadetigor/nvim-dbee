package builders

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/kndndrj/nvim-dbee/dbee/core"
)

// querier is the common query interface of *sql.DB and *sql.Conn.
type querier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// default sql client used by other specific implementations
type Client struct {
	db             *sql.DB
	typeProcessors map[string]func(any) any

	sessionMu     sync.Mutex
	sessionConn   *sql.Conn
	sessionResult *ResultStream
}

func NewClient(db *sql.DB, opts ...ClientOption) *Client {
	config := clientConfig{
		typeProcessors: make(map[string]func(any) any),
	}
	for _, opt := range opts {
		opt(&config)
	}

	return &Client{
		db:             db,
		typeProcessors: config.typeProcessors,
	}
}

func (c *Client) Close() {
	c.EndSession()
	c.db.Close()
}

// Swap swaps current database connection for another one
// and closes the old one.
func (c *Client) Swap(db *sql.DB) {
	c.EndSession()
	c.db.Close()
	c.db = db
}

// StartSession pins a dedicated connection so that consecutive queries share
// a single database session. This makes interactive transactions
// (BEGIN in one call, COMMIT in a later one) possible - the pool would
// otherwise hand each call a different connection.
// No-op if a session is already active.
func (c *Client) StartSession(ctx context.Context) error {
	c.sessionMu.Lock()
	defer c.sessionMu.Unlock()

	if c.sessionConn != nil {
		return nil
	}

	conn, err := c.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("c.db.Conn: %w", err)
	}
	c.sessionConn = conn

	return nil
}

// EndSession releases the pinned connection back to the pool.
// No-op if no session is active.
func (c *Client) EndSession() {
	c.sessionMu.Lock()
	defer c.sessionMu.Unlock()

	if c.sessionResult != nil {
		c.sessionResult.Close()
		c.sessionResult = nil
	}
	if c.sessionConn != nil {
		_ = c.sessionConn.Close()
		c.sessionConn = nil
	}
}

// InSession reports whether a pinned session is active.
func (c *Client) InSession() bool {
	c.sessionMu.Lock()
	defer c.sessionMu.Unlock()
	return c.sessionConn != nil
}

// acquireQuerier returns the pinned session connection if one is active,
// otherwise the pool. A connection supports only one open result set, so the
// previous session result (if any) is drained first.
func (c *Client) acquireQuerier() querier {
	c.sessionMu.Lock()
	defer c.sessionMu.Unlock()

	if c.sessionConn == nil {
		return c.db
	}

	if c.sessionResult != nil {
		c.sessionResult.Close()
		c.sessionResult = nil
	}

	return c.sessionConn
}

// trackSessionResult remembers the result stream produced on the session
// connection, so the next session query can drain it before executing.
func (c *Client) trackSessionResult(rs *ResultStream) {
	c.sessionMu.Lock()
	defer c.sessionMu.Unlock()

	if c.sessionConn != nil {
		c.sessionResult = rs
	}
}

// ColumnsFromQuery executes a given query on a new connection and
// converts the results to columns. A query should return a result that is
// at least 2 columns wide and have the following structure:
//
//	1st elem: name - string
//	2nd elem: type - string
//
// Query is sprintf-ed with args, so ColumnsFromQuery("select a from %s", "table_name") works.
func (c *Client) ColumnsFromQuery(query string, args ...any) ([]*core.Column, error) {
	result, err := c.Query(context.Background(), fmt.Sprintf(query, args...))
	if err != nil {
		return nil, err
	}

	return ColumnsFromResultStream(result)
}

// Exec executes a query and returns a stream with single row (number of affected results).
func (c *Client) Exec(ctx context.Context, query string) (*ResultStream, error) {
	res, err := c.acquireQuerier().ExecContext(ctx, query)
	if err != nil {
		return nil, err
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}

	rows := NewResultStreamBuilder().
		WithNextFunc(NextSingle(affected)).
		WithHeader(core.Header{"Rows Affected"}).
		Build()

	return rows, nil
}

// Query executes a query on a connection and returns a result stream.
func (c *Client) Query(ctx context.Context, query string) (*ResultStream, error) {
	rows, err := c.acquireQuerier().QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}

	result, err := c.parseRows(rows)
	if err != nil {
		return nil, err
	}
	c.trackSessionResult(result)

	return result, nil
}

// QueryUntilNotEmpty executes given queries on a single connection and returns when one of them
// has a nonempty result.
// Useful for specifying "fallback" queries like "ROWCOUNT()" when there are no results in query.
func (c *Client) QueryUntilNotEmpty(ctx context.Context, queries ...string) (*ResultStream, error) {
	if len(queries) < 1 {
		return nil, errors.New("no queries provided")
	}

	// in a session, queries run on the pinned connection which must stay open;
	// otherwise grab a dedicated connection and release it when done
	var conn querier
	cleanup := func() {}
	if c.InSession() {
		conn = c.acquireQuerier()
	} else {
		dedicated, err := c.db.Conn(ctx)
		if err != nil {
			return nil, fmt.Errorf("c.db.Conn: %w", err)
		}
		conn = dedicated
		cleanup = func() { _ = dedicated.Close() }
	}

	for _, query := range queries {
		rows, err := conn.QueryContext(ctx, query)
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("conn.QueryContext: %w", err)
		}

		result, err := c.parseRows(rows)
		if err != nil {
			cleanup()
			return nil, err
		}

		// has result
		if len(result.Header()) > 0 {
			result.AddCallback(cleanup)
			c.trackSessionResult(result)
			return result, nil
		}

		result.Close()
	}

	cleanup()

	// return an empty result
	return NewResultStreamBuilder().
		WithNextFunc(NextNil()).
		WithHeader(core.Header{"No Results"}).
		Build(), nil
}

func (c *Client) getTypeProcessor(typ string) func(any) any {
	proc, ok := c.typeProcessors[strings.ToLower(typ)]
	if ok {
		return proc
	}

	return func(val any) any {
		valb, ok := val.([]byte)
		if ok {
			return string(valb)
		}
		return val
	}
}

// parseRows transforms sql rows to result stream.
func (c *Client) parseRows(rows *sql.Rows) (*ResultStream, error) {
	// create new rows
	header, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	hasNextFunc := func() bool {
		// TODO: do we even support multiple result sets?
		// if not next result, check for any new sets
		if !rows.Next() {
			if !rows.NextResultSet() {
				return false
			}
			return rows.Next()
		}
		return true
	}

	nextFunc := func() (core.Row, error) {
		dbCols, err := rows.ColumnTypes()
		if err != nil {
			return nil, err
		}

		columns := make([]any, len(dbCols))
		columnPointers := make([]any, len(dbCols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}

		if err := rows.Scan(columnPointers...); err != nil {
			return nil, err
		}

		row := make(core.Row, len(dbCols))
		for i := range dbCols {
			val := *columnPointers[i].(*any)

			proc := c.getTypeProcessor(dbCols[i].DatabaseTypeName())

			row[i] = proc(val)
		}

		return row, nil
	}

	result := NewResultStreamBuilder().
		WithNextFunc(nextFunc, hasNextFunc).
		WithHeader(header).
		WithCloseFunc(func() {
			_ = rows.Close()
		}).
		Build()

	return result, nil
}
