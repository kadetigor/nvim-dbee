package adapters

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/gob"
	"encoding/json"
	"fmt"
	nurl "net/url"
	"strings"

	"github.com/kndndrj/nvim-dbee/dbee/core"
	"github.com/kndndrj/nvim-dbee/dbee/core/builders"
)

var (
	_ core.Driver           = (*postgresDriver)(nil)
	_ core.DatabaseSwitcher = (*postgresDriver)(nil)
)

type postgresDriver struct {
	c   *builders.Client
	url *nurl.URL
}

func (c *postgresDriver) Query(ctx context.Context, query string) (core.ResultStream, error) {
	lower := strings.ToLower(query)
	fields := strings.Fields(lower)
	action := ""
	if len(fields) > 0 {
		action = fields[0]
	}

	// Interactive transactions: BEGIN pins a dedicated session so that
	// consecutive calls run on the same connection, COMMIT/ROLLBACK release it.
	// Without this each call may get a different pooled connection and an open
	// transaction silently doesn't apply to subsequent queries.
	if action == "begin" || action == "start" {
		if err := c.c.StartSession(ctx); err != nil {
			return nil, err
		}
	}
	if lastStatementEndsTransaction(lower) {
		defer c.c.EndSession()
	}

	hasReturnValues := strings.Contains(lower, " returning ")

	if (action == "update" || action == "delete" || action == "insert") && !hasReturnValues {
		return c.c.Exec(ctx, query)
	}

	return c.c.QueryUntilNotEmpty(ctx, query)
}

// lastStatementEndsTransaction reports whether the final statement of a
// (possibly multi-statement) query commits or rolls back the transaction.
// "ROLLBACK TO SAVEPOINT" stays inside the transaction and doesn't count.
func lastStatementEndsTransaction(lowerQuery string) bool {
	statements := strings.Split(lowerQuery, ";")
	for i := len(statements) - 1; i >= 0; i-- {
		fields := strings.Fields(statements[i])
		if len(fields) == 0 {
			continue
		}
		switch fields[0] {
		case "commit", "end", "abort":
			return true
		case "rollback":
			return len(fields) < 2 || fields[1] != "to"
		default:
			return false
		}
	}
	return false
}

func (c *postgresDriver) Columns(opts *core.TableOptions) ([]*core.Column, error) {
	return c.c.ColumnsFromQuery(`
		SELECT column_name, data_type
		FROM information_schema.columns
		WHERE
			table_schema='%s' AND
			table_name='%s'
		`, opts.Schema, opts.Table)
}

func (c *postgresDriver) Structure() ([]*core.Structure, error) {
	query := `
		SELECT table_schema, table_name, table_type FROM information_schema.tables UNION ALL
		SELECT schemaname, matviewname, 'VIEW' FROM pg_matviews;
	`

	rows, err := c.Query(context.TODO(), query)
	if err != nil {
		return nil, err
	}

	return core.GetGenericStructure(rows, getPGStructureType)
}

func (c *postgresDriver) Close() {
	c.c.Close()
}

func (c *postgresDriver) ListDatabases() (current string, available []string, err error) {
	query := `
		SELECT current_database(), datname FROM pg_database
		WHERE datistemplate = false
		AND datname != current_database();
	`

	rows, err := c.Query(context.TODO(), query)
	if err != nil {
		return "", nil, err
	}

	for rows.HasNext() {
		row, err := rows.Next()
		if err != nil {
			return "", nil, err
		}

		// We know for a fact there are 2 string fields (see query above)
		current = row[0].(string)
		available = append(available, row[1].(string))
	}

	return current, available, nil
}

func (c *postgresDriver) SelectDatabase(name string) error {
	c.url.Path = fmt.Sprintf("/%s", name)
	db, err := sql.Open("postgres", c.url.String())
	if err != nil {
		return fmt.Errorf("unable to switch databases: %w", err)
	}

	// sql.Open just validate its arguments
	// without creating a connection to the database
	// so we need to ping the database to check if it's valid
	if err = db.Ping(); err != nil {
		return fmt.Errorf("unable to connect to database: %q, err: %w", name, err)
	}

	c.c.Swap(db)

	return nil
}

// getPGStructureType returns the structure type based on the provided string.
func getPGStructureType(typ string) core.StructureType {
	switch typ {
	case "TABLE", "BASE TABLE", "FOREIGN", "FOREIGN TABLE", "SYSTEM TABLE":
		return core.StructureTypeTable
	case "VIEW", "SYSTEM VIEW":
		return core.StructureTypeView
	case "MATERIALIZED VIEW":
		return core.StructureTypeMaterializedView
	case "SINK":
		return core.StructureTypeSink
	case "SOURCE":
		return core.StructureTypeSource
	default:
		return core.StructureTypeNone
	}
}

// postgresJSONResponse serves as a wrapper around the json response
// to pretty-print the return values
type postgresJSONResponse struct {
	value []byte
}

func newPostgresJSONResponse(val []byte) *postgresJSONResponse {
	return &postgresJSONResponse{
		value: val,
	}
}

func (pj *postgresJSONResponse) String() string {
	var parsed bytes.Buffer
	err := json.Indent(&parsed, pj.value, "", "  ")
	if err != nil {
		return string(pj.value)
	}
	return parsed.String()
}

func (pj *postgresJSONResponse) MarshalJSON() ([]byte, error) {
	if json.Valid(pj.value) {
		return pj.value, nil
	}

	return json.Marshal(pj.value)
}

func (pj *postgresJSONResponse) GobEncode() ([]byte, error) {
	var err error
	w := new(bytes.Buffer)
	encoder := gob.NewEncoder(w)
	err = encoder.Encode(pj.value)
	if err != nil {
		return nil, err
	}
	return w.Bytes(), err
}

func (pj *postgresJSONResponse) GobDecode(buf []byte) error {
	var err error
	r := bytes.NewBuffer(buf)
	decoder := gob.NewDecoder(r)
	err = decoder.Decode(&pj.value)
	if err != nil {
		return err
	}
	return err
}
