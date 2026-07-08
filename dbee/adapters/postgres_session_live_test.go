package adapters

// Live integration test for interactive transaction sessions.
// Requires a local postgres; skipped unless DBEE_LIVE_PG_URL is set.
// Run with:
//   DBEE_LIVE_PG_URL='postgres://user:pass@localhost:5432/db?sslmode=disable' go test ./adapters/ -run TestPostgresInteractiveTransaction -v

import (
	"context"
	"os"
	"testing"

	"github.com/kndndrj/nvim-dbee/dbee/core"
)

func run(t *testing.T, d core.Driver, ctx context.Context, query string) []core.Row {
	t.Helper()
	rs, err := d.Query(ctx, query)
	if err != nil {
		t.Fatalf("query %q failed: %s", query, err)
	}
	var rows []core.Row
	for rs.HasNext() {
		row, err := rs.Next()
		if err != nil {
			t.Fatalf("next failed: %s", err)
		}
		if row != nil {
			rows = append(rows, row)
		}
	}
	rs.Close()
	return rows
}

func TestPostgresInteractiveTransaction(t *testing.T) {
	url := os.Getenv("DBEE_LIVE_PG_URL")
	if url == "" {
		t.Skip("DBEE_LIVE_PG_URL not set")
	}
	ctx := context.Background()

	adapter := &Postgres{}

	// two independent drivers = two pools, like two dbee connections
	main, err := adapter.Connect(url)
	if err != nil {
		t.Fatalf("connect: %s", err)
	}
	defer main.Close()
	observer, err := adapter.Connect(url)
	if err != nil {
		t.Fatalf("connect observer: %s", err)
	}
	defer observer.Close()

	run(t, main, ctx, "DROP TABLE IF EXISTS dbee_session_test")
	run(t, main, ctx, "CREATE TABLE dbee_session_test (id int)")
	defer run(t, main, ctx, "DROP TABLE IF EXISTS dbee_session_test")

	// --- interactive transaction: separate calls, must share one session ---
	run(t, main, ctx, "BEGIN;")
	run(t, main, ctx, "INSERT INTO dbee_session_test VALUES (1);")

	// uncommitted row: invisible to other connections
	rows := run(t, observer, ctx, "SELECT * FROM dbee_session_test")
	if len(rows) != 0 {
		t.Fatalf("uncommitted row visible to other connection: autocommit leak, session pinning broken")
	}

	// but visible inside the session
	rows = run(t, main, ctx, "SELECT * FROM dbee_session_test")
	if len(rows) != 1 {
		t.Fatalf("row not visible inside own transaction: queries not on pinned connection (got %d rows)", len(rows))
	}

	run(t, main, ctx, "COMMIT;")

	rows = run(t, observer, ctx, "SELECT * FROM dbee_session_test")
	if len(rows) != 1 {
		t.Fatalf("committed row not visible after COMMIT (got %d rows)", len(rows))
	}

	// --- rollback path ---
	run(t, main, ctx, "BEGIN;")
	run(t, main, ctx, "INSERT INTO dbee_session_test VALUES (2);")
	run(t, main, ctx, "ROLLBACK;")

	rows = run(t, main, ctx, "SELECT * FROM dbee_session_test")
	if len(rows) != 1 {
		t.Fatalf("rollback failed: expected 1 row, got %d", len(rows))
	}

	// --- savepoint: ROLLBACK TO must keep the session open ---
	run(t, main, ctx, "BEGIN;")
	run(t, main, ctx, "SAVEPOINT sp1;")
	run(t, main, ctx, "INSERT INTO dbee_session_test VALUES (3);")
	run(t, main, ctx, "ROLLBACK TO SAVEPOINT sp1;")
	run(t, main, ctx, "INSERT INTO dbee_session_test VALUES (4);")
	run(t, main, ctx, "COMMIT;")

	rows = run(t, main, ctx, "SELECT id FROM dbee_session_test ORDER BY id")
	if len(rows) != 2 {
		t.Fatalf("savepoint flow: expected rows [1 4], got %d rows: %v", len(rows), rows)
	}
}
