package adapters

// Edge-case suite for interactive transaction sessions.
// Requires a local postgres; skipped unless DBEE_LIVE_PG_URL is set.

import (
	"context"
	"os"
	"testing"

	"github.com/kndndrj/nvim-dbee/dbee/core"
)

// runErr executes a query and drains the result, returning the error instead
// of failing the test - for scenarios that expect failure.
func runErr(d core.Driver, ctx context.Context, query string) error {
	rs, err := d.Query(ctx, query)
	if err != nil {
		return err
	}
	for rs.HasNext() {
		if _, err := rs.Next(); err != nil {
			return err
		}
	}
	rs.Close()
	return nil
}

func connectPair(t *testing.T) (core.Driver, *postgresDriver, core.Driver) {
	t.Helper()
	url := os.Getenv("DBEE_LIVE_PG_URL")
	if url == "" {
		t.Skip("DBEE_LIVE_PG_URL not set")
	}
	adapter := &Postgres{}
	main, err := adapter.Connect(url)
	if err != nil {
		t.Fatalf("connect: %s", err)
	}
	observer, err := adapter.Connect(url)
	if err != nil {
		t.Fatalf("connect observer: %s", err)
	}
	t.Cleanup(func() { main.Close(); observer.Close() })
	return main, main.(*postgresDriver), observer
}

func TestPostgresSessionCommentPrefix(t *testing.T) {
	main, drv, _ := connectPair(t)
	ctx := context.Background()

	run(t, main, ctx, "-- prod fix for ticket X\nBEGIN;")
	if !drv.InSession() {
		t.Fatal("line-comment-prefixed BEGIN did not start a session")
	}
	run(t, main, ctx, "ROLLBACK;")
	if drv.InSession() {
		t.Fatal("session not released after ROLLBACK")
	}

	run(t, main, ctx, "/* block\ncomment */ BEGIN;")
	if !drv.InSession() {
		t.Fatal("block-comment-prefixed BEGIN did not start a session")
	}
	run(t, main, ctx, "-- done\nROLLBACK;")
	if drv.InSession() {
		t.Fatal("comment-prefixed ROLLBACK did not release session")
	}
}

func TestPostgresSessionKeywordVariants(t *testing.T) {
	main, drv, _ := connectPair(t)
	ctx := context.Background()

	for _, begin := range []string{"BEGIN;", "begin;", "BeGiN;", "BEGIN TRANSACTION;", "START TRANSACTION;", "BEGIN"} {
		run(t, main, ctx, begin)
		if !drv.InSession() {
			t.Fatalf("%q did not start a session", begin)
		}
		run(t, main, ctx, "ROLLBACK;")
		if drv.InSession() {
			t.Fatalf("session not released after ROLLBACK (begin variant %q)", begin)
		}
	}

	// END; commits like COMMIT
	run(t, main, ctx, "BEGIN;")
	run(t, main, ctx, "END;")
	if drv.InSession() {
		t.Fatal("session not released after END")
	}
}

func TestPostgresSessionErrorInsideTransaction(t *testing.T) {
	main, drv, _ := connectPair(t)
	ctx := context.Background()

	run(t, main, ctx, "BEGIN;")
	if err := runErr(main, ctx, "SELECT * FROM table_that_does_not_exist_xyz;"); err == nil {
		t.Fatal("expected error from bad query")
	}
	// session must survive the error, so the user can still roll back
	if !drv.InSession() {
		t.Fatal("session dropped after failed query - user cannot ROLLBACK")
	}
	// postgres is now in aborted-transaction state: normal queries fail...
	if err := runErr(main, ctx, "SELECT 1;"); err == nil {
		t.Fatal("expected aborted-transaction error")
	}
	// ...until rollback
	run(t, main, ctx, "ROLLBACK;")
	if drv.InSession() {
		t.Fatal("session not released after ROLLBACK from aborted state")
	}
	run(t, main, ctx, "SELECT 1;") // connection healthy again
}

func TestPostgresSessionNoopEnds(t *testing.T) {
	main, drv, _ := connectPair(t)
	ctx := context.Background()

	// COMMIT with no open transaction: postgres warns, must not crash or pin
	run(t, main, ctx, "COMMIT;")
	if drv.InSession() {
		t.Fatal("bare COMMIT pinned a session")
	}

	// double BEGIN: postgres warns, session stays
	run(t, main, ctx, "BEGIN;")
	run(t, main, ctx, "BEGIN;")
	if !drv.InSession() {
		t.Fatal("session lost after double BEGIN")
	}
	run(t, main, ctx, "ROLLBACK;")
}

func TestPostgresSessionSingleCallBlock(t *testing.T) {
	main, drv, observer := connectPair(t)
	ctx := context.Background()

	run(t, main, ctx, "DROP TABLE IF EXISTS dbee_block_test; CREATE TABLE dbee_block_test (id int);")
	defer run(t, main, ctx, "DROP TABLE IF EXISTS dbee_block_test;")

	// whole transaction in one call: must commit and NOT leave a pinned session
	run(t, main, ctx, "BEGIN;\nINSERT INTO dbee_block_test VALUES (1);\nCOMMIT;")
	if drv.InSession() {
		t.Fatal("single-call BEGIN..COMMIT left session pinned")
	}
	if rows := run(t, observer, ctx, "SELECT * FROM dbee_block_test;"); len(rows) != 1 {
		t.Fatalf("single-call block not committed: observer sees %d rows", len(rows))
	}

	// block that opens but doesn't close: session stays for follow-up calls
	run(t, main, ctx, "BEGIN;\nINSERT INTO dbee_block_test VALUES (2);")
	if !drv.InSession() {
		t.Fatal("BEGIN-starting block did not pin session")
	}
	run(t, main, ctx, "ROLLBACK;")
	if rows := run(t, main, ctx, "SELECT * FROM dbee_block_test;"); len(rows) != 1 {
		t.Fatalf("expected rollback of second row, got %d rows", len(rows))
	}
}

func TestPostgresSessionUndrainedResult(t *testing.T) {
	main, drv, _ := connectPair(t)
	ctx := context.Background()

	run(t, main, ctx, "BEGIN;")
	// leave a large result stream completely undrained...
	if _, err := main.Query(ctx, "SELECT generate_series(1, 50000);"); err != nil {
		t.Fatalf("large query failed: %s", err)
	}
	// ...next query on the pinned connection must still work (auto-drain)
	run(t, main, ctx, "SELECT 1;")
	run(t, main, ctx, "ROLLBACK;")
	if drv.InSession() {
		t.Fatal("session not released")
	}
}

func TestPostgresAutocommitUnaffected(t *testing.T) {
	main, drv, observer := connectPair(t)
	ctx := context.Background()

	run(t, main, ctx, "DROP TABLE IF EXISTS dbee_ac_test; CREATE TABLE dbee_ac_test (id int);")
	defer run(t, main, ctx, "DROP TABLE IF EXISTS dbee_ac_test;")

	// no transaction: plain statement autocommits, no session appears
	run(t, main, ctx, "INSERT INTO dbee_ac_test VALUES (1);")
	if drv.InSession() {
		t.Fatal("plain INSERT pinned a session")
	}
	if rows := run(t, observer, ctx, "SELECT * FROM dbee_ac_test;"); len(rows) != 1 {
		t.Fatalf("autocommit broken: observer sees %d rows", len(rows))
	}
}
