//go:build integration

package pgledger_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/urmzd/anchor/adapters/postgres/pgledger"
	"github.com/urmzd/anchor/pkg/ledger"
	"github.com/urmzd/anchor/pkg/ledger/ledgertest"
	"github.com/urmzd/anchor/pkg/run"
)

// The integration harness owns all DDL: it applies pgledger.RecommendedDDL
// (or deliberately user-shaped schemas) through its own connection, because
// the adapter itself never creates or migrates anything.

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("ANCHOR_POSTGRES_URL")
	if url == "" {
		url = "postgres://anchor:anchor@localhost:5432/anchor"
	}
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatalf("pgxpool.New(%q): %v", url, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping %q: %v (is docker compose up?)", url, err)
	}
	t.Cleanup(pool.Close)
	return pool
}

var tableSeq atomic.Int64

// uniqueName returns a table name unique to this test-binary run, so
// concurrent and repeated runs never collide.
func uniqueName(prefix string) string {
	return fmt.Sprintf("%s_%d_%d", prefix, time.Now().UnixMilli(), tableSeq.Add(1))
}

// applyDDL executes semicolon-separated statements one at a time through the
// test's own connection.
func applyDDL(t *testing.T, pool *pgxpool.Pool, ddl string) {
	t.Helper()
	for _, stmt := range strings.Split(ddl, ";") {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if _, err := pool.Exec(context.Background(), stmt); err != nil {
			t.Fatalf("apply DDL %q: %v", stmt, err)
		}
	}
}

func dropTables(t *testing.T, pool *pgxpool.Pool, tables ...string) {
	t.Cleanup(func() {
		for _, tab := range tables {
			if _, err := pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+tab); err != nil {
				t.Errorf("drop %s: %v", tab, err)
			}
		}
	})
}

// newDefaultLedger provisions fresh DefaultMapping-shaped tables and returns
// a validated Ledger over them.
func newDefaultLedger(t *testing.T, pool *pgxpool.Pool) ledger.Ledger {
	t.Helper()
	m := pgledger.DefaultMapping()
	m.Runs.Table = uniqueName("dt_runs")
	m.Records.Table = uniqueName("dt_records")

	applyDDL(t, pool, pgledger.RecommendedDDL(m))
	dropTables(t, pool, m.Runs.Table, m.Records.Table)

	l, err := pgledger.New(pool, m)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := l.Validate(context.Background()); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	return l
}

func TestLedgerConformanceDefaultMapping(t *testing.T) {
	pool := testPool(t)
	ledgertest.Run(t, func(t *testing.T) ledger.Ledger {
		return newDefaultLedger(t, pool)
	})
}

// TestLedgerConformanceExistingUserTables is the prime directive's proof:
// the full conformance suite runs against deliberately user-shaped tables
// (foreign column names, extra NOT NULL columns filled by Defaults, a scope
// discriminator, no dedicated created-at mapping) that also hold another
// application's rows the adapter must never see or touch.
func TestLedgerConformanceExistingUserTables(t *testing.T) {
	pool := testPool(t)
	ledgertest.Run(t, func(t *testing.T) ledger.Ledger {
		ctx := context.Background()
		runsTable := uniqueName("app_runs")
		eventsTable := uniqueName("app_events")

		applyDDL(t, pool, fmt.Sprintf(`
CREATE TABLE %[1]s (
    correlation_id text,
    state text,
    source text NOT NULL,
    tenant text NOT NULL,
    payload jsonb,
    started_at timestamptz DEFAULT now()
);
CREATE UNIQUE INDEX ON %[1]s (correlation_id);
CREATE TABLE %[2]s (
    correlation_id text,
    event_key text,
    source text NOT NULL,
    tenant text NOT NULL,
    payload jsonb,
    occurred_at timestamptz DEFAULT now()
);
CREATE UNIQUE INDEX ON %[2]s (correlation_id, event_key)`, runsTable, eventsTable))
		dropTables(t, pool, runsTable, eventsTable)

		// Another application's rows share the tables. The alien run is
		// pending and ancient, so any scope leak would surface in the
		// suite's ListPending expectations; the alien event would
		// surface in Load.
		if _, err := pool.Exec(ctx, fmt.Sprintf(
			`INSERT INTO %s (correlation_id, state, source, tenant, payload, started_at)
			 VALUES ('alien-run', 'pending', 'other-app', 'acme', '{"CreatedAt":"2000-01-01T00:00:00Z"}', '2000-01-01')`,
			runsTable)); err != nil {
			t.Fatalf("seed alien run: %v", err)
		}
		if _, err := pool.Exec(ctx, fmt.Sprintf(
			`INSERT INTO %s (correlation_id, event_key, source, tenant, payload)
			 VALUES ('alien-run', 'alien-key', 'other-app', 'acme', '{}')`,
			eventsTable)); err != nil {
			t.Fatalf("seed alien event: %v", err)
		}

		m := pgledger.Mapping{
			Runs: pgledger.RunsMap{
				Table:    runsTable,
				RunID:    "correlation_id",
				Status:   "state",
				Envelope: "payload",
				// CreatedAt deliberately unmapped: exercises the
				// envelope fallback for ListPending.
				Scope: pgledger.Scope{Column: "source", Value: "anchor"},
				Defaults: func(r run.Run) map[string]any {
					return map[string]any{"tenant": "acme"}
				},
			},
			Records: pgledger.RecordsMap{
				Table:    eventsTable,
				RunID:    "correlation_id",
				Key:      "event_key",
				Envelope: "payload",
				Scope:    pgledger.Scope{Column: "source", Value: "anchor"},
				Defaults: func(rec run.Record) map[string]any {
					return map[string]any{"tenant": "acme"}
				},
			},
		}
		l, err := pgledger.New(pool, m)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if err := l.Validate(ctx); err != nil {
			t.Fatalf("Validate: %v", err)
		}
		return l
	})
}

// TestExistingUserTablesUntouchedRows double-checks the prime directive from
// the other side: after driving the adapter, the alien rows are intact.
func TestExistingUserTablesUntouchedRows(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	runsTable := uniqueName("app_runs_iso")

	applyDDL(t, pool, fmt.Sprintf(`
CREATE TABLE %[1]s (
    correlation_id text,
    state text,
    source text NOT NULL,
    tenant text NOT NULL,
    payload jsonb,
    started_at timestamptz DEFAULT now()
);
CREATE UNIQUE INDEX ON %[1]s (correlation_id)`, runsTable))
	dropTables(t, pool, runsTable)

	if _, err := pool.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s (correlation_id, state, source, tenant, payload)
		 VALUES ('alien-run', 'pending', 'other-app', 'acme', '{"CreatedAt":"2000-01-01T00:00:00Z"}')`,
		runsTable)); err != nil {
		t.Fatalf("seed alien run: %v", err)
	}

	recordsTable := uniqueName("app_events_iso")
	applyDDL(t, pool, fmt.Sprintf(`
CREATE TABLE %[1]s (
    correlation_id text,
    event_key text,
    source text NOT NULL,
    tenant text NOT NULL,
    payload jsonb
);
CREATE UNIQUE INDEX ON %[1]s (correlation_id, event_key)`, recordsTable))
	dropTables(t, pool, recordsTable)

	m := pgledger.Mapping{
		Runs: pgledger.RunsMap{
			Table: runsTable, RunID: "correlation_id", Status: "state", Envelope: "payload",
			Scope:    pgledger.Scope{Column: "source", Value: "anchor"},
			Defaults: func(r run.Run) map[string]any { return map[string]any{"tenant": "acme"} },
		},
		Records: pgledger.RecordsMap{
			Table: recordsTable, RunID: "correlation_id", Key: "event_key", Envelope: "payload",
			Scope:    pgledger.Scope{Column: "source", Value: "anchor"},
			Defaults: func(rec run.Record) map[string]any { return map[string]any{"tenant": "acme"} },
		},
	}
	l, err := pgledger.New(pool, m)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// The alien run must be invisible: completing it reports not found.
	err = l.Complete(ctx, "alien-run", 1, run.Result{Status: run.StatusSucceeded})
	if !errors.Is(err, run.ErrNotFound) {
		t.Fatalf("Complete(alien-run) = %v, want run.ErrNotFound", err)
	}
	if _, _, err := l.Load(ctx, "alien-run"); !errors.Is(err, run.ErrNotFound) {
		t.Fatalf("Load(alien-run) = %v, want run.ErrNotFound", err)
	}

	var state string
	if err := pool.QueryRow(ctx, fmt.Sprintf(
		"SELECT state FROM %s WHERE correlation_id = 'alien-run'", runsTable)).Scan(&state); err != nil {
		t.Fatalf("read alien run: %v", err)
	}
	if state != "pending" {
		t.Fatalf("alien run state = %q, want untouched 'pending'", state)
	}
}

// TestValidateActionableErrors checks Validate's failure modes: the error
// says exactly what to create.
func TestValidateActionableErrors(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	t.Run("missing unique index", func(t *testing.T) {
		m := pgledger.DefaultMapping()
		m.Runs.Table = uniqueName("dt_runs_noidx")
		m.Records.Table = uniqueName("dt_records_noidx")

		// Tables exist but carry none of the required unique indexes.
		applyDDL(t, pool, fmt.Sprintf(`
CREATE TABLE %s (run_id text, status text, envelope jsonb, created_at timestamptz);
CREATE TABLE %s (run_id text, record_key text, envelope jsonb)`,
			m.Runs.Table, m.Records.Table))
		dropTables(t, pool, m.Runs.Table, m.Records.Table)

		l, err := pgledger.New(pool, m)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		err = l.Validate(ctx)
		if err == nil {
			t.Fatal("Validate = nil, want error for missing unique indexes")
		}
		if !strings.Contains(err.Error(), "CREATE UNIQUE INDEX") {
			t.Fatalf("Validate error must suggest CREATE UNIQUE INDEX, got:\n%v", err)
		}
		if !strings.Contains(err.Error(), m.Runs.Table) || !strings.Contains(err.Error(), m.Records.Table) {
			t.Fatalf("Validate error must name both offending tables, got:\n%v", err)
		}
	})

	t.Run("non-plain unique indexes rejected", func(t *testing.T) {
		m := pgledger.DefaultMapping()
		m.Runs.Table = uniqueName("dt_runs_nonplain")
		m.Records.Table = uniqueName("dt_records_nonplain")

		// Both tables carry unique indexes on the mapped columns, but
		// neither is usable as an ON CONFLICT arbiter: the runs index
		// is partial, the records index has an expression key. A laxer
		// Validate would pass here and Accept/Record would then fail at
		// runtime.
		applyDDL(t, pool, fmt.Sprintf(`
CREATE TABLE %[1]s (run_id text, status text, envelope jsonb, created_at timestamptz);
CREATE UNIQUE INDEX ON %[1]s (run_id) WHERE status = 'pending';
CREATE TABLE %[2]s (run_id text, record_key text, envelope jsonb);
CREATE UNIQUE INDEX ON %[2]s (run_id, lower(record_key))`,
			m.Runs.Table, m.Records.Table))
		dropTables(t, pool, m.Runs.Table, m.Records.Table)

		l, err := pgledger.New(pool, m)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		err = l.Validate(ctx)
		if err == nil {
			t.Fatal("Validate = nil, want error: partial and expression unique indexes cannot back ON CONFLICT")
		}
		if !strings.Contains(err.Error(), "non-partial") || !strings.Contains(err.Error(), "CREATE UNIQUE INDEX") {
			t.Fatalf("Validate error must demand a plain unique index and suggest creating one, got:\n%v", err)
		}
		if !strings.Contains(err.Error(), m.Runs.Table) || !strings.Contains(err.Error(), m.Records.Table) {
			t.Fatalf("Validate error must name both offending tables, got:\n%v", err)
		}
	})

	t.Run("missing table", func(t *testing.T) {
		m := pgledger.DefaultMapping()
		m.Runs.Table = uniqueName("dt_runs_missing")
		m.Records.Table = uniqueName("dt_records_missing")
		l, err := pgledger.New(pool, m)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		err = l.Validate(ctx)
		if err == nil {
			t.Fatal("Validate = nil, want error for missing tables")
		}
		if !strings.Contains(err.Error(), "does not exist") || !strings.Contains(err.Error(), "CREATE TABLE") {
			t.Fatalf("Validate error must say the table is missing and suggest CREATE TABLE, got:\n%v", err)
		}
	})

	t.Run("missing column and wrong envelope type", func(t *testing.T) {
		m := pgledger.DefaultMapping()
		m.Runs.Table = uniqueName("dt_runs_badcols")
		m.Records.Table = uniqueName("dt_records_badcols")

		applyDDL(t, pool, fmt.Sprintf(`
CREATE TABLE %[1]s (run_id text, envelope integer, created_at timestamptz);
CREATE UNIQUE INDEX ON %[1]s (run_id);
CREATE TABLE %[2]s (run_id text, record_key text, envelope jsonb);
CREATE UNIQUE INDEX ON %[2]s (run_id, record_key)`,
			m.Runs.Table, m.Records.Table))
		dropTables(t, pool, m.Runs.Table, m.Records.Table)

		l, err := pgledger.New(pool, m)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		err = l.Validate(ctx)
		if err == nil {
			t.Fatal("Validate = nil, want error")
		}
		if !strings.Contains(err.Error(), "ALTER TABLE") {
			t.Fatalf("Validate error must suggest ALTER TABLE, got:\n%v", err)
		}
		if !strings.Contains(err.Error(), "status") {
			t.Fatalf("Validate error must name the missing status column, got:\n%v", err)
		}
		if !strings.Contains(err.Error(), "jsonb") {
			t.Fatalf("Validate error must call out the envelope type, got:\n%v", err)
		}
	})
}

// TestTextEnvelope exercises the tolerated text-typed envelope column
// through the full lifecycle: Complete's JSON merge must adapt.
func TestTextEnvelope(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	m := pgledger.DefaultMapping()
	m.Runs.Table = uniqueName("dt_runs_text")
	m.Records.Table = uniqueName("dt_records_text")

	applyDDL(t, pool, fmt.Sprintf(`
CREATE TABLE %[1]s (run_id text NOT NULL, status text NOT NULL, envelope text NOT NULL, created_at timestamptz NOT NULL);
CREATE UNIQUE INDEX ON %[1]s (run_id);
CREATE TABLE %[2]s (run_id text NOT NULL, record_key text NOT NULL, envelope text NOT NULL);
CREATE UNIQUE INDEX ON %[2]s (run_id, record_key)`,
		m.Runs.Table, m.Records.Table))
	dropTables(t, pool, m.Runs.Table, m.Records.Table)

	l, err := pgledger.New(pool, m)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := l.Validate(ctx); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	r := run.Run{ID: "r-text", Name: "wf", Input: []byte("in"), Status: run.StatusPending, CreatedAt: time.Now()}
	if err := l.Accept(ctx, r); err != nil {
		t.Fatalf("Accept: %v", err)
	}
	if _, err := l.Record(ctx, run.Record{RunID: "r-text", Key: "s#0", Kind: run.KindStep, Status: run.RecordOK, Output: []byte("v")}); err != nil {
		t.Fatalf("Record: %v", err)
	}
	if err := l.Complete(ctx, "r-text", 2, run.Result{Status: run.StatusSucceeded, Output: []byte("out")}); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	got, recs, err := l.Load(ctx, "r-text")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.Status != run.StatusSucceeded || string(got.Output) != "out" || got.Attempt != 2 || got.CompletedAt.IsZero() {
		t.Fatalf("Load after Complete = %+v, want succeeded/out/attempt 2/completed", got)
	}
	if len(recs) != 1 || recs[0].Key != "s#0" || string(recs[0].Output) != "v" {
		t.Fatalf("records = %+v, want one s#0 with output v", recs)
	}
	batch, err := l.ListPending(ctx, time.Now().Add(time.Hour), run.PendingCursor{}, 10)
	if err != nil {
		t.Fatalf("ListPending: %v", err)
	}
	if len(batch) != 0 {
		t.Fatalf("ListPending = %v, want empty after Complete", batch)
	}

	// GetRun must work over the text envelope too: the point read decodes
	// the same envelope Load does, minus the record prefetch.
	gotRun, err := l.GetRun(ctx, "r-text")
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Status != run.StatusSucceeded || string(gotRun.Output) != "out" {
		t.Fatalf("GetRun = %+v, want the completed run", gotRun)
	}
}

// TestGetRun exercises the optional ledger.RunGetter capability the way the
// runtime discovers it — by type assertion — and checks that the point read
// returns the run without needing its records.
func TestGetRun(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	l := newDefaultLedger(t, pool)

	getter, ok := l.(ledger.RunGetter)
	if !ok {
		t.Fatal("pgledger.Ledger does not implement ledger.RunGetter")
	}

	r := run.Run{ID: "r-get", Name: "wf", Input: []byte("in"), Status: run.StatusPending, CreatedAt: time.Now().UTC()}
	if err := l.Accept(ctx, r); err != nil {
		t.Fatalf("Accept: %v", err)
	}
	if _, err := l.Record(ctx, run.Record{
		RunID: "r-get", Key: "s#0", Kind: run.KindStep, Status: run.RecordOK,
		Codec: "application/json", Output: []byte("v"),
	}); err != nil {
		t.Fatalf("Record: %v", err)
	}

	got, err := getter.GetRun(ctx, "r-get")
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if got.ID != "r-get" || got.Name != "wf" || string(got.Input) != "in" || got.Status != run.StatusPending {
		t.Fatalf("GetRun = %+v, want the accepted run", got)
	}

	if _, err := getter.GetRun(ctx, "r-missing"); !errors.Is(err, run.ErrNotFound) {
		t.Fatalf("GetRun(missing) = %v, want run.ErrNotFound", err)
	}

	// The record's Codec field must round-trip through the envelope: the
	// adapter stores and decodes the whole struct, stripping nothing.
	_, recs, err := l.Load(ctx, "r-get")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(recs) != 1 || recs[0].Codec != "application/json" {
		t.Fatalf("records = %+v, want one record with Codec application/json", recs)
	}
}
