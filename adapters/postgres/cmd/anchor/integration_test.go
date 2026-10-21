//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/urmzd/anchor"
	"github.com/urmzd/anchor/adapters/postgres/pgledger"
	"github.com/urmzd/anchor/pkg/queue"
	"github.com/urmzd/anchor/pkg/run"
	"github.com/urmzd/anchor/pkg/worker"
)

// The harness owns all DDL, as in pgledger's integration tests: it applies
// pgledger.RecommendedDDL to fresh, uniquely named tables through its own
// connection, then drives the CLI against them.

func testURL() string {
	if u := os.Getenv("ANCHOR_POSTGRES_URL"); u != "" {
		return u
	}
	return "postgres://anchor:anchor@localhost:5432/anchor"
}

var tableSeq atomic.Int64

type fixture struct {
	url     string
	pool    *pgxpool.Pool
	lgr     *pgledger.Ledger
	m       pgledger.Mapping
	baseCLI []string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	ctx := context.Background()
	u := testURL()
	pool, err := pgxpool.New(ctx, u)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping: %v (is docker compose up?)", err)
	}

	n := fmt.Sprintf("%d_%d", time.Now().UnixMilli(), tableSeq.Add(1))
	m := pgledger.DefaultMapping()
	m.Runs.Table, m.Records.Table = "cli_runs_"+n, "cli_records_"+n
	for _, stmt := range strings.Split(pgledger.RecommendedDDL(m), ";") {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if _, err := pool.Exec(ctx, stmt); err != nil {
			t.Fatalf("apply DDL: %v", err)
		}
	}
	t.Cleanup(func() {
		for _, tab := range []string{m.Runs.Table, m.Records.Table} {
			if _, err := pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+tab); err != nil {
				t.Errorf("drop %s: %v", tab, err)
			}
		}
	})
	lgr, err := pgledger.New(pool, m)
	if err != nil {
		t.Fatalf("pgledger.New: %v", err)
	}
	return &fixture{
		url: u, pool: pool, lgr: lgr, m: m,
		baseCLI: []string{"-runs-table", m.Runs.Table, "-records-table", m.Records.Table},
	}
}

// cli runs the CLI against the fixture's tables, the URL coming from the
// environment, and checks the output never carries the URL.
func (f *fixture) cli(t *testing.T, cmd string, args ...string) (int, string, string) {
	t.Helper()
	full := append([]string{cmd}, f.baseCLI...)
	full = append(full, args...)
	code, out, errOut := cli(t, map[string]string{envDatabaseURL: f.url}, full...)
	f.noURL(t, out+errOut)
	return code, out, errOut
}

func (f *fixture) noURL(t *testing.T, s string) {
	t.Helper()
	if strings.Contains(s, f.url) {
		t.Fatalf("output carries the database URL:\n%s", s)
	}
	if u, err := url.Parse(f.url); err == nil && u.User != nil {
		if pw, ok := u.User.Password(); ok && pw != "" && strings.Contains(s, ":"+pw+"@") {
			t.Fatalf("output carries the database password:\n%s", s)
		}
	}
}

func (f *fixture) rowCount(t *testing.T) int {
	t.Helper()
	var n int
	err := f.pool.QueryRow(context.Background(), fmt.Sprintf(
		"SELECT (SELECT count(*) FROM %s) + (SELECT count(*) FROM %s)", f.m.Runs.Table, f.m.Records.Table)).Scan(&n)
	if err != nil {
		t.Fatalf("count rows: %v", err)
	}
	return n
}

type item struct {
	ID  string
	Qty int
}

// seed produces real histories with the SDK and a worker on the fixture's
// ledger: a succeeded run with nested and keyed calls and a step, a run
// parked on an event, and a run failed by a recorded failure. Two more are
// written by hand: a run whose worker died inside a parent call, and a run
// recorded with another codec.
func (f *fixture) seed(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	reg := run.NewRegistry()
	charge := anchor.ActivityIn(reg, "charge-payment", func(_ context.Context, it item) (string, error) { return "ch_" + it.ID, nil })
	pick := anchor.ActivityIn(reg, "pick", func(context.Context, item) (anchor.Void, error) { return anchor.Void{}, nil })
	fulfil := anchor.ActivityIn(reg, "fulfil", func(ctx context.Context, it item) (int, error) {
		for range it.Qty {
			if _, err := pick.Call(ctx, it); err != nil {
				return 0, err
			}
		}
		return it.Qty, nil
	})
	refuse := anchor.ActivityIn(reg, "refuse", func(context.Context, item) (string, error) {
		return "", anchor.NonRetryable(errors.New("card declined"))
	})
	order := anchor.ActivityIn(reg, "order", func(ctx context.Context, it item) (string, error) {
		if _, err := charge.Call(ctx, it); err != nil {
			return "", err
		}
		if _, err := fulfil.CallKeyed(ctx, "fulfil-"+it.ID, it); err != nil {
			return "", err
		}
		at, err := anchor.Step(ctx, "issued-at", func(context.Context) (time.Time, error) { return time.Now(), nil })
		return "receipt@" + at.Format(time.RFC3339), err
	})
	approve := anchor.ActivityIn(reg, "approve", func(ctx context.Context, it item) (string, error) {
		if _, err := charge.Call(ctx, it); err != nil {
			return "", err
		}
		return anchor.Event[string](ctx, "approval")
	})
	decline := anchor.ActivityIn(reg, "decline", func(ctx context.Context, it item) (string, error) {
		return refuse.Call(ctx, it)
	})

	q := queue.NewMemory()
	c := anchor.New(f.lgr, q)
	wctx, stop := context.WithCancel(ctx)
	w := worker.New(f.lgr, q, worker.WithRegistry(reg), worker.WithJanitorEvery(0),
		worker.WithBackoff(func(int) time.Duration { return time.Millisecond }))
	done := make(chan error, 1)
	go func() { done <- w.Run(wctx) }()

	if _, err := anchor.Exec(ctx, c, order, item{ID: "1042", Qty: 2}, anchor.WithRunID("order-1042")); err != nil {
		t.Fatalf("Exec order: %v", err)
	}
	var failed *anchor.RunFailedError
	if _, err := anchor.Exec(ctx, c, decline, item{ID: "13"}, anchor.WithRunID("decline-13")); !errors.As(err, &failed) {
		t.Fatalf("Exec decline = %v, want a failed run", err)
	}
	if _, err := anchor.Start(ctx, c, approve, item{ID: "77"}, anchor.WithRunID("approve-77")); err != nil {
		t.Fatalf("Start approve: %v", err)
	}
	f.waitRecords(t, "approve-77", 1)
	stop()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("worker: %v", err)
	}

	// By hand: what a worker leaves when it dies inside fulfil, after one
	// pick recorded and before fulfil itself did, and a run recorded by
	// workers with another codec.
	in := []byte(`{"ID":"9","Qty":2}`)
	for _, r := range []run.Run{
		{ID: "crashed-9", Name: "order", Input: in, Status: run.StatusPending, MaxAttempts: 5, CreatedAt: time.Now()},
		{ID: "protobuf-5", Name: "order", Input: in, Status: run.StatusPending, MaxAttempts: 5, CreatedAt: time.Now().Add(time.Millisecond)},
	} {
		if err := f.lgr.Accept(ctx, r); err != nil {
			t.Fatalf("Accept %s: %v", r.ID, err)
		}
	}
	for _, rec := range []run.Record{
		{RunID: "crashed-9", Key: "charge-payment#0", Kind: run.KindActivity, Name: "charge-payment", Codec: jsonCT, Output: []byte(`"ch_9"`)},
		{RunID: "crashed-9", Key: "k:fulfil-9/pick#0", Kind: run.KindActivity, Name: "pick", Codec: jsonCT, Output: []byte(`{}`)},
		{RunID: "protobuf-5", Key: "charge-payment#0", Kind: run.KindActivity, Name: "charge-payment", Codec: "application/x-protobuf", Output: []byte{0x0a, 0x04}},
	} {
		rec.Status, rec.Attempt, rec.InputHash = run.RecordOK, 1, run.HashInput(in)
		if _, err := f.lgr.Record(ctx, rec); err != nil {
			t.Fatalf("Record %s/%s: %v", rec.RunID, rec.Key, err)
		}
		time.Sleep(2 * time.Millisecond) // distinct recording times
	}
}

func (f *fixture) waitRecords(t *testing.T, runID string, n int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		_, recs, err := f.lgr.Load(context.Background(), runID)
		if err != nil {
			t.Fatalf("Load %s: %v", runID, err)
		}
		if len(recs) >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s has %d records, want %d", runID, len(recs), n)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func compact(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	var b bytes.Buffer
	if err := json.Compact(&b, raw); err != nil {
		t.Fatalf("compact %q: %v", raw, err)
	}
	return b.String()
}

func decodeJSON[T any](t *testing.T, s string) T {
	t.Helper()
	var v T
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		t.Fatalf("decode JSON: %v\n%s", err, s)
	}
	return v
}

func TestCLIRuns(t *testing.T) {
	f := newFixture(t)
	f.seed(t)

	code, out, errOut := f.cli(t, "runs")
	if code != exitOK {
		t.Fatalf("runs = %d: %s", code, errOut)
	}
	for _, id := range []string{"order-1042", "decline-13", "approve-77", "crashed-9", "protobuf-5"} {
		if !strings.Contains(out, id) {
			t.Fatalf("runs output lacks %s:\n%s", id, out)
		}
	}
	if !strings.Contains(out, "card declined") {
		t.Fatalf("runs output lacks the failed run's error:\n%s", out)
	}

	type listing struct {
		Runs []runJSON `json:"runs"`
	}
	ids := func(l listing) string {
		var out []string
		for _, r := range l.Runs {
			out = append(out, r.ID)
		}
		return strings.Join(out, ",")
	}
	for _, tc := range []struct {
		args []string
		want string
	}{
		{nil, "protobuf-5,crashed-9,approve-77,decline-13,order-1042"},
		{[]string{"-limit", "2"}, "protobuf-5,crashed-9"},
		{[]string{"-status", "pending"}, "protobuf-5,crashed-9,approve-77"},
		{[]string{"-status", "succeeded"}, "order-1042"},
		{[]string{"-status", "failed"}, "decline-13"},
	} {
		code, out, errOut := f.cli(t, "runs", append([]string{"-json"}, tc.args...)...)
		if code != exitOK {
			t.Fatalf("runs %v = %d: %s", tc.args, code, errOut)
		}
		l := decodeJSON[listing](t, out)
		if got := ids(l); got != tc.want {
			t.Fatalf("runs %v = %s, want %s", tc.args, got, tc.want)
		}
		for _, r := range l.Runs {
			if r.Input != nil || r.Output != nil {
				t.Fatalf("runs lists payloads for %s", r.ID)
			}
		}
	}

	// -db works as well as the environment.
	args := append([]string{"runs", "-db", f.url, "-status", "succeeded"}, f.baseCLI...)
	code, out, errOut = cli(t, nil, args...)
	f.noURL(t, out+errOut)
	if code != exitOK || !strings.Contains(out, "order-1042") {
		t.Fatalf("runs -db = %d, %q, %q", code, out, errOut)
	}
}

func TestCLIShow(t *testing.T) {
	f := newFixture(t)
	f.seed(t)

	code, out, errOut := f.cli(t, "show", "order-1042")
	if code != exitOK {
		t.Fatalf("show = %d: %s", code, errOut)
	}
	// Records in recorded order: nested calls record before their parent.
	order := []string{"charge-payment#0", "k:fulfil-1042/pick#0", "k:fulfil-1042/pick#1", "k:fulfil-1042 ", "issued-at#0"}
	at := 0
	for _, k := range order {
		i := strings.Index(out[at:], k)
		if i < 0 {
			t.Fatalf("show lists %q out of recorded order:\n%s", k, out)
		}
		at += i
	}
	for _, want := range []string{`{"ID":"1042","Qty":2}`, "records (5, in recorded order)", "status: succeeded at attempt 1"} {
		if !strings.Contains(out, want) {
			t.Fatalf("show output lacks %q:\n%s", want, out)
		}
	}

	code, out, errOut = f.cli(t, "show", "order-1042", "-json")
	if code != exitOK {
		t.Fatalf("show -json = %d: %s", code, errOut)
	}
	shown := decodeJSON[struct {
		Run     runJSON      `json:"run"`
		Records []recordJSON `json:"records"`
		Status  string       `json:"status"`
	}](t, out)
	if shown.Status != "succeeded" || shown.Run.ID != "order-1042" || compact(t, shown.Run.Input) != `{"ID":"1042","Qty":2}` {
		t.Fatalf("show -json run = %+v, status %q", shown.Run, shown.Status)
	}
	var got []string
	for _, rec := range shown.Records {
		got = append(got, rec.Key)
	}
	if strings.Join(got, ",") != "charge-payment#0,k:fulfil-1042/pick#0,k:fulfil-1042/pick#1,k:fulfil-1042,issued-at#0" {
		t.Fatalf("show -json records = %v", got)
	}
	if string(shown.Records[0].Output) != `"ch_1042"` || shown.Records[0].Codec != jsonCT || shown.Records[0].InputHash == "" {
		t.Fatalf("show -json first record = %+v", shown.Records[0])
	}

	// Binary payloads come out base64-encoded.
	code, out, _ = f.cli(t, "show", "-json", "protobuf-5")
	if code != exitOK || !strings.Contains(out, `"output_base64": "CgQ="`) {
		t.Fatalf("show -json protobuf-5 = %d:\n%s", code, out)
	}

	code, _, errOut = f.cli(t, "show", "no-such-run")
	if code != exitFailed || !strings.Contains(errOut, `run "no-such-run" not found`) {
		t.Fatalf("show missing = %d, %q", code, errOut)
	}
}

func TestCLIReplay(t *testing.T) {
	f := newFixture(t)
	f.seed(t)
	before := f.rowCount(t)

	replayJSONOf := func(t *testing.T, wantCode int, args ...string) replayJSON {
		t.Helper()
		code, out, errOut := f.cli(t, "replay", append([]string{"-json"}, args...)...)
		if code != wantCode {
			t.Fatalf("replay %v = %d, want %d: %s\n%s", args, code, wantCode, errOut, out)
		}
		return decodeJSON[replayJSON](t, out)
	}

	t.Run("succeeded", func(t *testing.T) {
		rep := replayJSONOf(t, exitOK, "order-1042")
		if !rep.OK || !rep.Complete || rep.Frontier != nil {
			t.Fatalf("replay = %+v, want ok and complete", rep)
		}
		var got []string
		for _, c := range rep.Replayed {
			got = append(got, c.Key)
		}
		if strings.Join(got, ",") != "charge-payment#0,k:fulfil-1042,issued-at#0" ||
			strings.Join(rep.Covered, ",") != "k:fulfil-1042/pick#0,k:fulfil-1042/pick#1" {
			t.Fatalf("replayed %v, covered %v", got, rep.Covered)
		}
		code, out, _ := f.cli(t, "replay", "order-1042")
		if code != exitOK || !strings.Contains(out, "complete: succeeded at attempt 1") {
			t.Fatalf("replay text = %d:\n%s", code, out)
		}
	})

	t.Run("failed", func(t *testing.T) {
		rep := replayJSONOf(t, exitOK, "decline-13")
		if !rep.OK || !rep.Complete || len(rep.Replayed) != 1 || !rep.Replayed[0].Failed || rep.Replayed[0].Key != "refuse#0" {
			t.Fatalf("replay = %+v, want a complete run replaying the recorded failure", rep)
		}
	})

	t.Run("parked", func(t *testing.T) {
		rep := replayJSONOf(t, exitOK, "approve-77")
		if !rep.OK || rep.Complete || rep.Frontier == nil || rep.Frontier.After != "charge-payment#0" {
			t.Fatalf("replay = %+v frontier %+v, want resume after charge-payment#0", rep, rep.Frontier)
		}
		code, out, _ := f.cli(t, "replay", "approve-77")
		if code != exitOK || !strings.Contains(out, "resume: the next call after charge-payment#0 executes for real") {
			t.Fatalf("replay text = %d:\n%s", code, out)
		}
	})

	t.Run("crashed inside a parent", func(t *testing.T) {
		rep := replayJSONOf(t, exitOK, "crashed-9")
		fr := rep.Frontier
		if !rep.OK || fr == nil || strings.Join(fr.Reexecutes, ",") != "k:fulfil-9" || fr.After != "k:fulfil-9/pick#0" {
			t.Fatalf("replay = %+v frontier %+v, want k:fulfil-9 re-executing after its first pick", rep, fr)
		}
	})

	t.Run("another codec", func(t *testing.T) {
		rep := replayJSONOf(t, exitDiverged, "protobuf-5")
		if rep.OK || rep.Divergence == nil || rep.Divergence.Key != "charge-payment#0" || !strings.Contains(rep.Divergence.Message, "codec") {
			t.Fatalf("replay = %+v, want a codec divergence", rep)
		}
		code, out, _ := f.cli(t, "replay", "protobuf-5")
		if code != exitDiverged || !strings.Contains(out, "diverges: charge-payment#0") {
			t.Fatalf("replay text = %d:\n%s", code, out)
		}
		if rep := replayJSONOf(t, exitOK, "-codec", "application/x-protobuf", "protobuf-5"); !rep.OK {
			t.Fatalf("replay with the recording codec = %+v", rep)
		}
	})

	t.Run("missing", func(t *testing.T) {
		code, _, errOut := f.cli(t, "replay", "no-such-run")
		if code != exitFailed || !strings.Contains(errOut, "not found") {
			t.Fatalf("replay missing = %d, %q", code, errOut)
		}
	})

	if after := f.rowCount(t); after != before {
		t.Fatalf("rows went from %d to %d across the CLI's commands, want no writes", before, after)
	}

	t.Run("signaled", func(t *testing.T) {
		// The approval arrives: the replay now gets past the event.
		if err := anchor.New(f.lgr, queue.NewMemory()).Signal(context.Background(), "approve-77", "approval", "ok"); err != nil {
			t.Fatalf("Signal: %v", err)
		}
		rep := replayJSONOf(t, exitOK, "approve-77")
		if !rep.OK || rep.Frontier == nil || rep.Frontier.After != "event:approval#0" {
			t.Fatalf("replay = %+v frontier %+v, want resume after event:approval#0", rep, rep.Frontier)
		}
	})
}

func TestCLIValidatesTheSchema(t *testing.T) {
	f := newFixture(t)
	code, _, errOut := cli(t, map[string]string{envDatabaseURL: f.url}, "runs", "-runs-table", "cli_no_such_table", "-records-table", f.m.Records.Table)
	f.noURL(t, errOut)
	if code != exitFailed || !strings.Contains(errOut, `runs table "cli_no_such_table" does not exist`) || !strings.Contains(errOut, "CREATE TABLE") {
		t.Fatalf("runs on a missing table = %d:\n%s", code, errOut)
	}
}

// TestCLIConnectionsAreReadOnly: the server refuses a write on the CLI's
// connections, whatever code runs over them.
func TestCLIConnectionsAreReadOnly(t *testing.T) {
	f := newFixture(t)
	cfg, err := poolConfig(f.url)
	if err != nil {
		t.Fatalf("poolConfig: %v", err)
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()
	_, err = pool.Exec(context.Background(), fmt.Sprintf(
		`INSERT INTO %s (run_id, status, envelope) VALUES ('w', 'pending', '{}')`, f.m.Runs.Table))
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "25006" {
		t.Fatalf("insert over the CLI's connection = %v, want SQLSTATE 25006 read_only_sql_transaction", err)
	}
}
