//go:build integration

// The cross-process chaos e2e — this directory doubles as the repo's e2e
// suite (`make test-integration` runs it against the docker-compose infra).
//
// The test builds the worker binary, SIGKILLs one instance mid-activity, and
// starts another. Truth to assert against comes from three places anchor
// does not control: the chaos_effects witness table (raw SQL side effects),
// the ledger's records, and the Redis delta stream.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	anchor "github.com/urmzd/anchor"
	"github.com/urmzd/anchor/adapters/postgres/pgledger"
	"github.com/urmzd/anchor/adapters/redis/redisqueue"
	"github.com/urmzd/anchor/examples/chaos/internal/chaoswf"
	"github.com/urmzd/anchor/pkg/queue"
	"github.com/urmzd/anchor/pkg/run"
)

func TestKill_CrossProcessCrashRecovery(t *testing.T) {
	ctx := context.Background()
	pgURL := env("ANCHOR_POSTGRES_URL", "postgres://anchor:anchor@localhost:5432/anchor")
	redisAddr := env("ANCHOR_REDIS_ADDR", "localhost:6379")

	// (1) Real infra. Ledger tables come from the adapter's own suggested
	// DDL; chaos_effects is reset wholesale — it is this test's instrument.
	// Runs are isolated by a fresh run ID and a fresh Redis namespace, so
	// leftovers from earlier compose volumes are harmless.
	pool, err := pgxpool.New(ctx, pgURL)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer pool.Close()
	mustExec(t, pool, pgledger.RecommendedDDL(pgledger.DefaultMapping()))
	mustExec(t, pool, `DROP TABLE IF EXISTS chaos_effects`)
	mustExec(t, pool, chaoswf.EffectsDDL)

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("connect redis: %v", err)
	}
	namespace := fmt.Sprintf("chaos-kill-%d", time.Now().UnixNano())

	// (2) Build the worker binary the way an operator would.
	bin := filepath.Join(t.TempDir(), "chaos-worker")
	if out, err := exec.Command("go", "build", "-o", bin, "./worker").CombinedOutput(); err != nil {
		t.Fatalf("build worker: %v\n%s", err, out)
	}
	baseEnv := append(os.Environ(),
		"ANCHOR_POSTGRES_URL="+pgURL,
		"ANCHOR_REDIS_ADDR="+redisAddr,
		"ANCHOR_REDIS_NAMESPACE="+namespace,
	)

	// (3) Worker process 1: stage-3 sleeps ~3s, holding the run mid-activity
	// long enough to aim the kill.
	w1 := startWorker(t, bin, append(baseEnv, "CHAOS_SLEEP_STAGE_3=3s"))

	// (4) Submit from this process — the client side of the story.
	lgr, err := pgledger.New(pool, pgledger.DefaultMapping())
	if err != nil {
		t.Fatalf("ledger mapping: %v", err)
	}
	q := redisqueue.New(rdb, namespace)
	c := anchor.New(lgr, q)
	runID := fmt.Sprintf("kill-%d", time.Now().UnixNano())
	h, err := anchor.Start(ctx, c, chaoswf.Process, "seed", anchor.WithRunID(runID))
	if err != nil {
		t.Fatalf("start run: %v", err)
	}

	// (5) Wait until stages 1 and 2 are checkpointed — stage-3 is then in
	// its sleep — and SIGKILL worker 1. A real kill: no defer runs, no
	// release, no goodbye.
	waitForRecords(t, lgr, runID, 2)
	if err := w1.Process.Kill(); err != nil {
		t.Fatalf("kill worker 1: %v", err)
	}
	_ = w1.Wait()
	t.Logf("worker 1 (pid %d) killed mid stage-3", w1.Process.Pid)

	// (6) Worker process 2, no slow stage. It claims the run once the dead
	// worker's lease lapses (~2s) and resumes at the frontier.
	w2 := startWorker(t, bin, baseEnv)
	defer func() {
		_ = w2.Process.Kill()
		_ = w2.Wait()
	}()

	// (7) The run completes with the exact expected output.
	resCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	out, err := h.Result(resCtx)
	if err != nil {
		t.Fatalf("result after kill: %v", err)
	}
	if want := chaoswf.Expected("seed"); out != want {
		t.Fatalf("output = %q, want %q", out, want)
	}

	// The witness table: recorded activities never re-executed across the
	// kill; only the frontier activity sits in the at-least-once window.
	effects := effectCounts(t, pool, runID)
	for _, name := range []string{"stage-1", "stage-2", "stage-4"} {
		if effects[name] != 1 {
			t.Errorf("chaos_effects[%s] = %d, want exactly 1", name, effects[name])
		}
	}
	if n := effects["stage-3"]; n < 1 || n > 2 {
		t.Errorf("chaos_effects[stage-3] = %d, want 1..2 (killed mid-flight)", n)
	}

	// The ledger: exactly one record per key, converged across both workers.
	_, records, err := lgr.Load(ctx, runID)
	if err != nil {
		t.Fatalf("load ledger: %v", err)
	}
	keys := map[string]int{}
	for _, rec := range records {
		keys[rec.Key]++
	}
	for _, name := range chaoswf.StageNames {
		if keys[name+"#0"] != 1 {
			t.Errorf("ledger records for %s#0 = %d, want exactly 1", name, keys[name+"#0"])
		}
	}
	if len(records) != len(chaoswf.StageNames) {
		t.Errorf("ledger has %d records, want %d: %v", len(records), len(chaoswf.StageNames), keys)
	}

	// (8) The delta stream carries the divider that splits attempt 1's
	// partial output from attempt 2's — the divider-split story crossing a
	// real process death.
	if !hasAttemptDivider(t, q, runID, 2) {
		t.Error("delta stream has no attempt divider for attempt >= 2 after the kill")
	}
}

// startWorker launches one worker process wired to the test's infra.
func startWorker(t *testing.T, bin string, env []string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(bin)
	cmd.Env = env
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start worker: %v", err)
	}
	return cmd
}

// waitForRecords polls the ledger until the run has at least n successful
// activity records.
func waitForRecords(t *testing.T, lgr *pgledger.Ledger, runID string, n int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		_, records, err := lgr.Load(context.Background(), runID)
		if err != nil && !errors.Is(err, run.ErrNotFound) {
			t.Fatalf("load: %v", err)
		}
		got := 0
		for _, rec := range records {
			if rec.Kind == run.KindActivity && rec.Status == run.RecordOK {
				got++
			}
		}
		if got >= n {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("run %s never reached %d recorded activities", runID, n)
}

// effectCounts reads the witness table: invocations per activity for runID.
func effectCounts(t *testing.T, pool *pgxpool.Pool, runID string) map[string]int {
	t.Helper()
	rows, err := pool.Query(context.Background(),
		`SELECT activity, count(*) FROM chaos_effects WHERE run_id = $1 GROUP BY activity`, runID)
	if err != nil {
		t.Fatalf("query chaos_effects: %v", err)
	}
	defer rows.Close()
	counts := map[string]int{}
	for rows.Next() {
		var activity string
		var n int
		if err := rows.Scan(&activity, &n); err != nil {
			t.Fatalf("scan chaos_effects: %v", err)
		}
		counts[activity] = n
	}
	return counts
}

// hasAttemptDivider reads the run's delta log (bounded snapshot, no tail)
// and reports whether a framework attempt divider for attempt >= minAttempt
// exists.
func hasAttemptDivider(t *testing.T, dl queue.DeltaLog, runID string, minAttempt int) bool {
	t.Helper()
	cursor := queue.Cursor("")
	for {
		batch, next, err := dl.Read(context.Background(), runID, cursor, 512)
		if err != nil {
			t.Fatalf("read deltas: %v", err)
		}
		for _, d := range batch {
			if d.Kind == run.DeltaAttempt && d.Attempt >= minAttempt {
				return true
			}
		}
		if len(batch) == 0 || next == cursor {
			return false
		}
		cursor = next
	}
}

// mustExec runs sql over the simple protocol, so multi-statement DDL strings
// (like RecommendedDDL's output) execute in one call.
func mustExec(t *testing.T, pool *pgxpool.Pool, sql string) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), sql, pgx.QueryExecModeSimpleProtocol); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}
