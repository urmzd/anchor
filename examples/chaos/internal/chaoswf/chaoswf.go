// Package chaoswf is the workflow shared by the chaos worker binary, the
// demo driver, and the kill test: four sequential activities, each folding
// its name into the output and emitting a progress delta.
//
// Every activity also appends one row to the chaos_effects table through a
// plain pgxpool — raw SQL, not anchor. That table is the ledger-external
// witness of side effects: the kill test reads it to prove that recorded
// activities never re-executed across a real process death.
package chaoswf

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	anchor "github.com/urmzd/anchor"
)

// EffectsDDL creates the witness table. It belongs to the example, not to
// anchor; the worker binary creates it on startup and the kill test resets
// it.
const EffectsDDL = `CREATE TABLE IF NOT EXISTS chaos_effects (
	run_id   text        NOT NULL,
	activity text        NOT NULL,
	at       timestamptz NOT NULL DEFAULT now()
)`

// Effects is the pool the witness writes go through. The worker binary sets
// it before running; processes that never execute activities (the driver,
// the test) leave it nil.
var Effects *pgxpool.Pool

// StageNames lists the activities in call order.
var StageNames = []string{"stage-1", "stage-2", "stage-3", "stage-4"}

// Expected returns the workflow output for a seed — what the kill test
// asserts against after the run survives a SIGKILL.
func Expected(seed string) string {
	out := seed
	for _, name := range StageNames {
		out += "|" + name
	}
	return out
}

var stages = []*anchor.ActivityFn[string, string]{
	stage(1), stage(2), stage(3), stage(4),
}

// Process is the chaos workflow entry point.
var Process = anchor.Activity("chaos-process", process)

func process(ctx context.Context, in string) (string, error) {
	out := in
	for i, s := range stages {
		var err error
		if out, err = s.Call(ctx, out); err != nil {
			return "", err
		}
		_ = anchor.Emit(ctx, delta{Stage: StageNames[i], Value: out})
	}
	return out, nil
}

type delta struct {
	Stage string `json:"stage"`
	Value string `json:"value"`
}

// stage builds activity "stage-<n>". Its sleep comes from the
// CHAOS_SLEEP_STAGE_<n> env var (a Go duration, read at process start), so
// the kill test can hold one stage mid-flight while it aims the SIGKILL.
func stage(n int) *anchor.ActivityFn[string, string] {
	name := fmt.Sprintf("stage-%d", n)
	sleep, _ := time.ParseDuration(os.Getenv(fmt.Sprintf("CHAOS_SLEEP_STAGE_%d", n)))
	return anchor.Activity(name, func(ctx context.Context, in string) (string, error) {
		// Witness first, then sleep: a kill mid-sleep still leaves the row —
		// the side effect happened even though no record was written.
		if err := witness(ctx, name); err != nil {
			return "", err
		}
		if sleep > 0 {
			select {
			case <-time.After(sleep):
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}
		return in + "|" + name, nil
	})
}

// witness appends one invocation row, outside anchor entirely.
func witness(ctx context.Context, activity string) error {
	if Effects == nil {
		return nil
	}
	info, _ := anchor.FromContext(ctx)
	_, err := Effects.Exec(ctx,
		`INSERT INTO chaos_effects (run_id, activity) VALUES ($1, $2)`,
		info.RunID, activity)
	if err != nil {
		return fmt.Errorf("chaos_effects insert: %w", err)
	}
	return nil
}
