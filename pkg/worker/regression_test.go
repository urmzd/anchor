package worker_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	anchor "github.com/urmzd/anchor"
	"github.com/urmzd/anchor/pkg/ledger"
	"github.com/urmzd/anchor/pkg/queue"
	"github.com/urmzd/anchor/pkg/run"
	"github.com/urmzd/anchor/pkg/worker"
)

// Regression: review finding — parked runs must never burn retry budget.
// With a tight MaxAttempts and an aggressive janitor re-enqueueing the
// parked run every tick, the run must survive many park/resume cycles and
// still complete once signaled.
func TestRegression_ParkBurnsNoBudget(t *testing.T) {
	lgr := ledger.NewMemory()
	q := queue.NewMemory()
	reg := run.NewRegistry()
	c := anchor.New(lgr, q)

	wf := anchor.ActivityIn(reg, "gated", func(ctx context.Context, _ anchor.Void) (string, error) {
		return anchor.Event[string](ctx, "approval")
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := worker.New(lgr, q,
		worker.WithRegistry(reg),
		worker.WithLeaseTTL(30*time.Millisecond),
		worker.WithHeartbeatEvery(10*time.Millisecond),
		worker.WithJanitorEvery(15*time.Millisecond), // hammer the parked run
		worker.WithConcurrency(2),
	)
	go func() { _ = w.Run(ctx) }()

	h, err := anchor.Start(context.Background(), c, wf, anchor.Void{}, anchor.WithMaxAttempts(2))
	if err != nil {
		t.Fatal(err)
	}

	// Let the janitor drive many park/resume cycles — far more than the
	// budget of 2 — while the event stays absent.
	deadline := time.Now().Add(600 * time.Millisecond)
	for time.Now().Before(deadline) {
		r, gerr := lgr.GetRun(context.Background(), h.RunID())
		if gerr != nil {
			t.Fatal(gerr)
		}
		if r.Status.Terminal() {
			t.Fatalf("run went terminal while merely waiting: %s %q", r.Status, r.Error)
		}
		time.Sleep(20 * time.Millisecond)
	}

	if err := c.Signal(context.Background(), h.RunID(), "approval", "granted"); err != nil {
		t.Fatal(err)
	}
	resCtx, resCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer resCancel()
	out, err := h.Result(resCtx)
	if err != nil {
		t.Fatalf("run failed after signal: %v", err)
	}
	if out != "granted" {
		t.Fatalf("result = %q, want granted", out)
	}
}

// ctxCheckQueue fails the test if any outcome verb arrives on an
// already-canceled context — the review-confirmed graceful-shutdown bug.
type ctxCheckQueue struct {
	*queue.Memory
	t        *testing.T
	released atomic.Int32
}

func (c *ctxCheckQueue) Release(ctx context.Context, it queue.Item, delay time.Duration, failed bool) error {
	if ctx.Err() != nil {
		c.t.Errorf("Release issued on canceled context")
	}
	if failed {
		c.t.Errorf("shutdown release must not consume retry budget")
	}
	c.released.Add(1)
	return c.Memory.Release(ctx, it, delay, failed)
}

func (c *ctxCheckQueue) Settle(ctx context.Context, it queue.Item) error {
	if ctx.Err() != nil {
		c.t.Errorf("Settle issued on canceled context")
	}
	return c.Memory.Settle(ctx, it)
}

// Regression: review finding — graceful shutdown must persist outcomes on a
// cancellation-detached context and hand interrupted runs back without
// burning budget; a second worker then finishes the run.
func TestRegression_GracefulShutdownReleasesCleanly(t *testing.T) {
	lgr := ledger.NewMemory()
	q := &ctxCheckQueue{Memory: queue.NewMemory(), t: t}
	reg := run.NewRegistry()
	c := anchor.New(lgr, q)

	started := make(chan struct{}, 4)
	block := make(chan struct{})
	var executions atomic.Int32
	wf := anchor.ActivityIn(reg, "slow", func(ctx context.Context, _ anchor.Void) (string, error) {
		executions.Add(1)
		started <- struct{}{}
		select {
		case <-block:
			return "done", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	})

	ctx1, cancel1 := context.WithCancel(context.Background())
	w1 := worker.New(lgr, q,
		worker.WithRegistry(reg),
		worker.WithLeaseTTL(30*time.Second), // expiry must NOT be the recovery path
		worker.WithJanitorEvery(0),
		worker.WithConcurrency(1),
	)
	w1Done := make(chan struct{})
	go func() { _ = w1.Run(ctx1); close(w1Done) }()

	h, err := anchor.Start(context.Background(), c, wf, anchor.Void{}, anchor.WithMaxAttempts(1))
	if err != nil {
		t.Fatal(err)
	}
	<-started // worker 1 is mid-run
	cancel1() // graceful shutdown: the run must be RELEASED, not abandoned
	<-w1Done

	if q.released.Load() == 0 {
		t.Fatal("shutdown did not release the in-flight run")
	}

	// MaxAttempts is 1: if the shutdown had consumed budget, worker 2 would
	// terminally fail the run instead of completing it.
	close(block)
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	w2 := worker.New(lgr, q,
		worker.WithRegistry(reg),
		worker.WithLeaseTTL(30*time.Second),
		worker.WithJanitorEvery(0),
		worker.WithConcurrency(1),
	)
	go func() { _ = w2.Run(ctx2) }()

	resCtx, resCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer resCancel()
	out, err := h.Result(resCtx)
	if err != nil {
		t.Fatalf("run failed after clean shutdown handoff: %v", err)
	}
	if out != "done" {
		t.Fatalf("result = %q, want done", out)
	}
	if got := executions.Load(); got != 2 {
		t.Fatalf("executions = %d, want 2 (interrupted once, completed once)", got)
	}
}
