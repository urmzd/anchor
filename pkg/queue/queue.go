// Package queue defines the flow-and-clock interface: delivery of run IDs to
// workers under time-bounded, fenced claims. The queue carries only IDs —
// data lives in the ledger — and every queue backend is disposable: its
// entire content is derivable from the ledger, so wiping it loses no runs.
//
// Lifecycle per claim: claim → heartbeat* → exactly one of settle, release,
// expire. Delivery is at-least-once; exactly-once observation is
// manufactured above by the ledger's first-write-wins records and
// first-terminal-wins Complete.
package queue

import (
	"context"
	"time"
)

// Item is a claimed run. The (RunID, Attempt) pair is the fencing token for
// every subsequent operation on the claim; Failures is the run's retry
// budget consumption. The two are deliberately separate counters: Attempt
// increments on every claim (fencing must be monotonic across the run's
// whole lineage, including park/resume cycles), while Failures counts only
// executions that actually failed — a voluntary Release back onto the queue
// or a lease that expired under its worker. Settling and re-enqueueing (how
// parked runs wait) never touches Failures, so waiting is free.
type Item struct {
	RunID    string
	Attempt  int
	Failures int
}

// Queue is anchor's delivery primitive. Implementations must satisfy the
// conformance suite in queuetest.
type Queue interface {
	// Enqueue makes runID claimable after delay (0 = immediately).
	// Idempotent: enqueueing a run that is already queued or claimed is a
	// no-op, so duplicate enqueues (janitor, resubmission) are harmless.
	Enqueue(ctx context.Context, runID string, delay time.Duration) error

	// Claim blocks until a run is claimable or ctx is done, then hands it
	// to exactly one caller with a lease of ttl. The returned Item carries
	// the incremented attempt and the run's accumulated failure count; the
	// time is the lease deadline. Expiry is queue-internal: a lapsed lease
	// makes the run claimable again, and reclaiming it increments both the
	// attempt and — because the previous execution died — Failures.
	Claim(ctx context.Context, ttl time.Duration) (Item, time.Time, error)

	// Heartbeat extends the lease by ttl iff it still holds. A stale
	// attempt gets run.ErrSuperseded — how a zombie learns it lost.
	Heartbeat(ctx context.Context, it Item, ttl time.Duration) (time.Time, error)

	// Release gives the run back voluntarily, claimable again after delay.
	// Fenced by it.Attempt. failed says whether this execution consumed
	// retry budget: true after a retryable error (Failures increments),
	// false when the worker is merely interrupted (graceful shutdown) —
	// interruption is not a failure of the code, and deploys must not burn
	// the budget of long-running work.
	Release(ctx context.Context, it Item, delay time.Duration, failed bool) error

	// Settle removes the run from the queue entirely: after a terminal
	// Complete, or when a run parks. Fenced by it.Attempt.
	Settle(ctx context.Context, it Item) error
}
