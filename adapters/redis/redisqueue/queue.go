package redisqueue

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/urmzd/anchor/pkg/queue"
	"github.com/urmzd/anchor/pkg/run"
)

// Enqueue implements queue.Queue. A run already queued or leased is left
// untouched, so duplicate enqueues are harmless; otherwise it becomes
// claimable at now+delay. The attempt counter, if any, is untouched — a
// settled run re-enqueued here continues its lineage.
func (q *Queue) Enqueue(ctx context.Context, runID string, delay time.Duration) error {
	readyAt := time.Now().Add(delay).UnixMilli()
	err := scriptEnqueue.Run(ctx, q.client,
		[]string{q.keyReady, q.keyLeased},
		runID, readyAt,
	).Err()
	if err != nil {
		return fmt.Errorf("redisqueue: enqueue %q: %w", runID, err)
	}
	return nil
}

// Claim implements queue.Queue. It polls the claim script every
// PollInterval (plus jitter, so a fleet of workers doesn't thunder) until a
// run is claimable or ctx is done. The script atomically moves the winner —
// a due ready run, or an expired lease — to leased and increments its
// attempt, so exactly one caller wins each run. An expiry reclaim also
// increments the failure counter: the previous execution died holding the
// lease, which consumes retry budget.
func (q *Queue) Claim(ctx context.Context, ttl time.Duration) (queue.Item, time.Time, error) {
	for {
		if err := ctx.Err(); err != nil {
			return queue.Item{}, time.Time{}, err
		}

		now := time.Now()
		res, err := scriptClaim.Run(ctx, q.client,
			[]string{q.keyReady, q.keyLeased, q.keyAttempt, q.keyFailures},
			now.UnixMilli(), ceilMillis(ttl),
		).Result()
		switch {
		case errors.Is(err, redis.Nil):
			// Nothing claimable; fall through to the poll sleep.
		case err != nil:
			return queue.Item{}, time.Time{}, fmt.Errorf("redisqueue: claim: %w", err)
		default:
			it, err := parseClaimReply(res)
			if err != nil {
				return queue.Item{}, time.Time{}, fmt.Errorf("redisqueue: claim: %w", err)
			}
			// The returned deadline is computed from the Go clock that
			// produced the script's now-argument, so it is exact where the
			// stored score is millisecond-truncated.
			return it, now.Add(ttl), nil
		}

		timer := time.NewTimer(q.pollDelay())
		select {
		case <-ctx.Done():
			timer.Stop()
			return queue.Item{}, time.Time{}, ctx.Err()
		case <-timer.C:
		}
	}
}

// Heartbeat implements queue.Queue. Fenced: the lease must be live and held
// by it.Attempt, or the caller learns it lost via run.ErrSuperseded.
func (q *Queue) Heartbeat(ctx context.Context, it queue.Item, ttl time.Duration) (time.Time, error) {
	now := time.Now()
	res, err := scriptHeartbeat.Run(ctx, q.client,
		[]string{q.keyLeased, q.keyAttempt},
		it.RunID, it.Attempt, now.UnixMilli(), ceilMillis(ttl),
	).Int64()
	if err != nil {
		return time.Time{}, fmt.Errorf("redisqueue: heartbeat %q attempt %d: %w", it.RunID, it.Attempt, err)
	}
	if res == supersededReply {
		return time.Time{}, wrapSuperseded("heartbeat", it.RunID, it.Attempt)
	}
	return now.Add(ttl), nil
}

// Release implements queue.Queue. Fenced like Heartbeat; the run goes back
// to ready, claimable at now+delay. failed says whether this execution
// consumed retry budget: true after a retryable error (the failure counter
// increments, inside the same fenced script), false when the worker was
// merely interrupted — interruption is not a failure of the code.
func (q *Queue) Release(ctx context.Context, it queue.Item, delay time.Duration, failed bool) error {
	failedArg := 0
	if failed {
		failedArg = 1
	}
	res, err := scriptRelease.Run(ctx, q.client,
		[]string{q.keyReady, q.keyLeased, q.keyAttempt, q.keyFailures},
		it.RunID, it.Attempt, time.Now().UnixMilli(), ceilMillis(delay), failedArg,
	).Int64()
	if err != nil {
		return fmt.Errorf("redisqueue: release %q attempt %d: %w", it.RunID, it.Attempt, err)
	}
	if res == supersededReply {
		return wrapSuperseded("release", it.RunID, it.Attempt)
	}
	return nil
}

// Settle implements queue.Queue. Fenced on the attempt counter and presence
// in leased (an expired-but-unreclaimed lease may still settle). Both hash
// fields survive: a later Enqueue continues the lineage at N+1 with the
// accumulated failure count intact — park/resume is free but forgives
// nothing.
func (q *Queue) Settle(ctx context.Context, it queue.Item) error {
	res, err := scriptSettle.Run(ctx, q.client,
		[]string{q.keyReady, q.keyLeased, q.keyAttempt},
		it.RunID, it.Attempt,
	).Int64()
	if err != nil {
		return fmt.Errorf("redisqueue: settle %q attempt %d: %w", it.RunID, it.Attempt, err)
	}
	if res == supersededReply {
		return wrapSuperseded("settle", it.RunID, it.Attempt)
	}
	return nil
}

// wrapSuperseded is the shared failure for every fenced mutation whose
// script reported supersededReply; it wraps run.ErrSuperseded for errors.Is.
func wrapSuperseded(op, runID string, attempt int) error {
	return fmt.Errorf("redisqueue: %s %q attempt %d: %w", op, runID, attempt, run.ErrSuperseded)
}

// pollDelay is one Claim/Watch poll interval with up to 25% jitter.
func (q *Queue) pollDelay() time.Duration {
	jitter := q.pollInterval / 4
	if jitter <= 0 {
		return q.pollInterval
	}
	return q.pollInterval + rand.N(jitter)
}

// parseClaimReply decodes the claim script's {runID, attempt, failures,
// deadline} table. The deadline element is ignored in favor of the caller's
// exact Go-clock computation.
func parseClaimReply(res any) (queue.Item, error) {
	arr, ok := res.([]any)
	if !ok || len(arr) != 4 {
		return queue.Item{}, fmt.Errorf("unexpected script reply %T %v", res, res)
	}
	runID, ok := arr[0].(string)
	if !ok {
		return queue.Item{}, fmt.Errorf("unexpected runID element %T", arr[0])
	}
	attempt, ok := arr[1].(int64)
	if !ok {
		return queue.Item{}, fmt.Errorf("unexpected attempt element %T", arr[1])
	}
	failures, ok := arr[2].(int64)
	if !ok {
		return queue.Item{}, fmt.Errorf("unexpected failures element %T", arr[2])
	}
	return queue.Item{RunID: runID, Attempt: int(attempt), Failures: int(failures)}, nil
}
