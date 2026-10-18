//go:build integration

package redisqueue_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/urmzd/anchor/adapters/redis/redisqueue"
	"github.com/urmzd/anchor/pkg/queue"
	"github.com/urmzd/anchor/pkg/queue/queuetest"
)

// nsCounter disambiguates namespaces created within the same nanosecond.
var nsCounter atomic.Int64

// newQueue returns a Queue in a fresh namespace against the Redis at
// ANCHOR_REDIS_ADDR (default localhost:6379), with cleanup that deletes
// only that namespace's keys — no FlushDB, so parallel suites coexist.
func newQueue(t *testing.T) *redisqueue.Queue {
	t.Helper()

	addr := os.Getenv("ANCHOR_REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("redis at %s not reachable (set ANCHOR_REDIS_ADDR or start docker compose redis): %v", addr, err)
	}

	ns := fmt.Sprintf("test-%d-%d", time.Now().UnixNano(), nsCounter.Add(1))
	t.Cleanup(func() {
		cleanupNamespace(t, client, ns)
		if err := client.Close(); err != nil {
			t.Errorf("close client: %v", err)
		}
	})
	return redisqueue.New(client, ns, redisqueue.WithPollInterval(5*time.Millisecond))
}

// cleanupNamespace SCANs and deletes du:{ns}:* — namespace-scoped, never
// FlushDB, so unrelated keys in the same database survive.
func cleanupNamespace(t *testing.T, client redis.UniversalClient, ns string) {
	t.Helper()
	ctx := context.Background()
	pattern := fmt.Sprintf("du:{%s}:*", ns)
	var cursor uint64
	for {
		keys, next, err := client.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			t.Errorf("scan %q: %v", pattern, err)
			return
		}
		if len(keys) > 0 {
			if err := client.Del(ctx, keys...).Err(); err != nil {
				t.Errorf("del %v: %v", keys, err)
			}
		}
		cursor = next
		if cursor == 0 {
			return
		}
	}
}

// TestConformanceQueue runs the full Queue contract suite, including the
// fenced-append subtest (the backend implements DeltaLog too).
func TestConformanceQueue(t *testing.T) {
	queuetest.Run(t, func(t *testing.T) queue.Queue {
		return newQueue(t)
	})
}

// TestConformanceDeltaLog runs the DeltaLog contract suite.
func TestConformanceDeltaLog(t *testing.T) {
	queuetest.RunDeltaLog(t, func(t *testing.T) queue.DeltaLog {
		return newQueue(t)
	})
}

// TestNoDoubleClaim races two workers over 100 rapid enqueue/claim cycles in
// one namespace: every run must be claimed exactly once, always at attempt 1.
func TestNoDoubleClaim(t *testing.T) {
	q := newQueue(t)
	ctx := context.Background()

	const cycles = 100
	for i := range cycles {
		if err := q.Enqueue(ctx, fmt.Sprintf("run-%03d", i), 0); err != nil {
			t.Fatalf("Enqueue cycle %d: %v", i, err)
		}
	}

	var (
		mu      sync.Mutex
		claimed = make(map[string]int, cycles)
	)
	var wg sync.WaitGroup
	for w := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				// A worker that finds nothing for 500ms is done: the
				// other worker drained the rest.
				cctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
				it, _, err := q.Claim(cctx, time.Minute)
				cancel()
				if err != nil {
					return
				}
				if it.Attempt != 1 {
					t.Errorf("worker %d claimed %q at attempt %d, want 1", w, it.RunID, it.Attempt)
				}
				mu.Lock()
				claimed[it.RunID]++
				mu.Unlock()
				if err := q.Settle(ctx, it); err != nil {
					t.Errorf("worker %d settle %q: %v", w, it.RunID, err)
				}
			}
		}()
	}
	wg.Wait()

	if len(claimed) != cycles {
		t.Fatalf("claimed %d distinct runs, want %d", len(claimed), cycles)
	}
	for runID, n := range claimed {
		if n != 1 {
			t.Errorf("%q claimed %d times, want exactly once", runID, n)
		}
	}
}

// TestSettleThenEnqueueLineage proves the attempt hash survives Settle: a
// settled run re-enqueued later claims at attempt 2, never a reset 1.
func TestSettleThenEnqueueLineage(t *testing.T) {
	q := newQueue(t)
	ctx := context.Background()

	if err := q.Enqueue(ctx, "r1", 0); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	first, _, err := q.Claim(cctx, time.Minute)
	cancel()
	if err != nil {
		t.Fatalf("first Claim: %v", err)
	}
	if first.RunID != "r1" || first.Attempt != 1 {
		t.Fatalf("first Claim = %+v, want {r1 1}", first)
	}
	if err := q.Settle(ctx, first); err != nil {
		t.Fatalf("Settle: %v", err)
	}

	if err := q.Enqueue(ctx, "r1", 0); err != nil {
		t.Fatalf("re-Enqueue: %v", err)
	}
	cctx, cancel = context.WithTimeout(ctx, 5*time.Second)
	second, _, err := q.Claim(cctx, time.Minute)
	cancel()
	if err != nil {
		t.Fatalf("second Claim: %v", err)
	}
	if second.RunID != "r1" || second.Attempt != 2 {
		t.Fatalf("second Claim = %+v, want {r1 2} (attempt hash must survive Settle)", second)
	}
}
