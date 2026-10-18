// Package redisqueue is anchor's Redis queue backend: a sorted-set lease
// queue implementing queue.Queue, plus the queue.DeltaLog capability on
// Redis Streams.
//
// # Flow, not truth
//
// Redis here is disposable. The queue carries only run IDs and lease clocks,
// and every key is derivable from the ledger: the janitor can re-enqueue any
// pending run it finds there. Wiping Redis loses observability history (the
// delta streams) and at most one lease-TTL of delivery latency — never runs,
// never results. Correctness lives in the ledger's first-write-wins records;
// this package only moves IDs under fenced, time-bounded claims.
//
// # Layout
//
// All keys share one cluster slot via a hash tag on the namespace,
// prefix du:{<namespace>}:
//
//	du:{ns}:ready          ZSET  member=runID  score=readyAt unix-millis
//	du:{ns}:leased         ZSET  member=runID  score=lease deadline unix-millis
//	du:{ns}:attempt        HASH  field=runID   value=attempt counter
//	du:{ns}:failures       HASH  field=runID   value=failure counter
//	du:{ns}:deltas:<runID> STREAM entries {key, attempt, kind, payload}
//
// Neither hash is ever deleted on Settle. The attempt field is the fence:
// lineage survives settling, so a parked run that is settled and re-enqueued
// claims at attempt N+1, never 1 — a reset would let a zombie of the settled
// attempt pass the fence. The failures field is the retry budget: it moves
// only on a failed Release or an expiry reclaim, so park/resume cycles and
// interruptions are free but never forgive past failures. Retention is the
// caller's: delete the namespace.
//
// The work queue is deliberately NOT a Stream: Stream entries are immutable
// and XAUTOCLAIM's delivery counter fights the fenced attempt model. Streams
// are exactly right for the per-run delta log, where entries are immutable
// history.
//
// Every queue mutation is one atomic Lua script taking the current time as
// an argument (computed once in Go per call — scripts never call TIME, so
// they stay deterministic for replication).
package redisqueue

import (
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/urmzd/anchor/pkg/queue"
)

// defaultPollInterval is the Claim/Watch poll cadence when no option is set.
const defaultPollInterval = 100 * time.Millisecond

// Queue is a Redis-backed queue.Queue and queue.DeltaLog. All instances
// sharing a client and namespace see the same queue.
type Queue struct {
	client       redis.UniversalClient
	pollInterval time.Duration

	keyReady    string // ZSET: claimable runs, scored by readyAt millis
	keyLeased   string // ZSET: leased runs, scored by lease deadline millis
	keyAttempt  string // HASH: runID → attempt counter (never deleted on settle)
	keyFailures string // HASH: runID → failure counter (never deleted on settle)
	deltaPrefix string // stream key prefix; append runID
}

var (
	_ queue.Queue    = (*Queue)(nil)
	_ queue.DeltaLog = (*Queue)(nil)
)

// Option configures a Queue.
type Option func(*Queue)

// WithPollInterval sets the cadence at which blocked Claim and Watch callers
// re-check Redis. Defaults to 100ms; tests use a few milliseconds.
func WithPollInterval(d time.Duration) Option {
	return func(q *Queue) {
		if d > 0 {
			q.pollInterval = d
		}
	}
}

// New returns a Queue over client, with every key namespaced under
// du:{namespace}: — the hash tag keeps all of a namespace's keys in one
// cluster slot so the Lua scripts stay single-slot.
//
// The namespace must be non-empty and contain no '{' or '}'. It is embedded
// verbatim in the hash tag, and per the Redis Cluster spec an empty tag
// ("{}") means the whole key is hashed — the key families would land in
// different slots and every multi-key script would fail with CROSSSLOT;
// braces inside the namespace would truncate the tag the same way. A
// violation panics: a mis-configured constructor is a deterministic
// programmer error, same policy as the registry's duplicate-name panic.
func New(client redis.UniversalClient, namespace string, opts ...Option) *Queue {
	if namespace == "" || strings.ContainsAny(namespace, "{}") {
		panic(fmt.Sprintf(
			"redisqueue: invalid namespace %q: must be non-empty and contain no '{' or '}' (it forms the du:{namespace}: cluster hash tag; violating it splits keys across slots and breaks the multi-key Lua scripts with CROSSSLOT)",
			namespace,
		))
	}
	prefix := "du:{" + namespace + "}:"
	q := &Queue{
		client:       client,
		pollInterval: defaultPollInterval,
		keyReady:     prefix + "ready",
		keyLeased:    prefix + "leased",
		keyAttempt:   prefix + "attempt",
		keyFailures:  prefix + "failures",
		deltaPrefix:  prefix + "deltas:",
	}
	for _, opt := range opts {
		opt(q)
	}
	return q
}

// deltaKey is the stream key holding runID's delta log.
func (q *Queue) deltaKey(runID string) string {
	return q.deltaPrefix + runID
}

// ceilMillis converts d to whole milliseconds, rounding up so no positive
// duration collapses to zero and a lease always covers its full TTL.
func ceilMillis(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return int64((d + time.Millisecond - 1) / time.Millisecond)
}
