# Architecture Overview

anchor makes existing Go code durable without rewrites. Users wrap functions, run a worker, and their code survives crashes: on retry the workflow re-executes from the top, recorded calls return instantly from the ledger, and the first unrecorded call executes for real. This document describes the beta architecture; interfaces are stabilizing but pre-1.0.

## Three Concepts

1. **Ledger** (durable truth): runs and their memoized records, stored in tables the caller already owns. `pkg/ledger.Ledger` is a pure interface: an idempotent keyed insert (`Accept`), a conditional pending-to-terminal update (`Complete`), a prefetching read (`Load`), a write-once record insert (`Record`), and a cursor-paginated pending scan (`ListPending`). Any store that can do those five things qualifies. A point read of one run (`GetRun`) is the optional `ledger.RunGetter` capability, which result polling prefers over `Load`'s full record prefetch.
2. **Queue** (flow and clock): delivery of run IDs under fenced, time-bounded claims. `pkg/queue.Queue` is `Enqueue`, `Claim`, `Heartbeat`, `Release`, `Settle`. Every claim carries two separate counters: `Attempt`, the fencing token that increments on every claim, and `Failures`, the retry budget that moves only when an execution genuinely fails (a `Release` with `failed=true`, or a lease that expired under its worker). Waiting is free: park/resume cycles never touch the budget. The queue carries only IDs; data lives in the ledger, so every queue backend is disposable and rebuildable from ledger truth.
3. **Worker** (a pull loop, no inbound surface): claims runs, re-executes them under replay to the frontier, persists exactly one outcome per claim. It exposes no listener; every arrow points outward, to the ledger for truth and the queue for flow. It can be the submitting process itself (a goroutine) or a separate fleet; workers coordinate through nothing but the two interfaces. Graceful shutdown is an interruption, not a crash: outcomes are written on a cancellation-detached context, and an interrupted run is released immediately without burning retry budget.

There is no owned schema, no server, and no third-party dependency in the root module.

## Package Graph

The packages form a strict one-way DAG:

- `pkg/run` is the leaf: runs, records, deltas, sentinel errors, the codec, the type-erased registry. Stdlib only, imported by every other package, imports none of them.
- `pkg/ledger` and `pkg/queue` define the two interfaces, each with a complete single-process `Memory` implementation (the queue's also implements `DeltaLog`). Their conformance suites, `pkg/ledger/ledgertest` and `pkg/queue/queuetest`, are the executable contracts every backend must pass.
- `pkg/replay` is the correctness heart: the `Frame` that memoizes re-execution (see `docs/design/replay.md`).
- `pkg/worker` is the consumer loop over ledger, queue, and replay.
- The root package (`anchor`) is the user-facing SDK: `Activity`, `Step`, `Event`, `Emit`, `Client`, `Start`/`Exec`. It imports only the `pkg/*` interfaces and replay, never `pkg/worker` and never an adapter.
- Generics stop at the root: `ActivityFn[I, O]` compiles down to a type-erased `run.WorkflowFn` (bytes in, bytes out) in `run.Registry`, so `pkg/worker` never sees a type parameter.

Nothing under `pkg/` imports the root package, and adapters are invisible to all of it.

## Module Layout

Every `go.mod` in the tree is its own module:

- root (`github.com/urmzd/anchor`): the SDK and `pkg/*`, zero third-party dependencies.
- `adapters/postgres` (pgx) and `adapters/redis` (go-redis): separate modules so their dependencies never touch the root. They are imported only by user code and `examples/`.
- `examples/*`: separate modules wiring root + adapters together; the only code in the repo that imports adapters.

## The Two Adapters

**adapters/postgres** maps the ledger onto existing user tables via a declarative mapping (`pgledger`). `Validate` introspects the live schema and errors with guidance when a column is missing or mistyped; `RecommendedDDL` returns suggested DDL as a string and never executes it. Adapters validate, never migrate: anchor runs no DDL, ever. Two contract details: mapped identifiers are lower_snake_case only, and the uniqueness the ledger depends on (the runs table's run-ID column, the records table's run-ID and key pair) must be a plain unique constraint or unique index: non-partial, non-expression, non-deferrable. Partial and expression unique indexes are not accepted, because the adapter's `ON CONFLICT` inference cannot use them; `Validate` rejects them rather than letting first-write-wins silently degrade. The same module ships `pgqueue`, a Postgres queue built on `FOR UPDATE SKIP LOCKED`, so a Postgres-only deployment (ledger and queue in one database) is fully supported.

**adapters/redis** (`redisqueue`) is a throughput upgrade, not a requirement: a sorted-set lease queue (ready and leased ZSETs plus counter hashes, every mutation one atomic Lua script) implementing `queue.Queue`, plus the `DeltaLog` capability on Redis Streams. All keys share one cluster slot via a hash tag on the namespace, so the namespace must be non-empty and must contain no braces; the constructor rejects anything else, because a malformed hash tag would silently split a namespace's keys across cluster slots and break the atomic scripts. Redis is disposable by design: wiping it loses delta history and at most one lease of delivery latency, never runs or results; the janitor rebuilds the queue from `ListPending`, paginating the entire pending set.

## Capabilities, Not Interface Growth

Optional powers are capability interfaces discovered by type assertion, never widenings of `Ledger` or `Queue`. Two examples set the pattern:

- `queue.DeltaLog`: `queue.Memory` and `redisqueue.Queue` implement it, and a backend without it simply makes `Emit` a no-op and skips dividers. The worker discovers it once, at construction; the client asserts per call inside `Client.Watch` and returns an error when the queue lacks it.
- `ledger.RunGetter`: a point read of one run (`GetRun`) without its records. `Handle.Result` polls with it when present and falls back to `Load` (which prefetches the full record history on every poll) when not. Implement it on custom ledgers.

## Deltas and Dividers

Activities can `Emit` streaming deltas (progress, token streams). Deltas persist on the queue side, in the `DeltaLog`, because they are flow, not truth: losing them loses observability history, never correctness. The framework writes attempt dividers (`DeltaAttempt`) at each claim and seal dividers (`DeltaSeal`) at each checkpoint, so consumers can split superseded partial output from live tail and sealed history. Checkpointed activities never re-stream: replay hits the memo and user code never runs. `Client.Watch` tails a run's stream; `PriorDeltas` exposes earlier attempts' output for provider-side resume. The consumer algorithm is specified in `docs/design/replay.md`.

## Wait as Absence

`Event[T](ctx, name)` is not a suspend mechanism; an event is simply a record that does not exist yet. Present: the payload returns instantly. Absent: `run.ErrParked` propagates, the worker settles the queue item, and the run stays pending at zero cost, with no retry budget spent no matter how long or how often it waits. Resume is one record write plus one enqueue, from anywhere: `Client.Signal` is the sugar, a row in your own table plus an enqueue is equivalent, and the janitor is the backstop. Call `Event` from the workflow body, not inside a nested activity: record keys are parent-scoped and `Signal` targets top-level `event:<name>#<k>` keys. Human-in-the-loop approval flows need no framework surface at all.

## Versioning Stance

Record identity is `name#occurrence`: keys follow the activity name, not the call site, and a call made inside another wrapped call is scoped under its parent's key (`process-order#0/charge-payment#0`), so a memoized parent cannot desynchronize its siblings' occurrence counters. Code rolls forward across releases with no version, patch, or pinning API; checkpoints stay valid when other, differently-named calls are inserted, removed, or reordered; detected divergence fails loudly with `run.ErrNonDeterministic` (terminal, no retry burn). Every record also stores the codec content type that produced it, and replay verifies it on each memo hit, failing terminally on mismatch. A breaking input/output change is a rename (`charge-payment.v2`). See `docs/design/replay.md` for the full rules.

## Adoption Is Monotone

Outside a run, every wrapped function is a plain Go call: `Activity` and `Step` pass straight through to the wrapped function, `Emit` is a no-op. Tests, scripts, and codebases mid-migration keep working unchanged; durability is added by running the code under a worker, not by changing the code.

## Roadmap

- Distributed activities (activities executed by a different worker than the run's)
- Forking and child runs as client-side compositions (`ParentRunID` and record-key namespaces are the reserved seams)
- Durable timers
- Multi-goroutine workflow bodies (v1 workflow bodies are single-goroutine; the frame is mutex-guarded so violations surface as loud determinism errors, not corruption)
- Wrapping codecs (encryption, claim-check) behind `run.Codec`
