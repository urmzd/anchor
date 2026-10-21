# anchor

Beta durable-execution framework for existing Go code: wrap functions with `Activity`/`Step`/`Event`, run a worker, and code survives crashes by replaying from the ledger. Go module `github.com/urmzd/anchor`; a library, whose one binary is `anchor`, a read-only ledger inspector in `adapters/postgres/cmd/anchor`.

## Architecture

Three concepts only: ledgers (durable truth), queues (flow and clock), workers (a pull loop, no inbound surface). Packages form a strict one-way DAG:

- `pkg/run` is the leaf: runs, records, deltas, sentinel errors, codec, type-erased registry. Stdlib only, imports nothing else in the repo.
- `pkg/ledger` and `pkg/queue` define the two interfaces, each with a complete single-process `Memory` implementation; their conformance suites (`pkg/ledger/ledgertest`, `pkg/queue/queuetest`) are the executable contracts.
- `pkg/replay` is the correctness heart: `Frame.Do` memoizes re-execution (spec in `docs/design/replay.md`).
- `pkg/worker` is the consumer loop: claim, replay to frontier, exactly one outcome per claim (persisted on a cancellation-detached context so graceful shutdown cannot discard it), plus the janitor repair loop that paginates the entire pending set.
- The root package is the SDK (`Activity`/`Step`/`Event`/`Emit`, `Client`, `Start`/`Exec`). It imports only `pkg/*` interfaces and `pkg/replay`, never `pkg/worker` and never adapters.
- `adapters/postgres` (pgx) and `adapters/redis` (go-redis) are separate `go.mod` modules, imported only by `examples/` and user code. The root module has zero third-party dependencies.
- `adapters/postgres/pgkit` is the stack's shared Postgres plumbing, so the layers above stop hand-rolling it: `Apply(ctx, db, name, steps...)` runs idempotent steps in one transaction under an advisory lock derived from `name`, and `Listener` carries LISTEN/NOTIFY for a process over one dedicated connection (per-channel subscriptions on Go channels, reconnect with backoff, a `Resync` message whenever notifications may have been missed). `pgqueue`'s delta log listens through it (`pgqueue.WithListener` shares one).
- `adapters/postgres/cmd/anchor` is the read-only ledger inspector (`runs`, `show`, `replay`), in the adapter module so the root keeps zero dependencies. `replay` plays a run's recorded calls into a `replay.Frame` whose ledger refuses every write, to report the frontier without the workflow code. It reads through `pgledger` (`ListRuns` is an adapter method for it, not a ledger capability) after `Validate`, over connections opened with `default_transaction_read_only` on.
- Generics stop at the root: `pkg/run.Registry` carries type-erased `run.WorkflowFn` (bytes in, bytes out), so `pkg/worker` never sees a type parameter.

Full guide: `docs/architecture/overview.md`. Discover layout with `tree` or ripgrep; do not trust stale listings.

## Commands

| Action | Command |
|--------|---------|
| init | `make init` |
| build | `make build` (every module) |
| test | `make test` (every module; root alone: `go test ./...`) |
| lint | `make lint` (golangci-lint per module) |
| fmt | `make fmt` |
| quality gate | `make check` |
| integration | `make test-integration` (docker compose + `-tags=integration`); against another Postgres: `ANCHOR_POSTGRES_URL=postgres://... go test -tags=integration ./...` in `adapters/postgres` |
| demo | `make demo` (compose up + quickstart example) |
| inspect a ledger | `cd adapters/postgres && go run ./cmd/anchor replay RUN` (URL from `-db` or `ANCHOR_DATABASE_URL`; also `runs`, `show`, `-json`) |
| one module | `cd adapters/postgres && go test ./...` (same for `adapters/redis`, `examples/*`) |

## Code Style

- Idiomatic Go, stdlib-first; the root module has zero third-party dependencies and must stay that way. Adapters keep their dependencies to themselves.
- Errors: wrap with `fmt.Errorf("pkg: context: %w", err)`; the shared sentinels live in `pkg/run` (`ErrSuperseded`, `ErrAlreadyRecorded`, `ErrNonDeterministic`, `ErrParked`, ...) for callers to `errors.Is` on.
- Interfaces stay small and live with their concern; implementations may depend on interfaces, never the reverse.
- Package doc comments explain the concern and its boundary.
- Conventional commits (feat/fix/chore/...); sr cuts releases from them.

## Rules

- Preserve the dependency DAG: nothing under `pkg/` imports the root package; the root never imports `pkg/worker` or adapters; `pkg/run` stays stdlib-only.
- Adapters VALIDATE, never migrate. The no-owned-schema promise is a product invariant: no adapter code path executes DDL against a user's database on its own. `RecommendedDDL` returns a string suggestion, never runs it. `pgkit.Apply` runs only the statements its caller passes, when its caller calls it; `pgledger` and `pgqueue` never call it.
- One Postgres plumbing implementation: advisory-locked migrations go through `pgkit.Apply` and LISTEN goes through `pgkit.Listener`, never a private connection loop.
- Recommended DDL keeps run IDs and record keys `COLLATE "C"`: they are opaque bytes, ordered (the janitor's `(created_at, run_id)` cursor) and prefix-scanned exactly as Go compares strings. `Validate` accepts any collation.
- Never widen `Ledger` or `Queue` for one backend's needs: optional powers are capability interfaces discovered by type assertion. `queue.DeltaLog`, `ledger.RunGetter` and `ledger.PrefixReader` are the templates.
- Conformance suites are the contract: every new backend must pass `ledgertest.Run` / `queuetest.Run` from its own tests.
- Attempt lineage must survive `Settle`: a settled-and-re-enqueued run (a parked run resuming) claims at attempt N+1, never a reset, or a zombie holding the old attempt would pass the fence.
- Attempt and Failures are separate counters and must stay that way: Attempt is the fencing token (increments on every claim), Failures is the retry budget (moves only on `Release(..., failed=true)` or an expiry reclaim). Settle-and-re-enqueue never touches Failures; waiting is free, and `Run.MaxAttempts` bounds failures, never claims.
- Record identity is load-bearing: keys are `name#occurrence`, scoped under the executing parent's key for nested calls (activities are the ABI). A breaking input/output change is a new name, never a mutation of what an existing name means.
- The `anchor` command only reads: keep its connections read-only, its replay frame's ledger write-refusing, and the database URL out of every output (errors and `-h` included). A command that writes or repairs is a design decision, not an addition.
- This is a beta: keep the surface small, document limitations honestly (README beta note, overview roadmap) rather than papering over them.

## Releases

sr versions the root and adapters together per `sr.yaml`, cutting one `vX.Y.Z` tag (no per-module `adapters/...` tags, so external consumers of the adapter modules resolve pseudo-versions). The adapters' `go.mod` files pin the released root version and keep `replace github.com/urmzd/anchor => ../..` for in-repo development; downstream consumers ignore the replace and resolve the pinned require. Bump the pin when a release changes the core contracts.

## Extension Guide

- **New ledger backend**: implement the five `ledger.Ledger` methods (`Accept`, `Complete`, `Load`, `Record`, and the cursor-paginated `ListPending(ctx, before time.Time, cursor run.PendingCursor, limit int)`) in its own module under `adapters/`, and pass `ledgertest.Run`. Also implement the optional `ledger.RunGetter` capability (`GetRun`, a point read without records) so `Handle.Result` polling avoids `Load`'s full record prefetch, and `ledger.PrefixReader` (`RecordsWithPrefix`, a run's records by literal key prefix), which `ledgertest.Run` exercises whenever a ledger implements it. Adapter package code must still build against the root version its `go.mod` pins: assert a capability newer than that pin in tests, not in package code.
- **New queue backend**: implement `queue.Queue` and pass `queuetest.Run`. Claims return `queue.Item{RunID, Attempt, Failures}`: increment Attempt on every claim, increment Failures only on `Release(ctx, it, delay, failed=true)` and on expiry reclaims, never on Settle-and-re-enqueue. Optionally add the `queue.DeltaLog` capability (exercised by `queuetest.RunDeltaLog`, plus the fenced-append subtest that `Run` adds for backends implementing both).
- **Custom codec**: implement `run.Codec` (`Marshal`/`Unmarshal`/`ContentType`); install with `anchor.WithCodec` on the client and `worker.WithCodec` on the worker. They must agree: each record stores the `ContentType()` that produced it, and replay verifies it on every memo hit, failing the run terminally on a mismatch.
- **Forking, child runs, timers**: these land as client-side compositions, not engine features. `run.Run.ParentRunID` and record-key namespaces are the reserved seams; do not add framework surface for them ahead of that design.
