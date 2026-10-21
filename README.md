<p align="center">
  <h1 align="center">anchor</h1>
  <p align="center">
    Durable execution for existing Go code: wrap your functions, run a worker, done.
    <br /><br />
    <a href="https://github.com/urmzd/anchor/issues">Report Bug</a>
    &middot;
    <a href="https://pkg.go.dev/github.com/urmzd/anchor">Go Docs</a>
  </p>
</p>

<p align="center">
  <a href="https://github.com/urmzd/anchor/actions/workflows/ci.yml"><img src="https://github.com/urmzd/anchor/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  &nbsp;
  <a href="https://pkg.go.dev/github.com/urmzd/anchor"><img src="https://pkg.go.dev/badge/github.com/urmzd/anchor.svg" alt="Go Reference"></a>
  &nbsp;
  <a href="LICENSE"><img src="https://img.shields.io/github/license/urmzd/anchor" alt="License"></a>
</p>

> **Beta.** anchor is pre-1.0 and under active development. Interfaces are stabilizing but may change between minor versions, and the Postgres and Redis adapters are young. Crash recovery and graceful shutdown are correct today: a cancelled worker persists in-flight outcomes and hands interrupted runs back without burning retry budget. Workflow bodies are single-goroutine in v1; distributed activities, forking and child runs, and durable timers are on the roadmap.

## Features

- **Wrap, don't rewrite**: `Activity`, `Step`, and `Event` wrap the code you have. Outside a run every wrapped function is a plain Go call, so tests, scripts, and codebases mid-migration keep working unchanged. Adoption is monotone.
- **Interfaces first, no owned schema**: `pkg/ledger.Ledger` and `pkg/queue.Queue` are pure interfaces, the root module has zero third-party dependencies, and the in-memory implementations are complete single-process systems.
- **Your tables, validated, never migrated**: the Postgres adapter maps onto tables you already own via a declarative `Mapping`. `Validate` introspects the live schema and errors with guidance; `RecommendedDDL` returns suggested DDL and never executes it (hand it to `pgkit.Apply` if you want it applied at startup).
- **Postgres-only works**: `pgqueue` (`FOR UPDATE SKIP LOCKED`) runs the queue in the same database, and `pgqueue.NewDeltaQueue` adds streaming on an append-only delta table woken by `LISTEN/NOTIFY`. Redis (`redisqueue`: a ZSET lease queue plus a Redis Streams delta log) is a throughput upgrade, not a requirement, and is disposable: the janitor rebuilds it from the ledger.
- **Shared Postgres plumbing**: `pgkit`, in the same module, is what the rest of the stack uses instead of hand-rolling it: `pgkit.Apply` runs schema statements once per database under an advisory lock, and `pgkit.Listener` carries a process's `LISTEN/NOTIFY` over one connection.
- **Activities are the ABI**: record identity is `name#occurrence` (nested calls scoped under their parent's key), so code rolls forward across releases with no version, patch, or pinning APIs; checkpoints stay valid when other, differently-named calls are inserted, removed, or reordered; divergence fails loudly (`ErrNonDeterministic`); a breaking input/output change is a rename (`charge-payment.v2`).
- **Fenced attempts, budgeted failures**: `(runID, attempt)` fences every claim, with the attempt owned by the queue and incremented at every claim across the run's whole lineage; retry budget is a separate failure counter that moves only when an execution genuinely fails, so waiting and shutdown interruptions are free. The first terminal write wins; heartbeats never touch the ledger. The happy path costs two run-level ledger writes plus one per record.
- **Waiting is native**: an `Event` is a record that does not exist yet, so the run parks off the queue at zero cost. Resume is one record write plus one enqueue: `Client.Signal`, or a row in your own table with the janitor as backstop.
- **Streaming deltas**: activities `Emit` progress persisted on the queue side, split by attempt and seal dividers. Checkpointed activities never re-stream; losing deltas loses observability history, never correctness.
- **Errors that mean it**: retryable failures burn retry budget with replay-cheap retries, `NonRetryable` memoizes the failure itself and surfaces it as `*run.RecordedError` on every attempt including the first, panics are retryable, `MaxAttempts` bounds failed executions (waiting never counts, so parked runs are never poisoned), and `IdempotencyKey(ctx)` hands downstream systems the key that makes at-least-once effective-once.

## Installation

```sh
go get github.com/urmzd/anchor
```

The adapters are separate modules, so their dependencies never touch yours until you choose one:

```sh
go get github.com/urmzd/anchor/adapters/postgres
go get github.com/urmzd/anchor/adapters/redis
```

## Quick Start

You have an order pipeline (from [`examples/quickstart`](examples/quickstart/)):

```go
func process(ctx context.Context, o Order) (Receipt, error) {
	chargeID, err := charge(ctx, o)
	if err != nil {
		return Receipt{}, err
	}
	if _, err := reserve(ctx, o); err != nil {
		return Receipt{}, err
	}
	if _, err := confirm(ctx, o); err != nil {
		return Receipt{}, err
	}
	return Receipt{OrderID: o.ID, ChargeID: chargeID, Amount: o.Amount, IssuedAt: time.Now()}, nil
}
```

It works, until the process dies between the charge and the email. Wrap the pieces once, at package level:

```go
var (
	chargePayment    = anchor.Activity("charge-payment", charge)
	reserveInventory = anchor.Activity("reserve-inventory", reserve)
	sendConfirmation = anchor.Activity("send-confirmation", confirm)
	processOrder     = anchor.Activity("process-order", process)
)

func process(ctx context.Context, o Order) (Receipt, error) {
	chargeID, err := chargePayment.Call(ctx, o)
	if err != nil {
		return Receipt{}, err
	}
	if _, err := reserveInventory.Call(ctx, o); err != nil {
		return Receipt{}, err
	}
	if _, err := sendConfirmation.Call(ctx, o); err != nil {
		return Receipt{}, err
	}
	// Step records inline non-determinism: the first value is the value forever.
	issuedAt, err := anchor.Step(ctx, "issued-at",
		func(context.Context) (time.Time, error) { return time.Now(), nil })
	if err != nil {
		return Receipt{}, err
	}
	return Receipt{OrderID: o.ID, ChargeID: chargeID, Amount: o.Amount, IssuedAt: issuedAt}, nil
}
```

Then wire a ledger, a queue, and a worker. The worker can be your own process:

```go
// Ledger: your Postgres, your tables. anchor only validates, never migrates.
pool, _ := pgxpool.New(ctx, "postgres://anchor:anchor@localhost:5432/anchor")
lgr, err := pgledger.New(pool, pgledger.DefaultMapping())
if err != nil {
	log.Fatal(err)
}
if err := lgr.Validate(ctx); err != nil {
	log.Fatal(err)
}

// Queue: disposable flow. Wiping Redis loses no runs; they live in the ledger.
rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
q := redisqueue.New(rdb, "quickstart")

c := anchor.New(lgr, q)

// The worker is a pull loop, not a service: run it on a goroutine.
w := worker.New(lgr, q)
go w.Run(ctx)

receipt, err := anchor.Exec(ctx, c, processOrder,
	Order{ID: "1042", Email: "ada@example.com", Amount: 2400},
	anchor.WithRunID("order-1042")) // the run ID is the submit idempotency key
```

Now kill the worker after `charge-payment` records. The lease lapses, the next claim takes attempt N+1, and the workflow re-executes from the top: `charge-payment` returns instantly from the ledger instead of executing (no double charge), `reserve-inventory` likewise, and execution resumes for real at the first unrecorded call. The quickstart demos exactly this: it crashes its own worker mid-confirmation and lets a second worker finish the order.

```sh
docker compose up -d --wait   # Postgres 17 + Redis 8; the init hook applies the suggested schema
make demo                     # or: cd examples/quickstart && go run .
```

## Bring Your Own Tables

`DefaultMapping` targets the suggested `anchor_runs` / `anchor_records` tables, but a mapping can point at tables you already have, including tables shared with other rows:

```go
lgr, err := pgledger.New(pool, pgledger.Mapping{
	Runs: pgledger.RunsMap{
		Table:    "orders",
		RunID:    "order_ref",
		Status:   "workflow_status",
		Envelope: "workflow_state",
	},
	Records: pgledger.RecordsMap{
		Table:    "order_events",
		RunID:    "order_ref",
		Key:      "event_key",
		Envelope: "payload",
		// Scope: this table also holds non-anchor rows; see only ours.
		Scope: pgledger.Scope{Column: "source", Value: "anchor"},
		// Defaults: fill your own NOT NULL columns on insert.
		Defaults: func(rec run.Record) map[string]any {
			return map[string]any{"tenant_id": "acme"}
		},
	},
})
if err != nil {
	log.Fatal(err)
}
if err := lgr.Validate(ctx); err != nil {
	log.Fatal(err) // reports exactly what is missing; RecommendedDDL suggests the fix
}
```

Two validation rules worth knowing up front: mapped identifiers are lower_snake_case only, and the uniqueness the ledger depends on must be a plain unique index or unique constraint (the runs table on its run-ID column, the records table on its run-ID and key pair). Partial or expression unique indexes are rejected, because the adapter's `ON CONFLICT` writes cannot use them.

## Wait for a Human

Waiting is the absence of a record. An `Event` with no record parks the run: settled off the queue, still pending, costing nothing. Anything that writes the record and enqueues the run resumes it:

```go
// In the workflow: park until someone approves.
approval, err := anchor.Event[Approval](ctx, "approval")

// Anywhere else, any process: approve and wake the run.
err = c.Signal(ctx, "order-1042", "approval", Approval{By: "ada"})
```

`Signal` is sugar for "write the event record, enqueue the run". A row inserted into your own table plus an enqueue does the same, and the janitor re-enqueues parked runs as a backstop. Parking costs nothing: a run can wait days and resume with its full retry budget intact. Call `Event` from the workflow body, not inside a nested activity: record keys are parent-scoped, and `Signal` targets top-level event keys.

## Stream Progress

```go
// Inside an activity: emit deltas.
_ = anchor.Emit(ctx, progress{Stage: "charged"})

// Anywhere: catch-up, then live tail.
stream, _ := c.Watch(ctx, "order-1042", "")
for {
	d, _, err := stream.Recv(ctx)
	if err != nil {
		break
	}
	fmt.Printf("%s\n", d.Payload)
}
```

Deltas live on the queue side behind the optional `DeltaLog` capability (an append-only table plus `LISTEN/NOTIFY` in `pgqueue.NewDeltaQueue`, Redis Streams in the redis adapter), split by attempt and seal dividers so consumers can discard superseded partial output. Checkpointed activities never re-stream.

Postgres-only streaming swaps `pgqueue.New` for `pgqueue.NewDeltaQueue` and one more table (apply `pgqueue.RecommendedDeltaDDL` with your own migration tooling):

```go
q, err := pgqueue.NewDeltaQueue(pool, pgqueue.DefaultMapping(), pgqueue.DefaultDeltaMapping())
if err != nil {
	log.Fatal(err)
}
if err := q.Validate(ctx); err != nil {
	log.Fatal(err) // checks both the queue and the delta table
}
```

## Postgres Plumbing

`pgkit` holds the two pieces every Postgres-backed component needs. `Apply` runs idempotent steps in one transaction under an advisory lock named by the caller, so replicas that start together migrate one at a time:

```go
err := pgkit.Apply(ctx, pool, "orders.durable",
	pgledger.RecommendedDDL(pgledger.DefaultMapping()),
	pgqueue.RecommendedDDL(pgqueue.DefaultMapping()))
```

`Listener` multiplexes `LISTEN` over one dedicated connection, held only while something is subscribed. Every subscription starts with a `Resync`, and gets another after a reconnect, meaning "re-read your state, notifications may have been missed":

```go
l, err := pgkit.NewListener(pool)
notes, err := l.Listen(ctx, "orders") // closed when ctx ends
for n := range notes {
	if n.Resync {
		// re-read what you watch
		continue
	}
	fmt.Println(n.Payload)
}

// Share it: one connection for the delta log and everything else.
q, err := pgqueue.NewDeltaQueue(pool, qm, dm, pgqueue.WithListener(l))
```

## Inspect a Ledger

`anchor`, a command in the Postgres adapter module (`adapters/postgres/cmd/anchor`), validates a ledger from the outside: it lists runs, shows one with its records, and replays one without side effects to report where a worker would resume it. It only reads, and it checks the tables with `Validate` before it reads anything.

```sh
cd adapters/postgres && go build -o bin/anchor ./cmd/anchor
export ANCHOR_DATABASE_URL=postgres://anchor:anchor@localhost:5432/anchor   # or pass -db on each command

bin/anchor runs -status pending -limit 20   # newest first; -status pending|succeeded|failed
bin/anchor show order-1042                  # the run, its records in recorded order, its status
bin/anchor replay order-1042                # where a worker would resume, or that the run is complete
```

```text
$ bin/anchor replay order-1042
run order-1042 (process-order), pending, 2 records

replayed from the ledger, nothing executed:
  charge-payment#0     activity  recorded
  reserve-inventory#0  activity  recorded

resume: the next call after reserve-inventory#0 executes for real, or parks the run if it awaits an event
```

`replay` has no workflow code, so it plays the recorded calls themselves into the `replay.Frame` a worker builds, handing the frame a ledger that refuses every write, and reports what the frame does: a complete run (a worker settles it without replaying), a resume after the last recorded call, a skipped occurrence that would execute for real (`charge-payment#0` missing while `charge-payment#1` is recorded), a parent whose own record is missing and whose body would re-execute around its recorded calls (the worker died inside it), or a park at an event with no record. It checks what the ledger alone can show: every key derives from its record's kind, name and occurrence; every record carries the codec content type your workers use (`-codec`, default `application/json`); activity records carry an input hash. It cannot compare input hashes with live inputs, which only your code computes, so a changed input shows up at the worker, not here.

- `-json` prints machine output. Payloads that are JSON appear as JSON under `input` and `output`; other bytes appear base64-encoded under `input_base64` and `output_base64`.
- Exit codes: 0 ok, 1 the command failed (connection, schema, unknown run), 2 usage, 3 `replay` found a divergence a worker would fail the run on, or a record no call could have produced.
- The URL comes from `-db` or `ANCHOR_DATABASE_URL` and is never printed, not in errors and not in `-h`. Connections open with `default_transaction_read_only` on, so the server refuses any write.
- Tables: `-runs-table` and `-records-table` point at `DefaultMapping`-shaped tables under other names (fiber's are `fiber_runs` and `fiber_run_records`). Other column mappings are not supported by the command; in code, `pgledger.Ledger.ListRuns`, `GetRun` and `Load` read any mapping.
- Build it from a checkout: the adapter module's `go.mod` keeps a `replace` for in-repo development, which `go install ...@version` refuses.

## Concepts

| Concept | What it is |
|---------|------------|
| ledger | Durable truth: runs and their memoized records, in tables you already own (`pkg/ledger`) |
| queue | Flow and clock: delivery of run IDs under fenced, time-bounded claims; disposable, rebuildable from the ledger (`pkg/queue`) |
| worker | A pull loop with no inbound surface: claims, replays to the frontier, writes one outcome per claim (`pkg/worker`) |

## Examples

| Example | Description |
|---------|-------------|
| [`quickstart`](examples/quickstart/) | The hero, runnable: an order pipeline on Postgres + Redis that survives its own worker being killed mid-run |
| [`chaos`](examples/chaos/) | Cross-process kill test: SIGKILL a worker binary mid-activity and prove recorded activities never re-execute |

## Documentation

- [Architecture overview](docs/architecture/overview.md): the three concepts, the package DAG, modules, adapters, capabilities
- [Replay design](docs/design/replay.md): the replay algorithm, the claim protocol, and the failure-mode table
- [AGENTS.md](AGENTS.md): AI-facing conventions, commands, extension guide
- [Contributing](CONTRIBUTING.md): development workflow and commit convention

## Where this sits

anchor is the bottom of five layers (warp, fiber, saige, playscape, anchor):
durability for everything above it. playscape, saige and fiber build on its
ledger, queue and worker rather than keeping their own, and its Postgres
adapter module holds the stack's shared Postgres plumbing (`pgkit`:
advisory-locked migrations and LISTEN/NOTIFY). It is a library chosen in
code, with no configuration file; its one binary, `anchor`, only reads a
ledger to validate it. [The ecosystem](https://github.com/urmzd/warp/blob/main/docs/ecosystem.md) lists who owns what, the
remaining duplication and the stack's limitations in one place.

## License

[Apache-2.0](LICENSE)
