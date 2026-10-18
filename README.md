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
- **Your tables, validated, never migrated**: the Postgres adapter maps onto tables you already own via a declarative `Mapping`. `Validate` introspects the live schema and errors with guidance; `RecommendedDDL` returns suggested DDL and never executes it.
- **Postgres-only works**: `pgqueue` (`FOR UPDATE SKIP LOCKED`) runs the queue in the same database. Redis (`redisqueue`: a ZSET lease queue plus a Redis Streams delta log) is a throughput upgrade, not a requirement, and is disposable: the janitor rebuilds it from the ledger.
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

Deltas live on the queue side behind the optional `DeltaLog` capability (Redis Streams in the redis adapter), split by attempt and seal dividers so consumers can discard superseded partial output. Checkpointed activities never re-stream.

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

## License

[Apache-2.0](LICENSE)
