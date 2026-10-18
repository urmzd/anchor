// The README hero, runnable: an order-processing workflow on Postgres
// (ledger) and Redis (queue), with the worker embedded in this very process.
// It processes one order end to end, then kills its own worker mid-run and
// lets a second worker finish the order — without re-charging the customer.
//
//	docker compose up -d --wait   # from the repo root
//	go run .
package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	anchor "github.com/urmzd/anchor"
	"github.com/urmzd/anchor/adapters/postgres/pgledger"
	"github.com/urmzd/anchor/adapters/redis/redisqueue"
	"github.com/urmzd/anchor/pkg/worker"
)

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	ctx := context.Background()

	// Ledger: your Postgres, your tables. The compose file's init hook
	// applied schema.sql; anchor only validates, never migrates.
	pool, err := pgxpool.New(ctx, env("ANCHOR_POSTGRES_URL",
		"postgres://anchor:anchor@localhost:5432/anchor"))
	if err != nil {
		fatal("connect postgres: %v", err)
	}
	defer pool.Close()
	lgr, err := pgledger.New(pool, pgledger.DefaultMapping())
	if err != nil {
		fatal("ledger mapping: %v", err)
	}
	if err := lgr.Validate(ctx); err != nil {
		fatal("ledger schema: %v", err)
	}

	// Queue: disposable flow. Wiping Redis loses no runs — they live in the
	// ledger.
	rdb := redis.NewClient(&redis.Options{Addr: env("ANCHOR_REDIS_ADDR", "localhost:6379")})
	defer func() { _ = rdb.Close() }()
	q := redisqueue.New(rdb, "quickstart")

	c := anchor.New(lgr, q)
	batch := time.Now().UnixMilli() // fresh order IDs per demo run; the run ID is the submit idempotency key

	// The worker can be your own process: a pull loop on a goroutine, not a
	// service to deploy.
	worker1Ctx, crashWorker1 := context.WithCancel(ctx)
	go runWorker(worker1Ctx, lgr, q)

	fmt.Println("order 1 — the happy path")
	receipt, err := anchor.Exec(ctx, c, processOrder,
		Order{ID: fmt.Sprintf("%d-1", batch), Email: "ada@example.com", Amount: 2400},
		anchor.WithRunID(fmt.Sprintf("order-%d-1", batch)))
	if err != nil {
		fatal("order 1: %v", err)
	}
	fmt.Printf("  receipt  %s charged on %s at %s\n\n",
		receipt.ChargeID, receipt.OrderID, receipt.IssuedAt.Format(time.TimeOnly))

	fmt.Println("order 2 — the worker dies mid-confirmation")
	confirmShouldHang.Store(true)
	h, err := anchor.Start(ctx, c, processOrder,
		Order{ID: fmt.Sprintf("%d-2", batch), Email: "lin@example.com", Amount: 1800},
		anchor.WithRunID(fmt.Sprintf("order-%d-2", batch)))
	if err != nil {
		fatal("order 2: %v", err)
	}

	<-confirmEntered // sendConfirmation is in flight on worker 1...
	crashWorker1()   // ...and worker 1 is gone.
	fmt.Println("  crash    worker 1 died mid-send; charge and reserve are already in the ledger")

	go runWorker(ctx, lgr, q) // worker 2 claims the run when the lease lapses

	receipt, err = h.Result(ctx)
	if err != nil {
		fatal("order 2 after crash: %v", err)
	}
	fmt.Printf("  receipt  %s charged on %s at %s\n\n",
		receipt.ChargeID, receipt.OrderID, receipt.IssuedAt.Format(time.TimeOnly))

	fmt.Println("invocations across both orders")
	fmt.Printf("  charge-payment     %d  (one per order — replayed, never re-charged)\n", chargeCalls.Load())
	fmt.Printf("  reserve-inventory  %d  (one per order — replayed from the ledger)\n", reserveCalls.Load())
	fmt.Printf("  send-confirmation  %d  (order 2's first attempt died mid-send and retried)\n", confirmCalls.Load())
}

// runWorker starts one embedded worker. The short lease and quick backoff
// keep the crash-recovery window tight for the demo.
func runWorker(ctx context.Context, lgr *pgledger.Ledger, q *redisqueue.Queue) {
	w := worker.New(lgr, q,
		worker.WithLeaseTTL(2*time.Second),
		worker.WithHeartbeatEvery(500*time.Millisecond),
		worker.WithBackoff(func(int) time.Duration { return 500 * time.Millisecond }),
		worker.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	)
	_ = w.Run(ctx)
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
