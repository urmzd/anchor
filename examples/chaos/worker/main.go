// The chaos worker: a standalone process that registers the chaos workflow,
// builds the same Postgres ledger and Redis queue as the driver, and runs
// until killed. The kill test builds this binary, SIGKILLs one instance
// mid-activity, and starts another — cross-process crash recovery against
// real infrastructure.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/urmzd/anchor/adapters/postgres/pgledger"
	"github.com/urmzd/anchor/adapters/redis/redisqueue"
	"github.com/urmzd/anchor/examples/chaos/internal/chaoswf"
	"github.com/urmzd/anchor/pkg/worker"
)

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, env("ANCHOR_POSTGRES_URL",
		"postgres://anchor:anchor@localhost:5432/anchor"))
	if err != nil {
		fatal("connect postgres: %v", err)
	}
	defer pool.Close()

	// The witness table is the example's own; anchor never sees it.
	if _, err := pool.Exec(ctx, chaoswf.EffectsDDL); err != nil {
		fatal("create chaos_effects: %v", err)
	}
	chaoswf.Effects = pool

	lgr, err := pgledger.New(pool, pgledger.DefaultMapping())
	if err != nil {
		fatal("ledger mapping: %v", err)
	}
	if err := lgr.Validate(ctx); err != nil {
		fatal("ledger schema: %v", err)
	}

	rdb := redis.NewClient(&redis.Options{Addr: env("ANCHOR_REDIS_ADDR", "localhost:6379")})
	defer func() { _ = rdb.Close() }()
	q := redisqueue.New(rdb, env("ANCHOR_REDIS_NAMESPACE", "chaos"))

	// Short lease + quick heartbeat: a killed worker's claim lapses within
	// ~2s, so a peer picks the run up fast. Logs go to stderr for the test's
	// transcript; tune verbosity with CHAOS_QUIET=1.
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if os.Getenv("CHAOS_QUIET") != "" {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	w := worker.New(lgr, q,
		worker.WithLeaseTTL(2*time.Second),
		worker.WithHeartbeatEvery(500*time.Millisecond),
		worker.WithBackoff(func(int) time.Duration { return 250 * time.Millisecond }),
		worker.WithLogger(logger),
	)

	fmt.Printf("chaos worker up (pid %d)\n", os.Getpid())
	if err := w.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		fatal("worker: %v", err)
	}
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
