// Demo driver: submit one chaos run and wait for it. Run a worker first
// (`go run ./worker`), then:
//
//	go run . [seed]
//
// This process only submits and observes; execution happens in whichever
// worker claims the run.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	anchor "github.com/urmzd/anchor"
	"github.com/urmzd/anchor/adapters/postgres/pgledger"
	"github.com/urmzd/anchor/adapters/redis/redisqueue"
	"github.com/urmzd/anchor/examples/chaos/internal/chaoswf"
)

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	ctx := context.Background()
	seed := "chaos"
	if len(os.Args) > 1 {
		seed = os.Args[1]
	}

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

	rdb := redis.NewClient(&redis.Options{Addr: env("ANCHOR_REDIS_ADDR", "localhost:6379")})
	defer func() { _ = rdb.Close() }()
	q := redisqueue.New(rdb, env("ANCHOR_REDIS_NAMESPACE", "chaos"))

	c := anchor.New(lgr, q)
	h, err := anchor.Start(ctx, c, chaoswf.Process, seed)
	if err != nil {
		fatal("start: %v", err)
	}
	fmt.Printf("submitted run %s\n", h.RunID())

	waitCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	out, err := h.Result(waitCtx)
	if err != nil {
		fatal("result: %v", err)
	}
	fmt.Printf("succeeded: %s\n", out)
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
