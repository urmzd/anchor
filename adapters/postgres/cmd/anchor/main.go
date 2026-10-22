// Command anchor inspects an anchor ledger held in Postgres, to validate it:
// it lists runs, shows one run with its records, and replays a run's
// recorded calls through the replay frame a worker builds, executing
// nothing, to report where a worker would resume the run or that it is
// complete.
//
//	anchor runs   [flags]       list runs, newest first
//	anchor show   [flags] RUN   print a run, its records in recorded order, and its status
//	anchor replay [flags] RUN   replay the recorded calls without side effects
//
// It only reads. Its connections open with default_transaction_read_only
// on, so the server refuses any write, and the replay hands the frame a
// ledger that refuses every write. Before reading it validates the tables
// against the mapping (pgledger.Validate) and reports what is missing.
//
// It lives in the Postgres adapter module, beside pgledger, so the root
// module keeps zero third-party dependencies. It is a tool for people and
// scripts, not a service: it opens no listener and exits when the command
// does.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/urmzd/anchor/adapters/postgres/pgledger"
)

// envDatabaseURL names the environment variable read when -db is not set.
const envDatabaseURL = "ANCHOR_DATABASE_URL"

// Exit codes.
const (
	exitOK     = 0
	exitFailed = 1 // the command could not do its job: connection, schema, missing run
	exitUsage  = 2 // bad flags or arguments
	// exitDiverged means the replay ran and found the history does not
	// replay cleanly: a divergence a worker would fail the run on, or a
	// record no call could have produced.
	exitDiverged = 3
)

const usage = `anchor inspects an anchor ledger in Postgres. It only reads.

Usage:
  anchor runs   [flags]       list runs, newest first
  anchor show   [flags] RUN   print a run, its records in recorded order, and its status
  anchor replay [flags] RUN   replay the recorded calls without side effects and
                              report where a worker would resume, or that the run is complete

The database URL comes from -db or, when -db is not set, from ` + envDatabaseURL + `.
Run "anchor COMMAND -h" for the flags of a command.

Exit codes: 0 ok, 1 failed, 2 usage, 3 the replay found a divergence or a malformed record.
`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := execute(ctx, os.Args[1:], os.Getenv, os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

// execute runs the command line args and returns the process exit code.
// getenv supplies the environment; tests pass their own.
func execute(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return exitUsage
	}
	var cmd func(context.Context, *options, []string, io.Writer) error
	switch args[0] {
	case "runs":
		cmd = cmdRuns
	case "show":
		cmd = cmdShow
	case "replay":
		cmd = cmdReplay
	case "help", "-h", "-help", "--help":
		fmt.Fprint(stdout, usage)
		return exitOK
	default:
		fmt.Fprintf(stderr, "anchor: unknown command %q\n\n%s", args[0], usage)
		return exitUsage
	}

	o, rest, err := parseFlags(args[0], args[1:], stderr)
	if errors.Is(err, flag.ErrHelp) {
		return exitOK
	}
	if err == nil && o.db == "" {
		o.db = getenv(envDatabaseURL)
		if o.db == "" {
			err = usageError{msg: fmt.Sprintf("no database URL: set -db or %s", envDatabaseURL)}
		}
	}
	if err == nil {
		err = cmd(ctx, o, rest, stdout)
	}
	return report(err, o, stderr)
}

// report prints err once, scrubbed of the database URL, and maps it to an
// exit code.
func report(err error, o *options, stderr io.Writer) int {
	if err == nil {
		return exitOK
	}
	var dsn string
	if o != nil {
		dsn = o.db
	}
	var d divergedError
	if errors.As(err, &d) {
		return exitDiverged // the report on stdout already says why
	}
	var u usageError
	isUsage := errors.As(err, &u)
	if !isUsage || !u.printed {
		fmt.Fprintf(stderr, "anchor: %s\n", scrub(err.Error(), dsn))
	}
	if isUsage {
		return exitUsage
	}
	return exitFailed
}

// usageError is a bad flag or argument: exit 2. printed says the flag
// package already printed it, with the usage.
type usageError struct {
	msg     string
	printed bool
}

func (e usageError) Error() string { return e.msg }

// divergedError reports a replay that found problems; the report already
// went to stdout.
type divergedError struct{}

func (divergedError) Error() string { return "replay found problems" }

// options are the flags every command takes, plus the per-command ones.
type options struct {
	db           string
	runsTable    string
	recordsTable string
	json         bool

	// runs
	status string
	limit  int

	// replay
	codec string
}

// parseFlags parses a command's flags. Flags may come before or after the
// RUN argument.
func parseFlags(name string, args []string, stderr io.Writer) (*options, []string, error) {
	o := &options{}
	fs := flag.NewFlagSet("anchor "+name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	// The -db default stays empty: a default read from the environment
	// would print the URL in -h output.
	fs.StringVar(&o.db, "db", "", "Postgres URL of the ledger (default $"+envDatabaseURL+")")
	fs.StringVar(&o.runsTable, "runs-table", pgledger.DefaultMapping().Runs.Table, "runs table, optionally schema-qualified")
	fs.StringVar(&o.recordsTable, "records-table", pgledger.DefaultMapping().Records.Table, "records table, optionally schema-qualified")
	fs.BoolVar(&o.json, "json", false, "print JSON instead of text")
	argUsage := " RUN"
	switch name {
	case "runs":
		argUsage = ""
		fs.StringVar(&o.status, "status", "", "only runs with this status: pending, succeeded or failed")
		fs.IntVar(&o.limit, "limit", 50, "at most this many runs")
	case "replay":
		fs.StringVar(&o.codec, "codec", "application/json", "content type of the codec the workers record with")
	}
	fs.Usage = func() {
		fmt.Fprintf(stderr, "Usage: anchor %s [flags]%s\n\nFlags:\n", name, argUsage)
		fs.PrintDefaults()
	}

	var rest []string
	for {
		if err := fs.Parse(args); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return o, nil, err
			}
			return o, nil, usageError{msg: err.Error(), printed: true}
		}
		args = fs.Args()
		if len(args) == 0 {
			break
		}
		rest = append(rest, args[0])
		args = args[1:]
	}
	return o, rest, nil
}

// mapping is the default mapping with the tables the flags name.
func (o *options) mapping() pgledger.Mapping {
	m := pgledger.DefaultMapping()
	m.Runs.Table = o.runsTable
	m.Records.Table = o.recordsTable
	return m
}

// openLedger connects read-only, validates the tables against the mapping
// and returns the ledger. Errors never carry the URL.
func openLedger(ctx context.Context, o *options) (*pgledger.Ledger, func(), error) {
	cfg, err := poolConfig(o.db)
	if err != nil {
		return nil, nil, err
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, nil, connectError("open the database", err)
	}
	lgr, err := pgledger.New(pool, o.mapping())
	if err != nil {
		pool.Close()
		return nil, nil, usageError{msg: err.Error()}
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, nil, connectError("connect to the database", err)
	}
	if err := lgr.Validate(ctx); err != nil {
		pool.Close()
		return nil, nil, err
	}
	return lgr, pool.Close, nil
}

// poolConfig parses the URL into a small, read-only pool configuration.
func poolConfig(dsn string) (*pgxpool.Config, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		// pgx's parse error quotes the URL; say only that it did not parse.
		return nil, usageError{msg: "the database URL does not parse"}
	}
	cfg.MaxConns = 2
	if cfg.ConnConfig.ConnectTimeout == 0 {
		cfg.ConnConfig.ConnectTimeout = 10 * time.Second
	}
	// Every transaction on these connections is read-only: the server
	// itself refuses a write, whatever the code above it does.
	cfg.ConnConfig.RuntimeParams["default_transaction_read_only"] = "on"
	if _, ok := cfg.ConnConfig.RuntimeParams["application_name"]; !ok {
		cfg.ConnConfig.RuntimeParams["application_name"] = "anchor-cli"
	}
	return cfg, nil
}

// connectError reports a failure to connect. It drops pgx's "failed to
// connect to `user=... database=...`" prefix and keeps the reason, so the
// connection's identity stays out of the output; a reason per attempted
// address goes on its own indented line.
func connectError(what string, err error) error {
	var ce *pgconn.ConnectError
	if errors.As(err, &ce) {
		if inner := errors.Unwrap(ce); inner != nil {
			err = inner
		}
	}
	if msg := err.Error(); strings.Contains(msg, "\n") {
		return fmt.Errorf("%s:\n  %s", what, strings.ReplaceAll(msg, "\n", "\n  "))
	}
	return fmt.Errorf("%s: %w", what, err)
}

// scrub removes the database URL, and its password, from msg: defense in
// depth for driver errors that might quote them.
func scrub(msg, dsn string) string {
	if dsn == "" {
		return msg
	}
	msg = strings.ReplaceAll(msg, dsn, "[database URL]")
	cfg, err := pgconn.ParseConfig(dsn)
	if err != nil || cfg.Password == "" {
		return msg
	}
	for _, pw := range []string{cfg.Password, url.PathEscape(cfg.Password), url.QueryEscape(cfg.Password)} {
		msg = strings.ReplaceAll(msg, ":"+pw+"@", ":xxxxx@")
		msg = strings.ReplaceAll(msg, "password="+pw, "password=xxxxx")
	}
	return msg
}
