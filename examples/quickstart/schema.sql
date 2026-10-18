-- Suggested DDL — applied by Postgres's init hook via docker-compose;
-- anchor never creates tables.
--
-- Generated from pgledger.RecommendedDDL(pgledger.DefaultMapping()) and
-- pgqueue.RecommendedDDL(pgqueue.DefaultMapping()). In your own system,
-- apply the equivalent through your migration tooling — or map the adapters
-- onto tables you already have.

-- Runs table for pgledger. Suggested only: anchor never executes DDL.
CREATE TABLE IF NOT EXISTS "anchor_runs" (
    "run_id" text NOT NULL,
    "status" text NOT NULL,
    "envelope" jsonb NOT NULL,
    "created_at" timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS "anchor_runs_run_id_key" ON "anchor_runs" ("run_id");
CREATE INDEX IF NOT EXISTS "anchor_runs_pending_idx" ON "anchor_runs" ("status", "created_at");

-- Records table for pgledger. Suggested only: anchor never executes DDL.
CREATE TABLE IF NOT EXISTS "anchor_records" (
    "run_id" text NOT NULL,
    "record_key" text NOT NULL,
    "envelope" jsonb NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS "anchor_records_run_id_record_key_key" ON "anchor_records" ("run_id", "record_key");

-- Queue table for pgqueue. Suggested only: anchor never executes DDL.
CREATE TABLE IF NOT EXISTS "anchor_queue" (
    "run_id" text NOT NULL,
    "ready_at" timestamptz,
    "lease_until" timestamptz,
    "attempt" bigint NOT NULL DEFAULT 0,
    "failures" int NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS "anchor_queue_run_id_key" ON "anchor_queue" ("run_id");
CREATE INDEX IF NOT EXISTS "anchor_queue_claimable_idx" ON "anchor_queue" ("ready_at", "lease_until");
