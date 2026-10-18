# quickstart

The README hero, runnable: an order-processing workflow with the ledger in Postgres, the queue in Redis, and the worker embedded in the submitting process. It runs one order end to end, then crashes its own worker mid-confirmation and lets a second worker finish the run — without re-charging.

```sh
docker compose up -d --wait   # from the repo root; the init hook applies schema.sql
go run .
```

`schema.sql` is the suggested DDL from `pgledger.RecommendedDDL` and `pgqueue.RecommendedDDL` — applied by Postgres's own init hook, never by anchor.
