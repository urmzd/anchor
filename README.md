# anchor

> **Moved.** anchor is now `durable` in [urmzd/orchagent](https://github.com/urmzd/orchagent). This repository is archived. Its last release only marks the Go module deprecated.

Durable execution: a ledger of runs, a queue with leases and fencing, a worker that resumes after a crash, replay.

## Migrating

```sh
go get github.com/urmzd/orchagent@latest
```

Then rewrite imports:

| Before | After |
|--------|-------|
| `github.com/urmzd/anchor` | `github.com/urmzd/orchagent/durable` |
| `github.com/urmzd/anchor/pkg/<pkg>` | `github.com/urmzd/orchagent/durable/<pkg>` |
| `github.com/urmzd/anchor/adapters/postgres/...` | `github.com/urmzd/orchagent/durable/adapters/postgres/...` |
| `github.com/urmzd/anchor/adapters/redis/...` | `github.com/urmzd/orchagent/durable/adapters/redis/...` |

Package names follow the directory: `anchor.X` becomes `durable.X`. Names stored in databases or on the wire (table names, URIs, event kinds) are unchanged.

The history of this repository is preserved in orchagent under `durable/`.
