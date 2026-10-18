# Contributing

## Prerequisites

| Requirement | Version |
|-------------|---------|
| Go | per `go.mod` |
| golangci-lint | latest |
| make | any |
| Docker | any (integration tests and demo only) |

## Getting Started

```sh
git clone https://github.com/urmzd/anchor
cd anchor
make init
```

The repo is multi-module: the root SDK has zero third-party dependencies, and each adapter under `adapters/` is its own Go module. Every `make` target iterates all modules.

## Development

```sh
make check              # fmt + lint + test (quality gate)
make test               # go test ./... in every module
make fmt                # gofmt -w .
make test-integration   # docker compose up + go test -tags=integration in adapters and examples/chaos
make demo               # docker compose up + run the quickstart example
```

The replay engine (`pkg/replay`) and queue fencing are the correctness core: changes touching them must include tests proving the memoization and fencing contracts still hold. The conformance suites (`pkg/ledger/ledgertest`, `pkg/queue/queuetest`) are the executable contract for backends; new backends must pass them.

## Commit Convention

Conventional commits (Angular style): `feat:`, `fix:`, `docs:`, `chore:`, etc. Releases are cut automatically from commit types by [sr](https://github.com/urmzd/sr).

## Pull Requests

Fork, branch from `main`, make your change, ensure `make check` passes, and open a PR. CI must be green before review.

## Code Style

Idiomatic Go. Small interfaces, one concern per package, errors wrapped with context and sentinel errors for callers to match on. The root module stays stdlib-only; adapters keep their dependencies to themselves. See AGENTS.md for the full rules.
