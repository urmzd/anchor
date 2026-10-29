module github.com/urmzd/anchor/examples/chaos

go 1.26.4

require (
	github.com/jackc/pgx/v5 v5.11.0
	github.com/redis/go-redis/v9 v9.21.0
	github.com/urmzd/anchor v0.2.0
	github.com/urmzd/anchor/adapters/postgres v0.0.0
	github.com/urmzd/anchor/adapters/redis v0.0.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/sync v0.24.0 // indirect
	golang.org/x/text v0.43.0 // indirect
)

replace (
	github.com/urmzd/anchor => ../..
	github.com/urmzd/anchor/adapters/postgres => ../../adapters/postgres
	github.com/urmzd/anchor/adapters/redis => ../../adapters/redis
)
