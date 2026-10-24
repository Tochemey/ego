module github.com/tochemey/ego/v5/example/migration

go 1.27.0

replace (
	github.com/tochemey/ego/cmd/migrate => ../../cmd/migrate
	github.com/tochemey/ego/v5 => ../../
)

require (
	github.com/jackc/pgx/v5 v5.11.0
	github.com/spf13/cobra v1.10.2
	github.com/tochemey/ego/cmd/migrate v0.0.0-00010101000000-000000000000
	github.com/tochemey/ego/v5 v5.0.0
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/spf13/pflag v1.0.10 // indirect
	github.com/tochemey/goakt/v4 v4.6.3 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.28.0 // indirect
	golang.org/x/sync v0.24.0 // indirect
	golang.org/x/text v0.43.0 // indirect
)
