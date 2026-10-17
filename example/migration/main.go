// MIT License
//
// Copyright (c) 2022-2026 Arsene Tochemey Gandote
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

// Command ego-migration migrates a large eGo v4 PostgreSQL deployment to v5
// end to end, with the migration command of github.com/tochemey/ego/cmd/migrate.
//
// Its subcommands:
//
//   - seed fills the v4 database with a large journal, snapshots and
//     projection offsets, including the cases v5 fixes;
//   - write keeps appending events to the v4 journal for a while, as a live v4
//     deployment does during the first copy;
//   - migrate is the migration command itself, with an opener over the v4 and
//     v5 databases;
//   - verify checks the v5 database against the v4 one.
//
// run.sh runs them in order against the databases of docker-compose.yml.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"

	"github.com/tochemey/ego/cmd/migrate"
)

// maxConnections bounds each database pool. It covers the migration's default
// parallelism with room for the listing queries.
const maxConnections = 20

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	root := &cobra.Command{
		Use:          "ego-migration",
		Short:        "Migrates a large eGo v4 PostgreSQL deployment to v5",
		SilenceUsage: true,
	}
	root.AddCommand(newSeedCommand(), newWriteCommand(), newVerifyCommand(), migrate.NewCommand(openStores))

	if err := root.ExecuteContext(ctx); err != nil {
		os.Exit(1)
	}
}

// openStores is the migrate.Opener of the example: --source and --target are
// the connection strings of the v4 and v5 databases. The v4 and v5 tables of
// events and snapshots have the same columns, so the same stores read the
// legacy data and write the migrated data.
func openStores(ctx context.Context, config migrate.Config) (*migrate.Stores, migrate.Release, error) {
	source, err := openPool(ctx, config.Source)
	if err != nil {
		return nil, nil, fmt.Errorf("source: %w", err)
	}

	target, err := openPool(ctx, config.Target)
	if err != nil {
		source.Close()
		return nil, nil, fmt.Errorf("target: %w", err)
	}

	stores := &migrate.Stores{
		SourceJournal:   &eventsStore{pool: source},
		TargetJournal:   &eventsStore{pool: target},
		SourceSnapshots: &snapshotsStore{pool: source},
		TargetSnapshots: &snapshotsStore{pool: target},
		LegacyOffsets:   &legacyOffsets{pool: source},
		TargetOffsets:   &offsetsStore{pool: target},
	}

	release := func(context.Context) error {
		source.Close()
		target.Close()
		return nil
	}

	return stores, release, nil
}

// openPool opens a connection pool to the database at dsn.
func openPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}

	config.MaxConns = maxConnections
	return pgxpool.NewWithConfig(ctx, config)
}
