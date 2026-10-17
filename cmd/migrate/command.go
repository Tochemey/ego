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

// Package migrate provides a command line tool, built on cobra, that migrates
// the data of an earlier eGo release into the layout the current release reads.
//
// The command is store-agnostic: an Opener turns the command line's source and
// target into the stores to read and write. A tool for a given database is a
// few lines:
//
//	func main() {
//	    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
//	    defer stop()
//	    if err := migrate.NewCommand(openPostgresStores).ExecuteContext(ctx); err != nil {
//	        os.Exit(1)
//	    }
//	}
//
// Every migration reads the source one page of persistence IDs at a time and
// processes several entities in parallel, so its memory does not grow with the
// size of the stores. The journal and snapshot migrations can be stopped and
// run again: an entity continues after what the target holds already, so they
// can run while the legacy deployment still writes, with a final run once it
// stops. The offsets migration runs once, after the writers are stopped and
// before the migrated deployment starts its projections: it replaces every
// offset the projections hold.
package migrate

import (
	"context"
	"errors"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/tochemey/goakt/v4/log"

	"github.com/tochemey/ego/v5/migration"
	"github.com/tochemey/ego/v5/offsetstore"
	"github.com/tochemey/ego/v5/persistence"
)

const (
	// sourceFlag names the flag locating the legacy stores.
	sourceFlag = "source"
	// targetFlag names the flag locating the stores the data is migrated into.
	targetFlag = "target"
	// parallelismFlag names the flag setting how many entities are migrated at once.
	parallelismFlag = "parallelism"
	// pageSizeFlag names the flag setting how many persistence IDs, or events of
	// one entity, are read at once.
	pageSizeFlag = "page-size"
)

var (
	// ErrSourceJournalRequired is returned when a command needs the legacy
	// journal and the Opener provided none.
	ErrSourceJournalRequired = errors.New("migrate: the source journal is required")
	// ErrTargetJournalRequired is returned when a command needs the events
	// store to copy the journal into and the Opener provided none.
	ErrTargetJournalRequired = errors.New("migrate: the target journal is required")
	// ErrTargetSnapshotsRequired is returned when a command needs a snapshot
	// store to write into and the Opener provided none.
	ErrTargetSnapshotsRequired = errors.New("migrate: the target snapshot store is required")
	// ErrLegacyOffsetsRequired is returned when the offsets command runs and
	// the Opener provided no reader of the legacy offsets.
	ErrLegacyOffsetsRequired = errors.New("migrate: the legacy offsets reader is required")
	// ErrTargetOffsetsRequired is returned when the offsets command runs and
	// the Opener provided no offset store to resume the projections in.
	ErrTargetOffsetsRequired = errors.New("migrate: the target offset store is required")
)

// Config holds what the command line says about the stores to migrate. The
// meaning of Source and Target is the Opener's: for SQL stores, typically a
// connection string each.
type Config struct {
	// Source locates the stores of the legacy deployment.
	Source string
	// Target locates the stores the data is migrated into.
	Target string
}

// Stores holds the stores a migration reads and writes. A command fails with
// one of the Err...Required errors when a store it needs is nil; the journal
// command copies snapshots only when both snapshot stores are set.
type Stores struct {
	// SourceJournal reads the legacy journal.
	SourceJournal migration.JournalReader
	// TargetJournal is the events store the journal is copied into.
	TargetJournal persistence.EventsStore
	// SourceSnapshots reads the legacy snapshot store.
	SourceSnapshots migration.SnapshotReader
	// TargetSnapshots is the snapshot store snapshots are written to.
	TargetSnapshots persistence.SnapshotStore
	// LegacyOffsets reads the offsets the projections committed on the legacy
	// offset store.
	LegacyOffsets migration.LegacyOffsetReader
	// TargetOffsets is the offset store the projections resume from.
	TargetOffsets offsetstore.OffsetStore
}

// Release releases the stores an Opener opened.
type Release func(ctx context.Context) error

// Opener opens the stores the command line describes. The command calls the
// returned Release once the migration is over, whether it succeeded or not.
type Opener func(ctx context.Context, config Config) (*Stores, Release, error)

// NewCommand returns the migration command. Its subcommands are:
//
//   - journal: copies the legacy journal into the target journal, recomputing
//     every event's shard, then each entity's latest snapshot when both
//     snapshot stores are provided.
//   - offsets: makes every projection resume, on the migrated journal, from
//     the oldest of the offsets it committed on the legacy offset store.
//   - legacy-snapshots: writes, for every entity whose legacy events carry
//     their resulting state, a snapshot of its latest state.
//   - all: runs journal, then offsets.
func NewCommand(open Opener) *cobra.Command {
	root := &cobra.Command{
		Use:           "migrate",
		Short:         "Migrates the data of an earlier eGo release",
		Long:          "Migrates the journal, snapshots and projection offsets of an earlier eGo release into the layout the current release reads. The journal and snapshot migrations can be stopped and run again; the offsets migration runs once, after the legacy writers are stopped.",
		SilenceUsage:  true,
		SilenceErrors: false,
	}

	flags := root.PersistentFlags()
	flags.String(sourceFlag, "", "location of the legacy stores, as the opener understands it")
	flags.String(targetFlag, "", "location of the stores to migrate into, as the opener understands it")
	flags.Int(parallelismFlag, migration.DefaultParallelism, "how many entities are migrated at once")
	flags.Uint64(pageSizeFlag, migration.DefaultPageSize, "how many persistence IDs, or events of one entity, are read at once")

	root.AddCommand(
		newSubcommand(open, "journal", "Copies the journal, and the latest snapshot of every entity, into the target stores", migrateJournal),
		newSubcommand(open, "offsets", "Makes every projection resume on the migrated journal", migrateOffsets),
		newSubcommand(open, "legacy-snapshots", "Writes snapshots from legacy events that carry their resulting state", migrateLegacySnapshots),
		newSubcommand(open, "all", "Runs journal, then offsets", migrateAll),
	)

	return root
}

// migrationStep is the work of a subcommand, given the opened stores and the
// options the command line sets.
type migrationStep func(cmd *cobra.Command, stores *Stores, opts []migration.Option) error

// newSubcommand builds a subcommand that opens the stores, runs the step and
// releases the stores.
func newSubcommand(open Opener, use, short string, step migrationStep) *cobra.Command {
	return &cobra.Command{
		Use:   use,
		Short: short,
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) (err error) {
			config, opts, err := readFlags(cmd)
			if err != nil {
				return err
			}

			stores, release, err := open(cmd.Context(), config)
			if err != nil {
				return fmt.Errorf("migrate: failed to open the stores: %w", err)
			}

			defer func() {
				// the stores are released even after the command was interrupted
				if releaseErr := release(context.WithoutCancel(cmd.Context())); releaseErr != nil {
					err = errors.Join(err, fmt.Errorf("migrate: failed to release the stores: %w", releaseErr))
				}
			}()

			return step(cmd, stores, opts)
		},
	}
}

// readFlags reads the store locations and the migration options from the
// command line.
func readFlags(cmd *cobra.Command) (Config, []migration.Option, error) {
	flags := cmd.Flags()
	source, _ := flags.GetString(sourceFlag)
	target, _ := flags.GetString(targetFlag)
	parallelism, _ := flags.GetInt(parallelismFlag)
	pageSize, _ := flags.GetUint64(pageSizeFlag)

	if parallelism < 1 {
		return Config{}, nil, fmt.Errorf("migrate: --%s must be at least 1", parallelismFlag)
	}

	if pageSize < 1 {
		return Config{}, nil, fmt.Errorf("migrate: --%s must be at least 1", pageSizeFlag)
	}

	opts := []migration.Option{
		migration.WithParallelism(parallelism),
		migration.WithPageSize(pageSize),
		migration.WithLogger(log.NewZap(log.InfoLevel, cmd.ErrOrStderr())),
	}

	return Config{Source: source, Target: target}, opts, nil
}

// migrateJournal copies the journal, then the latest snapshot of every entity
// when both snapshot stores are provided.
func migrateJournal(cmd *cobra.Command, stores *Stores, opts []migration.Option) error {
	if stores.SourceJournal == nil {
		return ErrSourceJournalRequired
	}

	if stores.TargetJournal == nil {
		return ErrTargetJournalRequired
	}

	if err := migration.NewJournalMigrator(stores.SourceJournal, stores.TargetJournal, opts...).Run(cmd.Context()); err != nil {
		return err
	}

	if stores.SourceSnapshots == nil || stores.TargetSnapshots == nil {
		fmt.Fprintln(cmd.OutOrStdout(), "journal migrated; snapshots skipped: no snapshot stores")
		return nil
	}

	if err := migration.NewSnapshotMigrator(stores.SourceSnapshots, stores.TargetSnapshots, opts...).Run(cmd.Context()); err != nil {
		return err
	}

	fmt.Fprintln(cmd.OutOrStdout(), "journal and snapshots migrated")
	return nil
}

// migrateOffsets makes every projection resume on the migrated journal.
func migrateOffsets(cmd *cobra.Command, stores *Stores, _ []migration.Option) error {
	if stores.LegacyOffsets == nil {
		return ErrLegacyOffsetsRequired
	}

	if stores.TargetOffsets == nil {
		return ErrTargetOffsetsRequired
	}

	resumed, err := migration.ResumeProjections(cmd.Context(), stores.TargetOffsets, stores.LegacyOffsets)
	if err != nil {
		return err
	}

	fmt.Fprintf(cmd.OutOrStdout(), "%d projections resumed\n", resumed)
	return nil
}

// migrateLegacySnapshots writes snapshots from legacy events that carry their
// resulting state.
func migrateLegacySnapshots(cmd *cobra.Command, stores *Stores, opts []migration.Option) error {
	if stores.SourceJournal == nil {
		return ErrSourceJournalRequired
	}

	if stores.TargetSnapshots == nil {
		return ErrTargetSnapshotsRequired
	}

	if err := migration.New(stores.SourceJournal, stores.TargetSnapshots, opts...).Run(cmd.Context()); err != nil {
		return err
	}

	fmt.Fprintln(cmd.OutOrStdout(), "legacy snapshots migrated")
	return nil
}

// migrateAll runs the journal migration, then the offsets migration.
func migrateAll(cmd *cobra.Command, stores *Stores, opts []migration.Option) error {
	if err := migrateJournal(cmd, stores, opts); err != nil {
		return err
	}

	return migrateOffsets(cmd, stores, opts)
}
