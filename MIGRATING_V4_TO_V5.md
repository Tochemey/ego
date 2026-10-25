# Migrating from v4 to v5

eGo v5 changes where events live and how projections track their progress, so a v4 deployment cannot simply be redeployed on v5: its stores must implement the new contracts and its data must be copied into the v5 layout.

- **Journal shards are fixed.** An event's shard is `persistence.ShardFor(persistenceID)`, one of 1024 shards, instead of its entity's cluster partition. Every v4 event has to move to its new shard.
- **An entity's timestamps strictly increase.** v4 events stamped at or before their predecessor, for instance after a clock stepped back, are restamped a nanosecond after it.
- **Projections record one offset per entity**, instead of one per shard, so every projection has to be resumed on the migrated journal.

The migration copies the v4 data into new v5 stores and never modifies the v4 stores, so the v4 deployment keeps running until you switch over. The [changelog](./CHANGELOG.md) lists every breaking change of the release.

## 1. Update the code

Replace `github.com/tochemey/ego/v4` with `github.com/tochemey/ego/v5` in every import and in `go.mod`, including the generated `egopb` package, and build with Go 1.27 or later. The publisher modules move to `github.com/tochemey/ego/publisher/<name>/v2`. Go-Akt stays on `github.com/tochemey/goakt/v4`.

## 2. Implement the v5 store contracts

Every events store and offset store your application uses must implement the v5 interfaces:

- [`persistence.EventsStore`](./persistence/events_store.go): `EventsByShard` replaces `GetShardEvents`, and `WriteEvents` writes the events of one call atomically and rejects an event whose persistence ID and sequence number are stored already. An `ON CONFLICT DO NOTHING` insert must become a plain insert.
- [`offsetstore.OffsetStore`](./offsetstore/offset_store.go): `WriteOffsets`, `ReadOffsets`, `ReadEntityOffset`, `LatestOffsets`, `DeleteOffsetsBefore` and `StartOffset` replace `WriteOffset` and `GetCurrentOffset`, and `ResetOffset` now deletes the projection's offsets and records the timestamp it starts from.

The godoc of each interface gives the SQL its methods map to. The PostgreSQL reference schemas in [`resources`](./resources) define the v5 tables; create them in the database the v5 deployment will use. The [ego-contrib](https://github.com/Tochemey/ego-contrib) stores and the [cluster example](./example/cluster) show complete implementations.

## 3. Build the migration tool

The [`cmd/migrate`](./cmd/migrate) tool is not a prebuilt binary: it is the Go package `github.com/tochemey/ego/cmd/migrate`, a module of its own so that eGo itself does not depend on cobra. You build it once for your database, in the project that holds your store implementations or in a small module of its own:

```shell
go get github.com/tochemey/ego/v5@latest
go get github.com/tochemey/ego/cmd/migrate@latest
```

Then write a `main`, for instance in `cmd/migrate/main.go` of your project, that gives `migrate.NewCommand` an opener: a function that turns the `--source` and `--target` flags into the stores to read and write. For SQL stores the two flags are connection strings.

```go
package main

import (
    "context"
    "os"
    "os/signal"

    "github.com/jackc/pgx/v5/pgxpool"

    "github.com/tochemey/ego/cmd/migrate"
)

func main() {
    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
    defer stop()

    if err := migrate.NewCommand(openStores).ExecuteContext(ctx); err != nil {
        os.Exit(1)
    }
}

// openStores opens the v4 stores at config.Source and the v5 stores at config.Target.
func openStores(ctx context.Context, config migrate.Config) (*migrate.Stores, migrate.Release, error) {
    source, err := pgxpool.New(ctx, config.Source)
    if err != nil {
        return nil, nil, err
    }

    target, err := pgxpool.New(ctx, config.Target)
    if err != nil {
        source.Close()
        return nil, nil, err
    }

    stores := &migrate.Stores{
        SourceJournal:   newLegacyEventsStore(source),   // reads the v4 events_store table
        TargetJournal:   newEventsStore(target),         // the v5 events store
        SourceSnapshots: newLegacySnapshotStore(source), // reads the v4 snapshots_store table
        TargetSnapshots: newSnapshotStore(target),       // the v5 snapshot store
        LegacyOffsets:   newLegacyOffsetReader(source),  // reads the v4 offsets_store table
        TargetOffsets:   newOffsetStore(target),         // the v5 offset store
    }

    release := func(context.Context) error {
        source.Close()
        target.Close()
        return nil
    }

    return stores, release, nil
}
```

The target stores are your v5 implementations. The source stores only read the v4 tables:

- `SourceJournal` is a `migration.JournalReader`: `Ping`, `PersistenceIDs`, `GetLatestEvent` and `ReplayEvents`. A v4 events store already has them.
- `SourceSnapshots` is a `migration.SnapshotReader`: `Ping`, `GetLatestSnapshot` and `PersistenceIDs`. A v4 snapshot store lacks the listing; wrap it with one query, `SELECT DISTINCT persistence_id FROM snapshots_store WHERE persistence_id > $1 ORDER BY persistence_id LIMIT $2`, returning the last ID of the page as the next token. The migration lists the snapshot store rather than the journal because an entity whose events retention deleted has a snapshot and no event left. Leave both snapshot stores nil when the application uses none.
- `LegacyOffsets` is a `migration.LegacyOffsetReader`: the offset of every projection on every v4 shard that holds events, zero on a shard the projection committed no offset on, returned as a map from projection name to its offsets. When the v4 journal and offsets share a database, that is one query, given in the `LegacyOffsetReader` godoc.

## 4. Run the migration

```shell
go build -o migrate ./cmd/migrate

# 1. While the v4 deployment still runs: copy the journal and snapshots.
#    Run it as often as you like; every run continues where the previous one stopped.
./migrate journal --source "$V4_DSN" --target "$V5_DSN"

# 2. Stop every v4 writer.

# 3. Copy the remainder and resume the projections, once.
./migrate all --source "$V4_DSN" --target "$V5_DSN"

# 4. Start the v5 deployment.
```

Turn the v4 deployment's events retention off until step 3. Events it deletes after a copy, beyond what was copied, leave a gap in the v5 journal that projections cannot get past.

Run step 3 once, after the v4 writers are stopped and before the v5 deployment starts its projections. `migrate all` runs `migrate journal` and then `migrate offsets`, which resumes every projection from the oldest of its v4 offsets. Resuming replaces every offset a projection holds, so it refuses a projection that recorded offsets on the v5 journal already (`migration.ErrProjectionStarted`).

`--parallelism` (default 10) sets how many entities are migrated at once and `--page-size` (default 500) how many persistence IDs, or events of one entity, are read at once. Memory does not grow with the size of the journal. Interrupting a run with Ctrl-C is safe: the stores are released and the next run resumes.

`migrate legacy-snapshots` is only for journals written by releases that stored the resulting state in every event; it writes a snapshot of each entity's latest state from those events.

## 5. Start the v5 deployment

- Each projection delivers again, once, the events after the oldest of its v4 offsets, since those offsets do not map onto the new shards. Handlers are idempotent already, as at-least-once delivery requires.
- Keep each projection's `StartOffset` option unchanged: a v4 shard a projection never committed on is resumed from that start, as before.
- Leave each projection's `ResetOffset` option unset: it replaces the resumed start point every time the projection starts.
- Keep each projection's `BufferSize` above the number of events that can share one timestamp on a shard. The default of 500 leaves ample room; a projection that meets more fails with an error naming the shard and timestamp.

## Using the migration package directly

The tool is a thin layer over the [`migration`](./migration) package, which you can call from your own code instead:

- `migration.NewJournalMigrator` copies a journal into the v5 layout.
- `migration.NewSnapshotMigrator` copies the latest snapshot of every entity.
- `migration.ResumeProjections` resumes every projection a `migration.LegacyOffsetReader` reports, and `migration.ResumeProjection` resumes one.
- `migration.New` writes snapshots from legacy events that carry their resulting state.

## Example

The [migration example](./example/migration) runs all of this with Docker Compose against two PostgreSQL databases: it seeds a v4 deployment of a million events, copies it while it keeps writing, finishes the migration and checks the result.
