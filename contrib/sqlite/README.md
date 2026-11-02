# SQLite stores

This module provides the [eGo](https://github.com/Tochemey/ego) stores backed by SQLite:

| Store | Implements | Tables |
|---|---|---|
| `EventsStore` | `persistence.EventsStore` | `events_store` |
| `SnapshotStore` | `persistence.SnapshotStore` | `snapshots_store` |
| `DurableStore` | `persistence.StateStore` | `states_store` |
| `OffsetStore` | `offsetstore.OffsetStore` | `offsets_store`, `offsets_start` |

It uses [`modernc.org/sqlite`](https://pkg.go.dev/modernc.org/sqlite), a pure Go driver, so it builds without cgo.

## Installation

```bash
go get github.com/tochemey/ego/contrib/sqlite
```

## Schema

Create the tables before starting the application. The scripts are in [`resources`](./resources):

```bash
for script in resources/*.sql; do sqlite3 /var/lib/ego/ego.db < "$script"; done
```

| Script | Creates |
|---|---|
| `eventstore_sqlite.sql` | `events_store` and its shard index |
| `snapshotstore_sqlite.sql` | `snapshots_store` |
| `durablestore_sqlite.sql` | `states_store` |
| `offsetstore_sqlite.sql` | `offsets_store`, its entity index, and `offsets_start` |

Every query the stores run is served by the primary keys and these indexes.

## Open the database

`NewDB` opens the database file and returns the `*sql.DB` every store runs on:

```go
db, err := sqlite.NewDB(ctx, &sqlite.Config{DBPath: "/var/lib/ego/ego.db"})
if err != nil {
    return err
}
defer db.Close()

config := ego.NewConfig(sqlite.NewEventsStore(db),
    ego.WithSnapshotStore(sqlite.NewSnapshotStore(db)),
    ego.WithStateStore(sqlite.NewDurableStore(db)),
    ego.WithOffsetStore(sqlite.NewOffsetStore(db)),
)
```

Build the actor system and the engine from `config` as the [eGo README](../../readme.md) shows; the engine connects the stores itself.

| Field | Default | Meaning |
|---|---|---|
| `DBPath` | required | Path of the database file; its directory must exist |
| `BusyTimeout` | 5 seconds | How long a write waits for another one to finish |

SQLite allows one writer at a time. `NewDB` opens every connection so that concurrent writes wait for each other rather than fail with `SQLITE_BUSY`:

- write-ahead logging (`journal_mode=WAL`), so reads never wait for a write;
- `synchronous=NORMAL`, durable across a process crash; a power loss can lose the latest commits;
- the busy timeout, how long a write waits for the lock;
- transactions that begin `IMMEDIATE`, taking the write lock when they start, so they wait for the busy timeout instead of failing halfway.

## Reuse the database handle

The handle belongs to the application. The stores never close it: `Connect` and `Ping` check the database is reachable and `Disconnect` releases nothing. Close the handle after the engine stops.

The application can run its own queries and transactions on the same handle. A `projection.TransactionalHandler` uses it to apply an event and record the event's offset in one transaction, with `OffsetStore.WriteOffsetsTx`:

```go
type BalanceHandler struct {
    db      *sql.DB
    offsets *sqlite.OffsetStore
}

func (h *BalanceHandler) Handle(ctx context.Context, persistenceID string, event *anypb.Any, _ uint64, offset *egopb.EntityOffset) error {
    tx, err := h.db.BeginTx(ctx, nil)
    if err != nil {
        return err
    }
    defer tx.Rollback()

    if err := applyToBalances(ctx, tx, persistenceID, event); err != nil {
        return err
    }

    if err := h.offsets.WriteOffsetsTx(ctx, tx, "balances", []*egopb.EntityOffset{offset}); err != nil {
        return err
    }

    return tx.Commit()
}
```

The effect and the offset are committed together, so the projection applies each event exactly once.

## Storage

- A payload is stored as the type URL and the bytes of its `anypb.Any`, so a store reads it back without knowing its message type.
- `WriteEvents` and `WriteOffsets` insert many rows per statement. A write that fits in one statement is that statement alone; a larger one is split into statements within one transaction, so every write is atomic.
- `WriteEvents` rejects an event whose persistence ID and sequence number are stored already, and stores none of the write's events.
- `ShardOffsets` reads the newest event of each of the 1024 shards with one index seek per shard, so its cost does not grow with the journal.
- `DeleteState` with a positive version keeps a tombstone: the version, no state, and the entity's shard.

## Testing

```bash
go test -race ./...
go test -run '^$' -bench . ./
```
