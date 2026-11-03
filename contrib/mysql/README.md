# MySQL stores for eGo

`github.com/tochemey/ego/contrib/mysql` implements the four eGo v5 stores on MySQL 8.0.19 or later with [go-sql-driver/mysql](https://github.com/go-sql-driver/mysql):

| Store | Interface | Tables |
|---|---|---|
| `EventsStore` | `persistence.EventsStore` | `events_store` |
| `SnapshotStore` | `persistence.SnapshotStore` | `snapshots_store` |
| `DurableStore` | `persistence.StateStore` | `states_store` |
| `OffsetStore` | `offsetstore.OffsetStore` | `offsets_store`, `offsets_start` |

`WriteEvents` and `WriteOffsets` insert every row with one multi-row `INSERT`, so a write is atomic without an explicit transaction; only a write of more rows than MySQL binds in one statement (65535 placeholders) runs several statements, in one transaction. `ShardOffsets`, `LatestOffsets` and `PersistenceIDs` are served by loose index scans, one index seek per shard or per persistence ID, so their cost does not grow with the journal.

## Installation

```shell
go get github.com/tochemey/ego/contrib/mysql
```

## Schema

Create the tables before starting the application. The SQL files of [`resources`](./resources) define them, with the indexes every query relies on:

```shell
mysql ego < resources/eventstore_mysql.sql
mysql ego < resources/snapshotstore_mysql.sql
mysql ego < resources/durablestore_mysql.sql
mysql ego < resources/offsetstore_mysql.sql
```

Apply only the files of the stores the application uses. The tables use InnoDB and the `utf8mb4_bin` collation, so persistence IDs and projection names are compared byte by byte: `account-1` and `Account-1` are two entities. MySQL 8.0.19 is the minimum version: the upserts use the `INSERT ... AS alias ON DUPLICATE KEY UPDATE` row alias.

## Reuse the database handle

The stores run on a `*sql.DB` the application opens; they never close it, so the same handle serves the four stores and the application's own queries:

```go
import (
    "database/sql"

    _ "github.com/go-sql-driver/mysql"

    "github.com/tochemey/ego/contrib/mysql"
)

db, err := sql.Open("mysql", "ego:secret@tcp(localhost:3306)/ego?interpolateParams=true")
if err != nil {
    return err
}
defer db.Close() // after the engine has stopped

eventsStore := mysql.NewEventsStore(db)
snapshotStore := mysql.NewSnapshotStore(db)
durableStore := mysql.NewDurableStore(db)
offsetStore := mysql.NewOffsetStore(db)

// the application's own queries run on the same handle
var accounts int
err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM account_balances").Scan(&accounts)
```

`interpolateParams=true` lets the driver send a statement and its arguments in one round trip; without it every statement is prepared, executed and closed, three round trips. `Connect` only pings the handle and `Disconnect` releases nothing. A large write is sent as one statement, so MySQL's `max_allowed_packet` must hold it.

### Exactly-once projections

A `projection.TransactionalHandler` applies an event and records its offset in one transaction, so the effect is applied exactly once. Begin the transaction on the shared handle and record the offset with `OffsetStore.WriteOffsetsTx`:

```go
type BalanceHandler struct {
    db      *sql.DB
    offsets *mysql.OffsetStore
}

func (h *BalanceHandler) Handle(ctx context.Context, persistenceID string, event *anypb.Any, revision uint64, offset *egopb.EntityOffset) error {
    tx, err := h.db.BeginTx(ctx, nil)
    if err != nil {
        return err
    }
    defer tx.Rollback() // a no-op once committed

    if _, err := tx.ExecContext(ctx,
        "INSERT INTO account_balances (account_id, balance) VALUES (?, ?) AS applied ON DUPLICATE KEY UPDATE balance = applied.balance",
        persistenceID, balanceOf(event)); err != nil {
        return err
    }

    if err := h.offsets.WriteOffsetsTx(ctx, tx, "balances", []*egopb.EntityOffset{offset}); err != nil {
        return err
    }

    return tx.Commit()
}
```

`WriteOffsetsTx` runs the statements of `WriteOffsets` on the caller's transaction and never commits it. The offsets tables must live in the database the handler writes to.

## Plug the stores into eGo

```go
config := ego.NewConfig(eventsStore,
    ego.WithSnapshotStore(snapshotStore),
    ego.WithStateStore(durableStore),
    ego.WithOffsetStore(offsetStore),
)

actorSystem, err := goakt.NewActorSystem("accounts", config.GoaktOptions()...)
if err != nil {
    return err
}

if err := actorSystem.Start(ctx); err != nil {
    return err
}

engine, err := ego.NewEngine(actorSystem, config)
```

eGo connects and pings the stores itself.

## Encoding

Events, snapshots and durable states are `anypb.Any` messages. A store keeps the `Value` of the Any in the payload column and its `TypeUrl` in the manifest column, and reads back `&anypb.Any{TypeUrl: manifest, Value: payload}`, so no message type has to be registered to read a row.

A durable state deleted with a positive version keeps a tombstone row: its version, an empty payload and an empty manifest. `GetLatestState` returns it with that version and no resulting state, so the versions of the persistence ID keep increasing after the deletion.

## Testing

`go test ./...` runs the suite, including the shared contract suites of `persistencetest` and `offsetstoretest`, against MySQL 8.4 started with Testcontainers, so Docker must be running. `go test -run '^$' -bench .` runs the `WriteEvents` and `EventsByShard` benchmarks.
