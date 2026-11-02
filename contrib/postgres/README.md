# PostgreSQL stores for eGo

`github.com/tochemey/ego/contrib/postgres` implements the four eGo v5 stores on PostgreSQL with [pgx](https://github.com/jackc/pgx):

| Store | Interface | Tables |
|---|---|---|
| `EventsStore` | `persistence.EventsStore` | `events_store` |
| `SnapshotStore` | `persistence.SnapshotStore` | `snapshots_store` |
| `DurableStore` | `persistence.StateStore` | `states_store` |
| `OffsetStore` | `offsetstore.OffsetStore` | `offsets_store`, `offsets_start` |

Every store call is a single SQL statement, so a call costs one round trip and is atomic without an explicit transaction. `WriteEvents` and `WriteOffsets` insert any number of rows with one `INSERT ... SELECT FROM unnest(...)` statement whose parameter count does not depend on the number of rows.

## Installation

```shell
go get github.com/tochemey/ego/contrib/postgres
```

## Schema

Create the tables before starting the application. The SQL files of [`resources`](./resources) define them, with the indexes every query relies on:

```shell
psql "$DATABASE_URL" -f resources/eventstore_postgres.sql
psql "$DATABASE_URL" -f resources/snapshotstore_postgres.sql
psql "$DATABASE_URL" -f resources/durablestore_postgres.sql
psql "$DATABASE_URL" -f resources/offsetstore_postgres.sql
```

Apply only the files of the stores the application uses. To keep the tables in a schema other than the default one, create them there and set `search_path` in the connection string.

## Reuse the pool

The stores run on a pool the application provides. Build it with `pgxpool.New` or `pgxpool.NewWithConfig`; the stores never close it, so the same pool serves the four stores and the application's own queries:

```go
pool, err := pgxpool.New(ctx, "postgres://ego:secret@localhost:5432/ego?sslmode=disable")
if err != nil {
    return err
}
defer pool.Close() // after the engine has stopped

eventsStore := postgres.NewEventsStore(pool)
snapshotStore := postgres.NewSnapshotStore(pool)
durableStore := postgres.NewDurableStore(pool)
offsetStore := postgres.NewOffsetStore(pool)

// the application's own queries run on the same pool
var accounts int
err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM account_balances").Scan(&accounts)
```

The stores accept the `Pool` interface, the part of `*pgxpool.Pool` they use: `Exec`, `Query`, `QueryRow` and `Ping`. A wrapper that adds tracing or metrics works as long as it implements those four methods. `Connect` only pings the pool and `Disconnect` releases nothing; close the pool once every store using it is disconnected.

### Exactly-once projections

A `projection.TransactionalHandler` applies an event and records its offset in one transaction, so the effect is applied exactly once. Begin the transaction on the shared pool and record the offset with `OffsetStore.WriteOffsetsTx`:

```go
type BalanceHandler struct {
    pool    *pgxpool.Pool
    offsets *postgres.OffsetStore
}

func (h *BalanceHandler) Handle(ctx context.Context, persistenceID string, event *anypb.Any, revision uint64, offset *egopb.EntityOffset) error {
    tx, err := h.pool.Begin(ctx)
    if err != nil {
        return err
    }
    defer tx.Rollback(ctx) // a no-op once committed

    if _, err := tx.Exec(ctx,
        "INSERT INTO account_balances (account_id, balance) VALUES ($1, $2) ON CONFLICT (account_id) DO UPDATE SET balance = EXCLUDED.balance",
        persistenceID, balanceOf(event)); err != nil {
        return err
    }

    if err := h.offsets.WriteOffsetsTx(ctx, tx, "balances", []*egopb.EntityOffset{offset}); err != nil {
        return err
    }

    return tx.Commit(ctx)
}
```

`WriteOffsetsTx` runs the statement of `WriteOffsets` on the caller's transaction and never commits it. The offsets tables must live in the database the handler writes to.

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

`go test ./...` runs the suite against PostgreSQL 16 started with Testcontainers, so Docker must be running. `go test -run '^$' -bench .` runs the `WriteEvents` and `EventsByShard` benchmarks.
