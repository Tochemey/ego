# Cassandra stores

`github.com/tochemey/ego/contrib/cassandra` persists the snapshots, durable states and projection offsets of [eGo](https://github.com/Tochemey/ego) in Apache Cassandra, on a `*gocql.Session` from `github.com/apache/cassandra-gocql-driver/v2`:

| Store | Implements | Tables |
|---|---|---|
| `SnapshotStore` | `persistence.SnapshotStore` | `snapshots_store` |
| `DurableStore` | `persistence.StateStore` | `states_store` |
| `OffsetStore` | `offsetstore.OffsetStore` | `offsets_store`, `offsets_by_entity`, `offsets_start` |

The module has no events store. Reading events by shard needs a second table keyed by shard, and Cassandra cannot write it and the journal in one write that also rejects an event stored already: conditional batches cover a single partition. Writing them separately would leave, after a crash between the two writes, an event in the journal that projections never read. Use the events store of another contrib module, such as `contrib/postgres`.

## Installation

```shell
go get github.com/tochemey/ego/contrib/cassandra
```

## Schema

Create the tables of the [resources](./resources) folder in your keyspace before starting the engine:

- [durablestore_cassandra.cql](./resources/durablestore_cassandra.cql): one row per persistence ID, replaced on each write.
- [snapshotstore_cassandra.cql](./resources/snapshotstore_cassandra.cql): one partition per persistence ID, the highest sequence number first.
- [offsetstore_cassandra.cql](./resources/offsetstore_cassandra.cql): the offsets of a projection on a shard in one partition, newest first (`offsets_store`), the same offsets by persistence ID (`offsets_by_entity`), and the start offset of each projection (`offsets_start`).

Every payload is stored as the type URL and the bytes of its `anypb.Any`, so reading it back needs no type registry.

## Usage

The application creates the session and owns it: the stores never close it, so the same session can serve all three stores and your own queries.

```go
cluster := gocql.NewCluster("10.0.0.1", "10.0.0.2", "10.0.0.3")
cluster.Keyspace = "accounts"
cluster.Consistency = gocql.LocalQuorum

session, err := cluster.CreateSession()
if err != nil {
    return err
}
defer session.Close() // after the engine has stopped

cfg := ego.NewConfig(eventsStore,
    ego.WithSnapshotStore(cassandra.NewSnapshotStore(session)),
    ego.WithStateStore(cassandra.NewDurableStore(session)),
    ego.WithOffsetStore(cassandra.NewOffsetStore(session)),
)
```

Build the actor system and the engine from `cfg` as the [eGo readme](../../readme.md) shows.

Use `QUORUM` or `LOCAL_QUORUM` for both reads and writes, so an entity or a projection that recovers on another node reads the latest state, snapshot or offset it wrote.

## Behavior and costs

- **Durable states:** `DeleteState` with a version greater than zero keeps a tombstone row with that version and no state, so the versions of the persistence ID keep increasing after the deletion; with version zero it removes the row.
- **Snapshots:** `DeleteSnapshots` removes a range of a partition with one statement.
- **Offset writes:** `WriteOffsets` writes logged batches of 50 offsets, oldest timestamp first, each into both offsets tables. Every batch is atomic. If a write fails or the process stops between two batches, the stored offsets are the oldest ones, so a projection's position on a shard (the newest timestamp it stored there) never passes an offset that was not stored, and no event is skipped. Rewriting an offset replaces its rows.
- **Offset reads:** `LatestOffsets` runs one small query per shard, 1024 per call, 32 at a time. `ResetOffset` and `DeleteOffset` run the same queries to find the partitions to delete.
- **Exactly-once projections:** Cassandra has no multi-statement transactions, so `OffsetStore` cannot write an offset in the transaction of a projection's effect and does not support `projection.TransactionalHandler`. Use a `projection.Handler`, which processes events at least once.
- **Retries:** every single statement is marked idempotent, since each one replaces or removes whole rows, so the driver may retry it.

## Testing

`go test ./...` starts Cassandra with Testcontainers, so Docker must be running. The tests run the eGo contract suites (`persistencetest`, `offsetstoretest`) against the three stores.
