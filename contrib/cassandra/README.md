# Cassandra durable state store

`github.com/tochemey/ego/contrib/cassandra` persists the durable states of [eGo](https://github.com/Tochemey/ego) entities in Apache Cassandra.
`DurableStore` implements `persistence.StateStore` on a `*gocql.Session` from `github.com/apache/cassandra-gocql-driver/v2`.

## Installation

```shell
go get github.com/tochemey/ego/contrib/cassandra
```

## Schema

Create the `states_store` table of [resources/durablestore_cassandra.cql](./resources/durablestore_cassandra.cql) in your keyspace before starting the engine:

```sql
CREATE TABLE IF NOT EXISTS states_store
(
    persistence_id text PRIMARY KEY,
    version_number bigint,
    state_payload  blob,
    state_manifest text,
    timestamp      bigint,
    shard_number   bigint
);
```

Every persistence ID has one row, replaced on each write.
The state is stored as the type URL (`state_manifest`) and the bytes (`state_payload`) of its `anypb.Any`, so reading it back needs no type registry.

## Usage

The application creates the session and owns it: the store never closes it, so the same session can serve your own queries too.

```go
cluster := gocql.NewCluster("10.0.0.1", "10.0.0.2", "10.0.0.3")
cluster.Keyspace = "accounts"
cluster.Consistency = gocql.LocalQuorum

session, err := cluster.CreateSession()
if err != nil {
    return err
}
defer session.Close() // after the engine has stopped

cfg := ego.NewConfig(nil, ego.WithStateStore(cassandra.NewDurableStore(session)))
```

Build the actor system and the engine from `cfg` as the [eGo readme](../../readme.md) shows.

Use `QUORUM` or `LOCAL_QUORUM` for both reads and writes, so an entity that recovers on another node reads the latest state it wrote.
Every statement is marked idempotent, because each one replaces or removes a whole row, so the driver may retry it.

`DeleteState` with a version greater than zero keeps a tombstone row with that version and no state, so the versions of the persistence ID keep increasing after the deletion; with version zero it removes the row.

## Testing

`go test ./...` starts Cassandra with Testcontainers, so Docker must be running.
