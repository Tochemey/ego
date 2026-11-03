# DynamoDB stores

`github.com/tochemey/ego/contrib/dynamodb` implements the [eGo](https://github.com/Tochemey/ego) stores on Amazon DynamoDB with `github.com/aws/aws-sdk-go-v2/service/dynamodb`:

| Store | Constructor | Interface |
|---|---|---|
| Events | `NewEventsStore(table, client)` | `persistence.EventsStore` |
| Snapshots | `NewSnapshotStore(table, client)` | `persistence.SnapshotStore` |
| Durable state | `NewDurableStore(table, client)` | `persistence.StateStore` |
| Offsets | `NewOffsetStore(table, client)` | `offsetstore.OffsetStore` |

## Installation

```shell
go get github.com/tochemey/ego/contrib/dynamodb
```

## Usage

The application creates the client and owns it: the stores never close it, so the same client serves every store and your own requests.

```go
awsConfig, err := config.LoadDefaultConfig(ctx)
if err != nil {
    return err
}

client := dynamodb.NewFromConfig(awsConfig)
cfg := ego.NewConfig(dynamodb.NewEventsStore("events_store", client),
    ego.WithSnapshotStore(dynamodb.NewSnapshotStore("snapshots_store", client)),
    ego.WithStateStore(dynamodb.NewDurableStore("states_store", client)),
    ego.WithOffsetStore(dynamodb.NewOffsetStore("offsets_store", client)),
)
```

Build the actor system and the engine from `cfg` as the [eGo readme](../../readme.md) shows.
`Connect` and `Ping` describe the table, so a missing table fails at startup.

Payloads are stored as the type URL and the bytes of their `anypb.Any`, so reading one back needs no type registry.

## Tables

Create the tables before starting the engine. The names are yours; the key schemas and the index names are fixed.

### Events

```shell
aws dynamodb create-table \
    --table-name events_store \
    --attribute-definitions \
        AttributeName=PersistenceID,AttributeType=S \
        AttributeName=SequenceNumber,AttributeType=N \
        AttributeName=ShardNumber,AttributeType=N \
        AttributeName=ShardKey,AttributeType=S \
        AttributeName=MarkerShard,AttributeType=N \
    --key-schema AttributeName=PersistenceID,KeyType=HASH AttributeName=SequenceNumber,KeyType=RANGE \
    --global-secondary-indexes \
        'IndexName=shard_timestamp,KeySchema=[{AttributeName=ShardNumber,KeyType=HASH},{AttributeName=ShardKey,KeyType=RANGE}],Projection={ProjectionType=ALL}' \
        'IndexName=persistence_ids,KeySchema=[{AttributeName=MarkerShard,KeyType=HASH},{AttributeName=PersistenceID,KeyType=RANGE}],Projection={ProjectionType=KEYS_ONLY}' \
    --billing-mode PAY_PER_REQUEST
```

- Every event is one item keyed by persistence ID and sequence number. Replays read it with strongly consistent queries.
- `shard_timestamp` serves `EventsByShard` and `ShardOffsets`. Its sort key `ShardKey` is the fixed-width timestamp and sequence number of the event followed by its persistence ID, so a shard reads in the order the contract requires. DynamoDB updates the index eventually. A projection reads its shards again two minutes behind its position, so it delivers an event the index exposes up to two minutes after newer ones. Keep the index's write capacity at least the table's (on-demand tables scale it automatically), so index writes are never throttled long enough to fall further behind.
- `persistence_ids` serves `PersistenceIDs`. Every write also stores one marker item per persistence ID (sequence number 0), the only items carrying `MarkerShard`, spread over the 1024 journal shards. A persistence ID whose events are all deleted stays listed.

### Snapshots

```shell
aws dynamodb create-table \
    --table-name snapshots_store \
    --attribute-definitions AttributeName=PersistenceID,AttributeType=S AttributeName=SequenceNumber,AttributeType=N \
    --key-schema AttributeName=PersistenceID,KeyType=HASH AttributeName=SequenceNumber,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST
```

### Durable state

```shell
aws dynamodb create-table \
    --table-name states_store \
    --attribute-definitions AttributeName=PersistenceID,AttributeType=S \
    --key-schema AttributeName=PersistenceID,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST
```

Every persistence ID has one item, replaced on each write. `DeleteState` with a version greater than zero keeps a tombstone item with that version and no state; with version zero it removes the item.

### Offsets

```shell
aws dynamodb create-table \
    --table-name offsets_store \
    --attribute-definitions \
        AttributeName=Partition,AttributeType=S \
        AttributeName=OffsetKey,AttributeType=S \
        AttributeName=EntityKey,AttributeType=S \
    --key-schema AttributeName=Partition,KeyType=HASH AttributeName=OffsetKey,KeyType=RANGE \
    --local-secondary-indexes \
        'IndexName=entity_sequence,KeySchema=[{AttributeName=Partition,KeyType=HASH},{AttributeName=EntityKey,KeyType=RANGE}],Projection={ProjectionType=ALL}' \
    --billing-mode PAY_PER_REQUEST
```

A projection has one partition per shard holding its offsets, sorted by timestamp, persistence ID and sequence number, and one partition holding its start offset. `entity_sequence` serves `ReadEntityOffset` with one query. Every read is strongly consistent.

## Limits and costs

| Operation | Behaviour |
|---|---|
| `WriteEvents` | One transaction, so atomic, and it fails when an event is stored already. A transaction holds at most 100 items, the events plus one marker per persistence ID: a write holds at most 99 events of one persistence ID, and a larger one fails with `ErrTooManyEvents`. |
| `WriteOffsets` | At most 100 offsets per transaction; a larger set is written in several transactions, the oldest offsets first. A projection resumes a shard from the newest timestamp among its stored offsets, so a stop between two transactions never moves it past an offset that is not stored: no event is skipped, and the events whose offsets are missing are delivered again. |
| `ShardOffsets`, `LatestOffsets` | One query per shard: 1024 small queries per call, whatever the size of the journal. Projections call `ShardOffsets` on every pull, so the pull interval sets that cost. |
| `ResetOffset`, `DeleteOffset` | Read and delete the offsets of the projection on every shard. |

Timestamps must not be negative, so that their fixed-width form orders as they do.

## Exactly-once projections

`OffsetStore.TransactWriteItems(projectionName, offsets)` returns the items that store the offsets. A `projection.TransactionalHandler` adds them to the `TransactWriteItems` request that applies the event, so the effect and the offset commit together:

```go
func (h *Handler) Handle(ctx context.Context, persistenceID string, event *anypb.Any, revision uint64, offset *egopb.EntityOffset) error {
    items := append(h.effectItems(persistenceID, event), h.offsets.TransactWriteItems(h.projectionName, []*egopb.EntityOffset{offset})...)
    _, err := h.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items})
    return err
}
```

The request holds at most 100 items in all.

## Testing

`go test ./...` starts DynamoDB Local with Testcontainers, so Docker must be running.
The stores run the eGo contract suites, `persistencetest` and `offsetstoretest`, plus their DynamoDB-specific tests.
