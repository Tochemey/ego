# DynamoDB durable state store

`github.com/tochemey/ego/contrib/dynamodb` persists the durable states of [eGo](https://github.com/Tochemey/ego) entities in Amazon DynamoDB.
`DurableStore` implements `persistence.StateStore` on a `*dynamodb.Client` from `github.com/aws/aws-sdk-go-v2/service/dynamodb`.

## Installation

```shell
go get github.com/tochemey/ego/contrib/dynamodb
```

## Table

Create the table before starting the engine. Its partition key is the string attribute `PersistenceID`, with no sort key:

```shell
aws dynamodb create-table \
    --table-name states_store \
    --attribute-definitions AttributeName=PersistenceID,AttributeType=S \
    --key-schema AttributeName=PersistenceID,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST
```

Every persistence ID has one item, replaced on each write:

| Attribute | Type | Content |
|---|---|---|
| `PersistenceID` | S | partition key |
| `VersionNumber` | N | version of the state |
| `StatePayload` | B | bytes of the state's `anypb.Any` |
| `StateManifest` | S | type URL of the state's `anypb.Any` |
| `Timestamp` | N | write time, Unix nanoseconds |
| `ShardNumber` | N | journal shard of the persistence ID |

Storing the type URL and the bytes of the `anypb.Any` means reading a state back needs no type registry.

## Usage

The application creates the client and owns it, so the same client can serve your own requests too:

```go
awsConfig, err := config.LoadDefaultConfig(ctx)
if err != nil {
    return err
}

client := dynamodb.NewFromConfig(awsConfig)
cfg := ego.NewConfig(nil, ego.WithStateStore(dynamodb.NewDurableStore("states_store", client)))
```

Build the actor system and the engine from `cfg` as the [eGo readme](../../readme.md) shows.

Every read is strongly consistent, so an entity that recovers reads the latest state it wrote, and every write or deletion is a single request.
`Connect` and `Ping` describe the table, so a missing table fails at startup.

`DeleteState` with a version greater than zero keeps a tombstone item with that version and no `StatePayload` or `StateManifest`, so the versions of the persistence ID keep increasing after the deletion; with version zero it removes the item.

## Testing

`go test ./...` starts DynamoDB Local with Testcontainers, so Docker must be running.
