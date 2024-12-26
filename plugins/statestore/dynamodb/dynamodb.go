package dynamodb

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/tochemey/ego/v3/egopb"
	"github.com/tochemey/ego/v3/persistence"
	"google.golang.org/protobuf/proto"
)

// No sort key is needed because we are only storing the latest state
type StateItem struct {
	PersistenceID string // Partition key
	VersionNumber uint64
	StatePayload  []byte
	StateManifest string
	Timestamp     int64
	ShardNumber   uint64
}

const (
	tableName = "states_store"
)

// DynamoDurableStore implements the DurableStore interface
// and helps persist states in a DynamoDB
type DynamoDurableStore struct {
	client *dynamodb.Client
}

// enforce interface implementation
var _ persistence.StateStore = (*DynamoDurableStore)(nil)

func NewStateStore() *DynamoDurableStore {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil
	}

	return &DynamoDurableStore{
		client: dynamodb.NewFromConfig(cfg),
	}
}

// Connect connects to the journal store
// No connection is needed because the client is stateless
func (d DynamoDurableStore) Connect(ctx context.Context) error {
	return nil
}

// Disconnect disconnect the journal store
// There is no need to disconnect because the client is stateless
func (DynamoDurableStore) Disconnect(ctx context.Context) error {
	return nil
}

// Ping verifies a connection to the database is still alive, establishing a connection if necessary.
// There is no need to ping because the client is stateless
func (d DynamoDurableStore) Ping(ctx context.Context) error {
	_, err := d.client.ListTables(ctx, &dynamodb.ListTablesInput{})
	if err != nil {
		return fmt.Errorf("failed to fetch tables in the dynamodb: %w", err)
	}
	return nil
}

// WriteState persist durable state for a given persistenceID.
func (d DynamoDurableStore) WriteState(ctx context.Context, state *egopb.DurableState) error {

	bytea, _ := proto.Marshal(state.GetResultingState())
	manifest := string(state.GetResultingState().ProtoReflect().Descriptor().FullName())

	// Define the item to upsert
	item := map[string]types.AttributeValue{
		"PersistenceID": &types.AttributeValueMemberS{Value: state.GetPersistenceId()}, // Partition key
		"StatePayload":  &types.AttributeValueMemberB{Value: bytea},
		"StateManifest": &types.AttributeValueMemberS{Value: manifest},
		"Timestamp":     &types.AttributeValueMemberS{Value: string(state.GetTimestamp())},
	}

	_, err := d.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item:      item,
	})
	if err != nil {
		return fmt.Errorf("failed to upsert state into the dynamodb: %w", err)
	}

	return nil
}

// GetLatestState fetches the latest durable state
func (d DynamoDurableStore) GetLatestState(ctx context.Context, persistenceID string) (*egopb.DurableState, error) {
	// Get criteria
	key := map[string]types.AttributeValue{
		"PersistenceID": &types.AttributeValueMemberS{Value: persistenceID},
	}

	// Perform the GetItem operation
	resp, err := d.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key:       key,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch the latest state from the dynamodb: %w", err)
	}

	// Check if item exists
	if resp.Item == nil {
		return nil, nil
	}

	item := &StateItem{
		PersistenceID: persistenceID,
		VersionNumber: parseDynamoUint64(resp.Item["VersionNumber"]),
		StatePayload:  resp.Item["StatePayload"].(*types.AttributeValueMemberB).Value,
		StateManifest: resp.Item["StateManifest"].(*types.AttributeValueMemberS).Value,
		Timestamp:     parseDynamoInt64(resp.Item["Timestamp"]),
		ShardNumber:   parseDynamoUint64(resp.Item["ShardNumber"]),
	}

	// unmarshal the event and the state
	state, err := toProto(item.StateManifest, item.StatePayload)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal the durable state: %w", err)
	}

	return &egopb.DurableState{
		PersistenceId:  persistenceID,
		VersionNumber:  item.VersionNumber,
		ResultingState: state,
		Timestamp:      item.Timestamp,
		Shard:          item.ShardNumber,
	}, nil
}
