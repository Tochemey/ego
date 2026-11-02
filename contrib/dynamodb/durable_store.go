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

// Package dynamodb persists the durable states of eGo entities in Amazon
// DynamoDB.
//
// DurableStore implements persistence.StateStore on a *dynamodb.Client the
// application creates and owns. The table must exist before the engine starts,
// with the string attribute PersistenceID as its partition key and no sort key.
package dynamodb

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/tochemey/ego/v5/egopb"
	"github.com/tochemey/ego/v5/persistence"
)

// partitionKey is the name of the table's partition key attribute.
const partitionKey = "PersistenceID"

// DurableStore persists the latest durable state of every persistence ID as
// one item of a DynamoDB table, replaced on every write.
//
// Reads are strongly consistent, so an entity that recovers reads the latest
// state it wrote. A write or a deletion is a single request.
//
// A DurableStore is safe for concurrent use.
type DurableStore struct {
	// client sends the store's requests. The application owns it.
	client *dynamodb.Client
	// tableName is the table that holds the states.
	tableName *string
}

// enforce interface implementation
var _ persistence.StateStore = (*DurableStore)(nil)

// NewDurableStore creates a durable state store on the given table.
//
// The client stays owned by the caller, so the same client can serve the
// application's own requests.
func NewDurableStore(tableName string, client *dynamodb.Client) *DurableStore {
	return &DurableStore{
		client:    client,
		tableName: aws.String(tableName),
	}
}

// Connect verifies the table is reachable. The client needs no connection.
func (s *DurableStore) Connect(ctx context.Context) error {
	return s.Ping(ctx)
}

// Disconnect releases nothing: the client belongs to the application.
func (s *DurableStore) Disconnect(context.Context) error {
	return nil
}

// Ping verifies the table exists and DynamoDB answers.
func (s *DurableStore) Ping(ctx context.Context) error {
	if _, err := s.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: s.tableName}); err != nil {
		return fmt.Errorf("dynamodb: failed to reach the table: %w", err)
	}

	return nil
}

// WriteState persists the durable state of a persistence ID, replacing the
// state stored before.
func (s *DurableStore) WriteState(ctx context.Context, state *egopb.DurableState) error {
	resultingState := state.GetResultingState()
	return s.putItem(ctx, &stateItem{
		PersistenceID: state.GetPersistenceId(),
		VersionNumber: state.GetVersionNumber(),
		StatePayload:  resultingState.GetValue(),
		StateManifest: resultingState.GetTypeUrl(),
		Timestamp:     state.GetTimestamp(),
		ShardNumber:   state.GetShard(),
	})
}

// GetLatestState returns the durable state of the persistence ID, or nil when
// none is stored. A tombstone left by DeleteState comes back with its version
// and no resulting state.
func (s *DurableStore) GetLatestState(ctx context.Context, persistenceID string) (*egopb.DurableState, error) {
	output, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      s.tableName,
		Key:            itemKey(persistenceID),
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return nil, fmt.Errorf("dynamodb: failed to read the durable state: %w", err)
	}

	if output.Item == nil {
		return nil, nil
	}

	var item stateItem
	if err := attributevalue.UnmarshalMap(output.Item, &item); err != nil {
		return nil, fmt.Errorf("dynamodb: failed to decode the durable state: %w", err)
	}

	state := &egopb.DurableState{
		PersistenceId: item.PersistenceID,
		VersionNumber: item.VersionNumber,
		Timestamp:     item.Timestamp,
		Shard:         item.ShardNumber,
	}

	if item.StateManifest != "" {
		state.ResultingState = &anypb.Any{TypeUrl: item.StateManifest, Value: item.StatePayload}
	}

	return state, nil
}

// DeleteState deletes the durable state of the persistence ID.
//
// With a version greater than zero the item becomes a tombstone that keeps the
// version, so the versions of the persistence ID keep increasing after the
// deletion. With version zero the item is removed. A persistence ID with no
// state is not an error.
func (s *DurableStore) DeleteState(ctx context.Context, persistenceID string, version uint64) error {
	if version > 0 {
		return s.putItem(ctx, &stateItem{
			PersistenceID: persistenceID,
			VersionNumber: version,
			Timestamp:     time.Now().UnixNano(),
			ShardNumber:   persistence.ShardFor(persistenceID),
		})
	}

	_, err := s.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: s.tableName,
		Key:       itemKey(persistenceID),
	})
	if err != nil {
		return fmt.Errorf("dynamodb: failed to delete the durable state: %w", err)
	}

	return nil
}

// stateItem is the layout of a state item. A tombstone has no StatePayload
// and no StateManifest attribute.
type stateItem struct {
	// PersistenceID is the partition key.
	PersistenceID string `dynamodbav:"PersistenceID"`
	// VersionNumber is the version of the state.
	VersionNumber uint64 `dynamodbav:"VersionNumber"`
	// StatePayload is the Value of the state's anypb.Any.
	StatePayload []byte `dynamodbav:"StatePayload,omitempty"`
	// StateManifest is the TypeUrl of the state's anypb.Any.
	StateManifest string `dynamodbav:"StateManifest,omitempty"`
	// Timestamp is when the state was written, in Unix nanoseconds.
	Timestamp int64 `dynamodbav:"Timestamp"`
	// ShardNumber is the journal shard of the persistence ID.
	ShardNumber uint64 `dynamodbav:"ShardNumber"`
}

// putItem replaces the item of the persistence ID.
func (s *DurableStore) putItem(ctx context.Context, item *stateItem) error {
	attributes, err := attributevalue.MarshalMap(item)
	if err != nil {
		return fmt.Errorf("dynamodb: failed to encode the durable state: %w", err)
	}

	if _, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{TableName: s.tableName, Item: attributes}); err != nil {
		return fmt.Errorf("dynamodb: failed to write the durable state: %w", err)
	}

	return nil
}

// itemKey returns the primary key of the persistence ID's item.
func itemKey(persistenceID string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{partitionKey: &types.AttributeValueMemberS{Value: persistenceID}}
}
