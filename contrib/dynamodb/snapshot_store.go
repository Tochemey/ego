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

package dynamodb

import (
	"context"
	"fmt"
	"math"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/tochemey/ego/v5/egopb"
	"github.com/tochemey/ego/v5/persistence"
)

const (
	// statePayloadAttribute holds the Value of the snapshot's anypb.Any.
	statePayloadAttribute = "StatePayload"
	// stateManifestAttribute holds the TypeUrl of the snapshot's anypb.Any.
	stateManifestAttribute = "StateManifest"
)

// SnapshotStore implements persistence.SnapshotStore on a DynamoDB table.
//
// The table's partition key is the string attribute PersistenceID and its sort
// key the number attribute SequenceNumber: one item per snapshot. The latest
// snapshot is read with a strongly consistent query, so an entity that
// recovers reads the latest snapshot it wrote. Writing a snapshot at a
// sequence number stored already replaces it.
//
// A SnapshotStore is safe for concurrent use.
type SnapshotStore struct {
	// client sends the store's requests. The application owns it.
	client *dynamodb.Client
	// tableName is the table that holds the snapshots.
	tableName *string
}

// enforce interface implementation
var _ persistence.SnapshotStore = (*SnapshotStore)(nil)

// NewSnapshotStore creates a snapshot store on the given table.
//
// The client stays owned by the caller, so the same client can serve the
// application's own requests.
func NewSnapshotStore(tableName string, client *dynamodb.Client) *SnapshotStore {
	return &SnapshotStore{
		client:    client,
		tableName: aws.String(tableName),
	}
}

// Connect verifies the table is reachable. The client needs no connection.
func (s *SnapshotStore) Connect(ctx context.Context) error {
	return s.Ping(ctx)
}

// Disconnect releases nothing: the client belongs to the application.
func (s *SnapshotStore) Disconnect(context.Context) error {
	return nil
}

// Ping verifies the table exists and DynamoDB answers.
func (s *SnapshotStore) Ping(ctx context.Context) error {
	return ping(ctx, s.client, s.tableName)
}

// WriteSnapshot stores the snapshot, replacing one stored at the same sequence number.
func (s *SnapshotStore) WriteSnapshot(ctx context.Context, snapshot *egopb.Snapshot) error {
	payload := snapshot.GetState().GetValue()
	if payload == nil {
		payload = []byte{}
	}

	_, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: s.tableName,
		Item: map[string]types.AttributeValue{
			partitionKey:             stringValue(snapshot.GetPersistenceId()),
			sequenceNumberAttribute:  unsignedValue(snapshot.GetSequenceNumber()),
			statePayloadAttribute:    &types.AttributeValueMemberB{Value: payload},
			stateManifestAttribute:   stringValue(snapshot.GetState().GetTypeUrl()),
			timestampAttribute:       signedValue(snapshot.GetTimestamp()),
			encryptionKeyIDAttribute: stringValue(snapshot.GetEncryptionKeyId()),
			isEncryptedAttribute:     boolValue(snapshot.GetIsEncrypted()),
		},
	})
	if err != nil {
		return fmt.Errorf("dynamodb: failed to write the snapshot: %w", err)
	}

	return nil
}

// GetLatestSnapshot returns the snapshot of the persistence ID with the
// highest sequence number, or nil when it has none.
func (s *SnapshotStore) GetLatestSnapshot(ctx context.Context, persistenceID string) (*egopb.Snapshot, error) {
	items, err := queryItems(ctx, s.client, &dynamodb.QueryInput{
		TableName:                 s.tableName,
		KeyConditionExpression:    aws.String("#persistenceID = :persistenceID"),
		ExpressionAttributeNames:  map[string]string{"#persistenceID": partitionKey},
		ExpressionAttributeValues: map[string]types.AttributeValue{":persistenceID": stringValue(persistenceID)},
		ScanIndexForward:          aws.Bool(false),
		ConsistentRead:            aws.Bool(true),
	}, 1)
	if err != nil {
		return nil, fmt.Errorf("dynamodb: failed to read the latest snapshot: %w", err)
	}

	if len(items) == 0 {
		return nil, nil
	}

	var item snapshotItem
	if err := attributevalue.UnmarshalMap(items[0], &item); err != nil {
		return nil, fmt.Errorf("dynamodb: failed to decode the snapshot: %w", err)
	}

	return &egopb.Snapshot{
		PersistenceId:   item.PersistenceID,
		SequenceNumber:  item.SequenceNumber,
		State:           &anypb.Any{TypeUrl: item.StateManifest, Value: item.StatePayload},
		Timestamp:       item.Timestamp,
		EncryptionKeyId: item.EncryptionKeyID,
		IsEncrypted:     item.IsEncrypted,
	}, nil
}

// DeleteSnapshots deletes the snapshots of the persistence ID up to the given
// sequence number, inclusive.
func (s *SnapshotStore) DeleteSnapshots(ctx context.Context, persistenceID string, toSequenceNumber uint64) error {
	keys, err := queryItems(ctx, s.client, &dynamodb.QueryInput{
		TableName:                s.tableName,
		KeyConditionExpression:   aws.String("#persistenceID = :persistenceID AND #sequence <= :to"),
		ProjectionExpression:     aws.String("#persistenceID, #sequence"),
		ExpressionAttributeNames: map[string]string{"#persistenceID": partitionKey, "#sequence": sequenceNumberAttribute},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":persistenceID": stringValue(persistenceID),
			":to":            unsignedValue(toSequenceNumber),
		},
		ConsistentRead: aws.Bool(true),
	}, math.MaxUint64)
	if err == nil {
		err = deleteItems(ctx, s.client, *s.tableName, keys)
	}

	if err != nil {
		return fmt.Errorf("dynamodb: failed to delete the snapshots: %w", err)
	}

	return nil
}

// snapshotItem is the layout of a snapshot item.
type snapshotItem struct {
	// PersistenceID is the partition key.
	PersistenceID string `dynamodbav:"PersistenceID"`
	// SequenceNumber is the sort key.
	SequenceNumber uint64 `dynamodbav:"SequenceNumber"`
	// StatePayload is the Value of the snapshot's anypb.Any.
	StatePayload []byte `dynamodbav:"StatePayload"`
	// StateManifest is the TypeUrl of the snapshot's anypb.Any.
	StateManifest string `dynamodbav:"StateManifest"`
	// Timestamp is the timestamp of the snapshot, in Unix nanoseconds.
	Timestamp int64 `dynamodbav:"Timestamp"`
	// EncryptionKeyID identifies the key the snapshot is encrypted with.
	EncryptionKeyID string `dynamodbav:"EncryptionKeyID"`
	// IsEncrypted is the encryption flag of the snapshot.
	IsEncrypted bool `dynamodbav:"IsEncrypted"`
}
