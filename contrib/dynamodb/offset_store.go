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
	"cmp"
	"context"
	"fmt"
	"math"
	"slices"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/tochemey/ego/v5/egopb"
	"github.com/tochemey/ego/v5/offsetstore"
)

const (
	// offsetPartitionAttribute is the partition key of the offsets table.
	offsetPartitionAttribute = "Partition"
	// offsetKeyAttribute is the sort key of the offsets table.
	offsetKeyAttribute = "OffsetKey"
	// entityKeyAttribute is the sort key of the entity index: the length-prefixed
	// persistence ID followed by the fixed-width sequence number.
	entityKeyAttribute = "EntityKey"
	// consumedAtAttribute holds when an offset was consumed, in Unix nanoseconds.
	consumedAtAttribute = "ConsumedAt"
	// startOffsetAttribute holds the start offset of a projection.
	startOffsetAttribute = "StartOffset"
	// entityIndex is the local secondary index ReadEntityOffset queries.
	entityIndex = "entity_sequence"
	// offsetPartitionPrefix starts the partition of the offsets of a projection on a shard.
	offsetPartitionPrefix = "offsets:"
	// startPartitionPrefix starts the partition of the start offset of a projection.
	startPartitionPrefix = "start:"
	// startOffsetKey is the sort key of the start offset item of a projection.
	startOffsetKey = "start"
)

// OffsetStore implements offsetstore.OffsetStore on a DynamoDB table.
//
// The table's partition key is the string attribute Partition and its sort key
// the string attribute OffsetKey. Every projection has one partition per shard
// holding its offsets, sorted by timestamp, persistence ID and sequence number,
// and one partition holding its start offset. A local secondary index,
// entity_sequence, with the string attribute EntityKey as sort key and every
// attribute projected, serves ReadEntityOffset with one query. Every read is strongly consistent.
//
// WriteOffsets writes at most 100 offsets per transaction, the DynamoDB limit,
// and writes a larger set in several transactions, the oldest offsets first.
// Each transaction is atomic; when the writer stops between two transactions,
// the offsets stored are the oldest ones. A projection resumes a shard from the
// newest timestamp among its stored offsets, which therefore never passes an
// offset that is not stored, and no event is skipped: the events whose offsets
// are missing are delivered again.
//
// LatestOffsets runs one query per shard, persistence.NumberOfShards queries
// per call, and ResetOffset and DeleteOffset read and delete every shard of the
// projection. Timestamps must not be negative, so that their fixed-width form
// orders as they do.
//
// An OffsetStore is safe for concurrent use.
type OffsetStore struct {
	// client sends the store's requests. The application owns it.
	client *dynamodb.Client
	// tableName is the table that holds the offsets.
	tableName *string
}

// enforce interface implementation
var _ offsetstore.OffsetStore = (*OffsetStore)(nil)

// NewOffsetStore creates an offset store on the given table.
//
// The client stays owned by the caller, so the same client can serve the
// application's own requests.
func NewOffsetStore(tableName string, client *dynamodb.Client) *OffsetStore {
	return &OffsetStore{
		client:    client,
		tableName: aws.String(tableName),
	}
}

// Connect verifies the table is reachable. The client needs no connection.
func (s *OffsetStore) Connect(ctx context.Context) error {
	return s.Ping(ctx)
}

// Disconnect releases nothing: the client belongs to the application.
func (s *OffsetStore) Disconnect(context.Context) error {
	return nil
}

// Ping verifies the table exists and DynamoDB answers.
func (s *OffsetStore) Ping(ctx context.Context) error {
	return ping(ctx, s.client, s.tableName)
}

// WriteOffsets stores the offsets of the projection, the oldest first, in
// transactions of at most 100 offsets. An offset stored already is stored once.
func (s *OffsetStore) WriteOffsets(ctx context.Context, projectionName string, offsets []*egopb.EntityOffset) error {
	items := s.TransactWriteItems(projectionName, offsets)
	for start := 0; start < len(items); start += transactionItemLimit {
		chunk := items[start:min(start+transactionItemLimit, len(items))]
		if _, err := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: chunk}); err != nil {
			return fmt.Errorf("dynamodb: failed to write the offsets: %w", err)
		}
	}

	return nil
}

// TransactWriteItems returns the items that store the offsets of the
// projection, the oldest first and each offset once.
//
// A projection.TransactionalHandler adds them to the TransactWriteItems
// request that applies the event, so the effect and the offset commit
// together. The request must run on the account and region of the store's
// table and holds at most 100 items in all.
func (s *OffsetStore) TransactWriteItems(projectionName string, offsets []*egopb.EntityOffset) []types.TransactWriteItem {
	sorted := slices.Clone(offsets)
	slices.SortStableFunc(sorted, func(left, right *egopb.EntityOffset) int {
		return cmp.Compare(left.GetTimestamp(), right.GetTimestamp())
	})

	seen := make(map[string]struct{}, len(sorted))
	items := make([]types.TransactWriteItem, 0, len(sorted))
	for _, offset := range sorted {
		key := offsetPartition(projectionName, offset.GetShardNumber()) + offsetKey(offset)
		if _, duplicate := seen[key]; duplicate {
			continue
		}

		seen[key] = struct{}{}
		items = append(items, types.TransactWriteItem{Put: &types.Put{TableName: s.tableName, Item: offsetAttributes(projectionName, offset)}})
	}

	return items
}

// ReadOffsets returns at most limit offsets of the projection on the shard,
// the newest timestamp first.
func (s *OffsetStore) ReadOffsets(ctx context.Context, projectionName string, shardNumber uint64, limit uint64) ([]*egopb.EntityOffset, error) {
	items, err := queryItems(ctx, s.client, &dynamodb.QueryInput{
		TableName:                 s.tableName,
		KeyConditionExpression:    aws.String("#partition = :partition"),
		ExpressionAttributeNames:  map[string]string{"#partition": offsetPartitionAttribute},
		ExpressionAttributeValues: map[string]types.AttributeValue{":partition": stringValue(offsetPartition(projectionName, shardNumber))},
		ScanIndexForward:          aws.Bool(false),
		ConsistentRead:            aws.Bool(true),
	}, limit)
	if err != nil {
		return nil, fmt.Errorf("dynamodb: failed to read the offsets: %w", err)
	}

	return decodeOffsets(items)
}

// ReadEntityOffset returns the offset of the persistence ID with the highest
// sequence number, or nil when the projection has none.
func (s *OffsetStore) ReadEntityOffset(ctx context.Context, projectionName string, shardNumber uint64, persistenceID string) (*egopb.EntityOffset, error) {
	items, err := queryItems(ctx, s.client, &dynamodb.QueryInput{
		TableName:                s.tableName,
		IndexName:                aws.String(entityIndex),
		KeyConditionExpression:   aws.String("#partition = :partition AND begins_with(#entity, :persistenceID)"),
		ExpressionAttributeNames: map[string]string{"#partition": offsetPartitionAttribute, "#entity": entityKeyAttribute},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":partition":     stringValue(offsetPartition(projectionName, shardNumber)),
			":persistenceID": stringValue(lengthPrefixed(persistenceID)),
		},
		ScanIndexForward: aws.Bool(false),
		ConsistentRead:   aws.Bool(true),
	}, 1)
	if err != nil {
		return nil, fmt.Errorf("dynamodb: failed to read the entity offset: %w", err)
	}

	offsets, err := decodeOffsets(items)
	if err != nil || len(offsets) == 0 {
		return nil, err
	}

	return offsets[0], nil
}

// LatestOffsets returns, for every shard the projection has an offset on, the
// newest timestamp among its offsets on that shard, with one query per shard.
func (s *OffsetStore) LatestOffsets(ctx context.Context, projectionName string) (map[uint64]int64, error) {
	latest := &shardTimestamps{timestamps: make(map[uint64]int64)}
	err := forEachShard(ctx, func(ctx context.Context, shardNumber uint64) error {
		items, err := queryItems(ctx, s.client, &dynamodb.QueryInput{
			TableName:                 s.tableName,
			KeyConditionExpression:    aws.String("#partition = :partition"),
			ProjectionExpression:      aws.String("#timestamp"),
			ExpressionAttributeNames:  map[string]string{"#partition": offsetPartitionAttribute, "#timestamp": timestampAttribute},
			ExpressionAttributeValues: map[string]types.AttributeValue{":partition": stringValue(offsetPartition(projectionName, shardNumber))},
			ScanIndexForward:          aws.Bool(false),
			ConsistentRead:            aws.Bool(true),
		}, 1)
		if err != nil || len(items) == 0 {
			return err
		}

		var newest timestampItem
		if err := attributevalue.UnmarshalMap(items[0], &newest); err != nil {
			return err
		}

		latest.set(shardNumber, newest.Timestamp)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("dynamodb: failed to read the latest offsets: %w", err)
	}

	return latest.timestamps, nil
}

// DeleteOffsetsBefore deletes the offsets of the projection on the shard whose
// timestamp is before before and that were consumed before consumedBefore,
// both in Unix nanoseconds.
func (s *OffsetStore) DeleteOffsetsBefore(ctx context.Context, projectionName string, shardNumber uint64, before, consumedBefore int64) error {
	keys, err := queryItems(ctx, s.client, &dynamodb.QueryInput{
		TableName:              s.tableName,
		KeyConditionExpression: aws.String("#partition = :partition AND #key < :before"),
		FilterExpression:       aws.String("#consumedAt < :consumedBefore"),
		ProjectionExpression:   aws.String("#partition, #key"),
		ExpressionAttributeNames: map[string]string{
			"#partition":  offsetPartitionAttribute,
			"#key":        offsetKeyAttribute,
			"#consumedAt": consumedAtAttribute,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":partition":      stringValue(offsetPartition(projectionName, shardNumber)),
			":before":         stringValue(fixedWidth(before)),
			":consumedBefore": signedValue(consumedBefore),
		},
		ConsistentRead: aws.Bool(true),
	}, math.MaxUint64)
	if err == nil {
		err = deleteItems(ctx, s.client, *s.tableName, keys)
	}

	if err != nil {
		return fmt.Errorf("dynamodb: failed to delete the offsets: %w", err)
	}

	return nil
}

// ResetOffset deletes every offset of the projection, then records value, a
// timestamp in Unix nanoseconds, as the point the projection starts from on
// every shard. A projection stopped in between has no offset and its former
// start offset, so it only processes events again.
func (s *OffsetStore) ResetOffset(ctx context.Context, projectionName string, value int64) error {
	if err := s.deleteOffsets(ctx, projectionName); err != nil {
		return fmt.Errorf("dynamodb: failed to reset the offsets: %w", err)
	}

	_, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: s.tableName,
		Item: map[string]types.AttributeValue{
			offsetPartitionAttribute: stringValue(startPartition(projectionName)),
			offsetKeyAttribute:       stringValue(startOffsetKey),
			startOffsetAttribute:     signedValue(value),
		},
	})
	if err != nil {
		return fmt.Errorf("dynamodb: failed to reset the offsets: %w", err)
	}

	return nil
}

// StartOffset returns the timestamp the latest ResetOffset recorded for the
// projection, or zero when there is none.
func (s *OffsetStore) StartOffset(ctx context.Context, projectionName string) (int64, error) {
	output, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      s.tableName,
		Key:            startOffsetItemKey(projectionName),
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return 0, fmt.Errorf("dynamodb: failed to read the start offset: %w", err)
	}

	var start startOffsetItem
	if err := attributevalue.UnmarshalMap(output.Item, &start); err != nil {
		return 0, fmt.Errorf("dynamodb: failed to decode the start offset: %w", err)
	}

	return start.StartOffset, nil
}

// DeleteOffset deletes every offset and the start offset of the projection. A
// projection with no offset is not an error.
func (s *OffsetStore) DeleteOffset(ctx context.Context, projectionName string) error {
	err := s.deleteOffsets(ctx, projectionName)
	if err == nil {
		_, err = s.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
			TableName: s.tableName,
			Key:       startOffsetItemKey(projectionName),
		})
	}

	if err != nil {
		return fmt.Errorf("dynamodb: failed to delete the offsets: %w", err)
	}

	return nil
}

// deleteOffsets deletes the offsets of the projection on every shard.
func (s *OffsetStore) deleteOffsets(ctx context.Context, projectionName string) error {
	return forEachShard(ctx, func(ctx context.Context, shardNumber uint64) error {
		keys, err := queryItems(ctx, s.client, &dynamodb.QueryInput{
			TableName:                 s.tableName,
			KeyConditionExpression:    aws.String("#partition = :partition"),
			ProjectionExpression:      aws.String("#partition, #key"),
			ExpressionAttributeNames:  map[string]string{"#partition": offsetPartitionAttribute, "#key": offsetKeyAttribute},
			ExpressionAttributeValues: map[string]types.AttributeValue{":partition": stringValue(offsetPartition(projectionName, shardNumber))},
			ConsistentRead:            aws.Bool(true),
		}, math.MaxUint64)
		if err != nil {
			return err
		}

		return deleteItems(ctx, s.client, *s.tableName, keys)
	})
}

// offsetItem is the layout of an offset item.
type offsetItem struct {
	// ShardNumber is the journal shard of the offset.
	ShardNumber uint64 `dynamodbav:"ShardNumber"`
	// PersistenceID is the persistence ID the offset belongs to.
	PersistenceID string `dynamodbav:"PersistenceID"`
	// SequenceNumber is the sequence number of the event the offset records.
	SequenceNumber uint64 `dynamodbav:"SequenceNumber"`
	// Timestamp is the timestamp of the event the offset records, in Unix nanoseconds.
	Timestamp int64 `dynamodbav:"Timestamp"`
	// ConsumedAt is when the projection consumed the event, in Unix nanoseconds.
	ConsumedAt int64 `dynamodbav:"ConsumedAt"`
}

// startOffsetItem is the layout of the start offset item of a projection.
type startOffsetItem struct {
	// StartOffset is the timestamp the projection starts from, in Unix nanoseconds.
	StartOffset int64 `dynamodbav:"StartOffset"`
}

// offsetAttributes returns the attributes of the item of an offset of the projection.
func offsetAttributes(projectionName string, offset *egopb.EntityOffset) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		offsetPartitionAttribute: stringValue(offsetPartition(projectionName, offset.GetShardNumber())),
		offsetKeyAttribute:       stringValue(offsetKey(offset)),
		entityKeyAttribute:       stringValue(entityKey(offset)),
		shardNumberAttribute:     unsignedValue(offset.GetShardNumber()),
		partitionKey:             stringValue(offset.GetPersistenceId()),
		sequenceNumberAttribute:  unsignedValue(offset.GetSequenceNumber()),
		timestampAttribute:       signedValue(offset.GetTimestamp()),
		consumedAtAttribute:      signedValue(offset.GetConsumedAt()),
	}
}

// decodeOffsets decodes offset items into offsets.
func decodeOffsets(items []map[string]types.AttributeValue) ([]*egopb.EntityOffset, error) {
	var decoded []offsetItem
	if err := attributevalue.UnmarshalListOfMaps(items, &decoded); err != nil {
		return nil, fmt.Errorf("dynamodb: failed to decode the offsets: %w", err)
	}

	offsets := make([]*egopb.EntityOffset, len(decoded))
	for index, item := range decoded {
		offsets[index] = &egopb.EntityOffset{
			ShardNumber:    item.ShardNumber,
			PersistenceId:  item.PersistenceID,
			SequenceNumber: item.SequenceNumber,
			Timestamp:      item.Timestamp,
			ConsumedAt:     item.ConsumedAt,
		}
	}

	return offsets, nil
}

// offsetKey returns the sort key of an offset: its fixed-width timestamp
// followed by its entity key, so offsets sort by timestamp, persistence ID and
// sequence number.
func offsetKey(offset *egopb.EntityOffset) string {
	return fixedWidth(offset.GetTimestamp()) + entityKey(offset)
}

// entityKey returns the entity index sort key of an offset: its
// length-prefixed persistence ID followed by its fixed-width sequence number.
func entityKey(offset *egopb.EntityOffset) string {
	return lengthPrefixed(offset.GetPersistenceId()) + fixedWidth(offset.GetSequenceNumber())
}

// offsetPartition returns the partition of the offsets of the projection on the shard.
func offsetPartition(projectionName string, shardNumber uint64) string {
	return offsetPartitionPrefix + lengthPrefixed(projectionName) + ":" + strconv.FormatUint(shardNumber, 10)
}

// startPartition returns the partition of the start offset of the projection.
func startPartition(projectionName string) string {
	return startPartitionPrefix + lengthPrefixed(projectionName)
}

// startOffsetItemKey returns the primary key of the start offset item of the projection.
func startOffsetItemKey(projectionName string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		offsetPartitionAttribute: stringValue(startPartition(projectionName)),
		offsetKeyAttribute:       stringValue(startOffsetKey),
	}
}
