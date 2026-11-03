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
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/tochemey/ego/v5/egopb"
	"github.com/tochemey/ego/v5/persistence"
)

const (
	// sequenceNumberAttribute is the sort key of the events and snapshots tables.
	sequenceNumberAttribute = "SequenceNumber"
	// shardNumberAttribute holds the journal shard of an event.
	shardNumberAttribute = "ShardNumber"
	// shardKeyAttribute is the sort key of the shard index: the timestamp and the sequence
	// number of the event, fixed-width, followed by its persistence ID.
	shardKeyAttribute = "ShardKey"
	// timestampAttribute holds the timestamp of an event, a snapshot or an offset.
	timestampAttribute = "Timestamp"
	// eventPayloadAttribute holds the Value of the event's anypb.Any.
	eventPayloadAttribute = "EventPayload"
	// eventManifestAttribute holds the TypeUrl of the event's anypb.Any.
	eventManifestAttribute = "EventManifest"
	// isDeletedAttribute holds the deletion flag of an event.
	isDeletedAttribute = "IsDeleted"
	// encryptionKeyIDAttribute holds the identifier of the key an event or a snapshot is encrypted with.
	encryptionKeyIDAttribute = "EncryptionKeyID"
	// isEncryptedAttribute holds the encryption flag of an event or a snapshot.
	isEncryptedAttribute = "IsEncrypted"
	// markerShardAttribute is the partition key of the persistence IDs index. Only the marker
	// items carry it, so the index holds one item per persistence ID.
	markerShardAttribute = "MarkerShard"
	// shardIndex is the global secondary index the shard reads query.
	shardIndex = "shard_timestamp"
	// persistenceIDsIndex is the global secondary index PersistenceIDs queries.
	persistenceIDsIndex = "persistence_ids"
	// markerSequenceNumber is the sort key of the marker item of a persistence ID. Events
	// start at sequence number 1, so the marker is never read as an event.
	markerSequenceNumber uint64 = 0
	// firstSequenceNumber is the lowest sequence number of an event.
	firstSequenceNumber uint64 = 1
	// pageTokenSeparator separates the shard and the persistence ID in a page token.
	pageTokenSeparator = "/"
)

// ErrTooManyEvents is returned by WriteEvents when a write holds more items than
// one DynamoDB transaction accepts: its events plus one marker item per
// persistence ID, so at most 99 events of one persistence ID.
var ErrTooManyEvents = errors.New("dynamodb: a write holds at most 100 items, its events and one marker per persistence ID")

// EventsStore implements persistence.EventsStore on a DynamoDB table.
//
// The table's partition key is the string attribute PersistenceID and its sort
// key the number attribute SequenceNumber. It has two global secondary
// indexes:
//   - shard_timestamp, with the number attribute ShardNumber as partition key and
//     the string attribute ShardKey as sort key, projecting every attribute. It
//     serves EventsByShard and ShardOffsets.
//   - persistence_ids, with the number attribute MarkerShard as partition key and
//     the string attribute PersistenceID as sort key, projecting the keys only.
//     It serves PersistenceIDs.
//
// A write is one transaction, so it is atomic, and it fails when an event is
// stored already. A transaction holds at most 100 items: the events plus one
// marker item per persistence ID, so a write holds at most 99 events of one
// persistence ID and a larger one fails with ErrTooManyEvents. The marker
// lists the persistence ID in the persistence_ids index; it stays after
// DeleteEvents, so a persistence ID whose events are all deleted stays listed.
//
// Replays read the table with strongly consistent reads. The shard reads query
// the shard_timestamp index, which DynamoDB updates eventually. A projection
// reads its shards again two minutes behind its position, so it delivers an
// event the index exposes up to two minutes after newer ones; keep the index's
// write capacity at least the table's, so that index writes are never
// throttled long enough to fall further behind. ShardOffsets runs one query per
// shard, persistence.NumberOfShards queries per call. Timestamps must not be
// negative, so that their fixed-width form orders as they do.
//
// An EventsStore is safe for concurrent use.
type EventsStore struct {
	// client sends the store's requests. The application owns it.
	client *dynamodb.Client
	// tableName is the table that holds the events.
	tableName *string
}

// enforce interface implementation
var _ persistence.EventsStore = (*EventsStore)(nil)

// NewEventsStore creates an events store on the given table.
//
// The client stays owned by the caller, so the same client can serve the
// application's own requests.
func NewEventsStore(tableName string, client *dynamodb.Client) *EventsStore {
	return &EventsStore{
		client:    client,
		tableName: aws.String(tableName),
	}
}

// Connect verifies the table is reachable. The client needs no connection.
func (s *EventsStore) Connect(ctx context.Context) error {
	return s.Ping(ctx)
}

// Disconnect releases nothing: the client belongs to the application.
func (s *EventsStore) Disconnect(context.Context) error {
	return nil
}

// Ping verifies the table exists and DynamoDB answers.
func (s *EventsStore) Ping(ctx context.Context) error {
	return ping(ctx, s.client, s.tableName)
}

// WriteEvents stores the events in one transaction, together with the marker
// item of their persistence ID: either every event is stored or none is. An
// event whose persistence ID and sequence number are stored already fails the
// write, and so does a write of more than 100 items (ErrTooManyEvents).
func (s *EventsStore) WriteEvents(ctx context.Context, events []*egopb.Event) error {
	if len(events) == 0 {
		return nil
	}

	persistenceIDs := make(map[string]struct{})
	for _, event := range events {
		persistenceIDs[event.GetPersistenceId()] = struct{}{}
	}

	if len(events)+len(persistenceIDs) > transactionItemLimit {
		return ErrTooManyEvents
	}

	items := make([]types.TransactWriteItem, 0, len(events)+len(persistenceIDs))
	for _, event := range events {
		items = append(items, types.TransactWriteItem{Put: &types.Put{
			TableName:                s.tableName,
			Item:                     eventAttributes(event),
			ConditionExpression:      aws.String("attribute_not_exists(#sequence)"),
			ExpressionAttributeNames: map[string]string{"#sequence": sequenceNumberAttribute},
		}})
	}

	for persistenceID := range persistenceIDs {
		items = append(items, types.TransactWriteItem{Put: &types.Put{
			TableName: s.tableName,
			Item:      markerAttributes(persistenceID),
		}})
	}

	if _, err := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items}); err != nil {
		return fmt.Errorf("dynamodb: failed to write the events: %w", err)
	}

	return nil
}

// DeleteEvents deletes the events of the persistence ID up to the given
// sequence number, inclusive. The marker item stays.
func (s *EventsStore) DeleteEvents(ctx context.Context, persistenceID string, toSequenceNumber uint64) error {
	if toSequenceNumber < firstSequenceNumber {
		return nil
	}

	keys, err := queryItems(ctx, s.client, &dynamodb.QueryInput{
		TableName:                s.tableName,
		KeyConditionExpression:   aws.String("#persistenceID = :persistenceID AND #sequence BETWEEN :first AND :to"),
		ProjectionExpression:     aws.String("#persistenceID, #sequence"),
		ExpressionAttributeNames: map[string]string{"#persistenceID": partitionKey, "#sequence": sequenceNumberAttribute},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":persistenceID": stringValue(persistenceID),
			":first":         unsignedValue(firstSequenceNumber),
			":to":            unsignedValue(toSequenceNumber),
		},
		ConsistentRead: aws.Bool(true),
	}, math.MaxUint64)
	if err == nil {
		err = deleteItems(ctx, s.client, *s.tableName, keys)
	}

	if err != nil {
		return fmt.Errorf("dynamodb: failed to delete the events: %w", err)
	}

	return nil
}

// ReplayEvents returns at most limit events of the persistence ID between the
// two sequence numbers, both inclusive, in sequence order.
func (s *EventsStore) ReplayEvents(ctx context.Context, persistenceID string, fromSequenceNumber, toSequenceNumber uint64, limit uint64) ([]*egopb.Event, error) {
	fromSequenceNumber = max(fromSequenceNumber, firstSequenceNumber)
	if toSequenceNumber < fromSequenceNumber {
		return nil, nil
	}

	items, err := queryItems(ctx, s.client, &dynamodb.QueryInput{
		TableName:                s.tableName,
		KeyConditionExpression:   aws.String("#persistenceID = :persistenceID AND #sequence BETWEEN :from AND :to"),
		ExpressionAttributeNames: map[string]string{"#persistenceID": partitionKey, "#sequence": sequenceNumberAttribute},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":persistenceID": stringValue(persistenceID),
			":from":          unsignedValue(fromSequenceNumber),
			":to":            unsignedValue(toSequenceNumber),
		},
		ConsistentRead: aws.Bool(true),
	}, limit)
	if err != nil {
		return nil, fmt.Errorf("dynamodb: failed to replay the events: %w", err)
	}

	return decodeEvents(items)
}

// GetLatestEvent returns the event of the persistence ID with the highest
// sequence number, or nil when it has none.
func (s *EventsStore) GetLatestEvent(ctx context.Context, persistenceID string) (*egopb.Event, error) {
	items, err := queryItems(ctx, s.client, &dynamodb.QueryInput{
		TableName:                s.tableName,
		KeyConditionExpression:   aws.String("#persistenceID = :persistenceID AND #sequence >= :first"),
		ExpressionAttributeNames: map[string]string{"#persistenceID": partitionKey, "#sequence": sequenceNumberAttribute},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":persistenceID": stringValue(persistenceID),
			":first":         unsignedValue(firstSequenceNumber),
		},
		ScanIndexForward: aws.Bool(false),
		ConsistentRead:   aws.Bool(true),
	}, 1)
	if err != nil {
		return nil, fmt.Errorf("dynamodb: failed to read the latest event: %w", err)
	}

	events, err := decodeEvents(items)
	if err != nil || len(events) == 0 {
		return nil, err
	}

	return events[0], nil
}

// PersistenceIDs returns at most pageSize persistence IDs after pageToken and
// the token of the next page, empty once every persistence ID is listed. It
// reads the persistence_ids index shard after shard; within a shard the
// persistence IDs are in ascending order.
func (s *EventsStore) PersistenceIDs(ctx context.Context, pageSize uint64, pageToken string) ([]string, string, error) {
	if pageSize == 0 {
		return nil, "", nil
	}

	shardNumber, after, err := parsePageToken(pageToken)
	if err != nil {
		return nil, "", err
	}

	var persistenceIDs []string
	for ; shardNumber < persistence.NumberOfShards; shardNumber++ {
		listed, err := s.listShard(ctx, shardNumber, after, pageSize-uint64(len(persistenceIDs)))
		if err != nil {
			return nil, "", fmt.Errorf("dynamodb: failed to list the persistence IDs: %w", err)
		}

		persistenceIDs = append(persistenceIDs, listed...)
		if uint64(len(persistenceIDs)) == pageSize {
			return persistenceIDs, pageTokenOf(shardNumber, persistenceIDs[len(persistenceIDs)-1]), nil
		}

		after = ""
	}

	return persistenceIDs, "", nil
}

// EventsByShard returns at most limit events of the shard whose timestamp is
// at or after fromTimestamp, ordered by timestamp, then sequence number, then
// persistence ID.
func (s *EventsStore) EventsByShard(ctx context.Context, shardNumber uint64, fromTimestamp int64, limit uint64) ([]*egopb.Event, error) {
	items, err := queryItems(ctx, s.client, &dynamodb.QueryInput{
		TableName:                s.tableName,
		IndexName:                aws.String(shardIndex),
		KeyConditionExpression:   aws.String("#shard = :shard AND #shardKey >= :from"),
		ExpressionAttributeNames: map[string]string{"#shard": shardNumberAttribute, "#shardKey": shardKeyAttribute},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":shard": unsignedValue(shardNumber),
			":from":  stringValue(fixedWidth(fromTimestamp)),
		},
	}, limit)
	if err != nil {
		return nil, fmt.Errorf("dynamodb: failed to read the events of shard %d: %w", shardNumber, err)
	}

	return decodeEvents(items)
}

// ShardOffsets returns every shard holding events mapped to the timestamp of
// its most recent event, with one query per shard. An empty journal yields an
// empty map.
func (s *EventsStore) ShardOffsets(ctx context.Context) (map[uint64]int64, error) {
	offsets := &shardTimestamps{timestamps: make(map[uint64]int64)}
	err := forEachShard(ctx, func(ctx context.Context, shardNumber uint64) error {
		items, err := queryItems(ctx, s.client, &dynamodb.QueryInput{
			TableName:                 s.tableName,
			IndexName:                 aws.String(shardIndex),
			KeyConditionExpression:    aws.String("#shard = :shard"),
			ProjectionExpression:      aws.String("#timestamp"),
			ExpressionAttributeNames:  map[string]string{"#shard": shardNumberAttribute, "#timestamp": timestampAttribute},
			ExpressionAttributeValues: map[string]types.AttributeValue{":shard": unsignedValue(shardNumber)},
			ScanIndexForward:          aws.Bool(false),
		}, 1)
		if err != nil || len(items) == 0 {
			return err
		}

		var latest timestampItem
		if err := attributevalue.UnmarshalMap(items[0], &latest); err != nil {
			return err
		}

		offsets.set(shardNumber, latest.Timestamp)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("dynamodb: failed to read the shard offsets: %w", err)
	}

	return offsets.timestamps, nil
}

// listShard returns at most limit persistence IDs of the shard after the given
// one, or from the first one when after is empty.
func (s *EventsStore) listShard(ctx context.Context, shardNumber uint64, after string, limit uint64) ([]string, error) {
	input := &dynamodb.QueryInput{
		TableName:                 s.tableName,
		IndexName:                 aws.String(persistenceIDsIndex),
		KeyConditionExpression:    aws.String("#marker = :shard"),
		ProjectionExpression:      aws.String("#persistenceID"),
		ExpressionAttributeNames:  map[string]string{"#marker": markerShardAttribute, "#persistenceID": partitionKey},
		ExpressionAttributeValues: map[string]types.AttributeValue{":shard": unsignedValue(shardNumber)},
	}

	// DynamoDB rejects an empty string in a key condition
	if after != "" {
		input.KeyConditionExpression = aws.String("#marker = :shard AND #persistenceID > :after")
		input.ExpressionAttributeValues[":after"] = stringValue(after)
	}

	items, err := queryItems(ctx, s.client, input, limit)
	if err != nil {
		return nil, err
	}

	var markers []markerItem
	if err := attributevalue.UnmarshalListOfMaps(items, &markers); err != nil {
		return nil, err
	}

	persistenceIDs := make([]string, len(markers))
	for index, marker := range markers {
		persistenceIDs[index] = marker.PersistenceID
	}

	return persistenceIDs, nil
}

// eventItem is the layout of an event item.
type eventItem struct {
	// PersistenceID is the partition key.
	PersistenceID string `dynamodbav:"PersistenceID"`
	// SequenceNumber is the sort key.
	SequenceNumber uint64 `dynamodbav:"SequenceNumber"`
	// IsDeleted is the deletion flag of the event.
	IsDeleted bool `dynamodbav:"IsDeleted"`
	// EventPayload is the Value of the event's anypb.Any.
	EventPayload []byte `dynamodbav:"EventPayload"`
	// EventManifest is the TypeUrl of the event's anypb.Any.
	EventManifest string `dynamodbav:"EventManifest"`
	// Timestamp is the timestamp of the event, in Unix nanoseconds.
	Timestamp int64 `dynamodbav:"Timestamp"`
	// ShardNumber is the journal shard of the event.
	ShardNumber uint64 `dynamodbav:"ShardNumber"`
	// EncryptionKeyID identifies the key the event is encrypted with.
	EncryptionKeyID string `dynamodbav:"EncryptionKeyID"`
	// IsEncrypted is the encryption flag of the event.
	IsEncrypted bool `dynamodbav:"IsEncrypted"`
}

// markerItem is the layout of a marker item as the persistence IDs index returns it.
type markerItem struct {
	// PersistenceID is the persistence ID the marker lists.
	PersistenceID string `dynamodbav:"PersistenceID"`
}

// timestampItem is an item read for its timestamp only.
type timestampItem struct {
	// Timestamp is the timestamp of the item, in Unix nanoseconds.
	Timestamp int64 `dynamodbav:"Timestamp"`
}

// eventAttributes returns the attributes of the event's item.
func eventAttributes(event *egopb.Event) map[string]types.AttributeValue {
	payload := event.GetEvent().GetValue()
	if payload == nil {
		payload = []byte{}
	}

	return map[string]types.AttributeValue{
		partitionKey:             stringValue(event.GetPersistenceId()),
		sequenceNumberAttribute:  unsignedValue(event.GetSequenceNumber()),
		isDeletedAttribute:       boolValue(event.GetIsDeleted()),
		eventPayloadAttribute:    &types.AttributeValueMemberB{Value: payload},
		eventManifestAttribute:   stringValue(event.GetEvent().GetTypeUrl()),
		timestampAttribute:       signedValue(event.GetTimestamp()),
		shardNumberAttribute:     unsignedValue(event.GetShard()),
		shardKeyAttribute:        stringValue(fixedWidth(event.GetTimestamp()) + fixedWidth(event.GetSequenceNumber()) + event.GetPersistenceId()),
		encryptionKeyIDAttribute: stringValue(event.GetEncryptionKeyId()),
		isEncryptedAttribute:     boolValue(event.GetIsEncrypted()),
	}
}

// markerAttributes returns the attributes of the marker item of a persistence ID.
func markerAttributes(persistenceID string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		partitionKey:            stringValue(persistenceID),
		sequenceNumberAttribute: unsignedValue(markerSequenceNumber),
		markerShardAttribute:    unsignedValue(persistence.ShardFor(persistenceID)),
	}
}

// decodeEvents decodes event items into events.
func decodeEvents(items []map[string]types.AttributeValue) ([]*egopb.Event, error) {
	var decoded []eventItem
	if err := attributevalue.UnmarshalListOfMaps(items, &decoded); err != nil {
		return nil, fmt.Errorf("dynamodb: failed to decode the events: %w", err)
	}

	events := make([]*egopb.Event, len(decoded))
	for index, item := range decoded {
		events[index] = &egopb.Event{
			PersistenceId:   item.PersistenceID,
			SequenceNumber:  item.SequenceNumber,
			IsDeleted:       item.IsDeleted,
			Event:           &anypb.Any{TypeUrl: item.EventManifest, Value: item.EventPayload},
			Timestamp:       item.Timestamp,
			Shard:           item.ShardNumber,
			EncryptionKeyId: item.EncryptionKeyID,
			IsEncrypted:     item.IsEncrypted,
		}
	}

	return events, nil
}

// pageTokenOf returns the page token of the page that ends with the
// persistence ID of the shard.
func pageTokenOf(shardNumber uint64, persistenceID string) string {
	return strconv.FormatUint(shardNumber, 10) + pageTokenSeparator + persistenceID
}

// parsePageToken returns the shard and the persistence ID a page token ends
// with; an empty token starts at the first shard.
func parsePageToken(pageToken string) (uint64, string, error) {
	if pageToken == "" {
		return 0, "", nil
	}

	shard, persistenceID, found := strings.Cut(pageToken, pageTokenSeparator)
	shardNumber, err := strconv.ParseUint(shard, 10, 64)
	if !found || err != nil || shardNumber >= persistence.NumberOfShards || persistenceID == "" {
		return 0, "", fmt.Errorf("dynamodb: invalid page token %q", pageToken)
	}

	return shardNumber, persistenceID, nil
}
