/*
 * MIT License
 *
 * Copyright (c) 2022-2024 Tochemey
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/tochemey/ego/v3/egopb"
	"github.com/tochemey/ego/v3/eventstore"
	"github.com/tochemey/ego/v3/internal/telemetry"
)


var (
	tableName = "events_store"
)

type EventStore struct {
	db              *dynamodb.Client
	insertBatchSize int
	connected       *atomic.Bool
}

// enforce interface implementation
var _ eventstore.EventsStore = (*EventStore)(nil)

func NewEventStore() *EventStore {
	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
	)
	if err != nil {
		panic("unable to load SDK config, " + err.Error())
	}
	db := dynamodb.NewFromConfig(cfg)

	return &EventStore{
		db:              db,
		insertBatchSize: 100,
		connected:       atomic.NewBool(false),
	}
}

func (s *EventStore) Connect(ctx context.Context) error {
	_, span := telemetry.SpanContext(ctx, "eventsStore.Connect")
	defer span.End()

	if s.connected.Load() {
		return nil
	}

	// We'll assume the connection is always active since
	// the connection is stateless

	// Create DynamoDB Table
	_, err := s.db.CreateTable(context.TODO(), &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{{
			AttributeName: aws.String("persistence_id"),
			AttributeType: types.ScalarAttributeTypeS,
		}, {
			AttributeName: aws.String("sequence_number"),
			AttributeType: types.ScalarAttributeTypeN,
		}},
		KeySchema: []types.KeySchemaElement{{
			AttributeName: aws.String("persistence_id"),
			KeyType:       types.KeyTypeHash,
		}, {
			AttributeName: aws.String("sequence_number"),
			KeyType:       types.KeyTypeRange,
		}},
		TableName: aws.String(tableName),
		BillingMode: types.BillingModePayPerRequest,
	})

	if err != nil {
		panic("Got error calling CreateTable: " + err.Error())
	} else {
		fmt.Println("Created the table", tableName)
	}

	time.Sleep(10 * time.Second)

	s.connected.Store(true)
	return nil
}

// Disconnect implements eventstore.EventsStore.
func (s *EventStore) Disconnect(ctx context.Context) error {
	_, span := telemetry.SpanContext(ctx, "eventsStore.Disconnect")
	defer span.End()

	// check whether this instance of the journal is connected or not
	if !s.connected.Load() {
		return nil
	}

	// set the connection status
	s.connected.Store(false)

	return nil

}

// DeleteEvents implements eventstore.EventsStore.
func (s *EventStore) DeleteEvents(ctx context.Context, persistenceID string, toSequenceNumber uint64) error {
	panic("unimplemented")
}

// GetLatestEvent implements eventstore.EventsStore.
func (s *EventStore) GetLatestEvent(ctx context.Context, persistenceID string) (*egopb.Event, error) {
	panic("unimplemented")
}

// GetShardEvents implements eventstore.EventsStore.
func (s *EventStore) GetShardEvents(ctx context.Context, shardNumber uint64, offset int64, max uint64) ([]*egopb.Event, int64, error) {
	panic("unimplemented")
}

// PersistenceIDs implements eventstore.EventsStore.
func (s *EventStore) PersistenceIDs(ctx context.Context, pageSize uint64, pageToken string) (persistenceIDs []string, nextPageToken string, err error) {
	panic("unimplemented")
}

// Ping implements eventstore.EventsStore.
func (s *EventStore) Ping(ctx context.Context) error {
	// add a span context
	ctx, span := telemetry.SpanContext(ctx, "eventsStore.Ping")
	defer span.End()

	// check whether we are connected or not
	if !s.connected.Load() {
		return s.Connect(ctx)
	}

	return nil
}

// ReplayEvents implements eventstore.EventsStore.
func (s *EventStore) ReplayEvents(ctx context.Context, persistenceID string, fromSequenceNumber uint64, toSequenceNumber uint64, max uint64) ([]*egopb.Event, error) {
	panic("unimplemented")
}

// ShardNumbers implements eventstore.EventsStore.
func (s *EventStore) ShardNumbers(ctx context.Context) ([]uint64, error) {
	panic("unimplemented")
}

// WriteEvents implements eventstore.EventsStore.
func (s *EventStore) WriteEvents(ctx context.Context, events []*egopb.Event) error {
	_, span := telemetry.SpanContext(ctx, "eventsStore.WriteEvents")
	defer span.End()

	// check whether this instance of the journal is connected or not
	if !s.connected.Load() {
		return errors.New("journal store is not connected")
	}

	// check whether the journals list is empty
	if len(events) == 0 {
		// do nothing
		return nil
	}

	for _, event := range events {
		var (
			eventManifest string
			eventBytes    []byte
			stateManifest string
			stateBytes    []byte
		)

		// serialize the event and resulting state
		eventBytes, _ = proto.Marshal(event.GetEvent())
		stateBytes, _ = proto.Marshal(event.GetResultingState())

		// grab the manifest
		eventManifest = string(event.GetEvent().ProtoReflect().Descriptor().FullName())
		stateManifest = string(event.GetResultingState().ProtoReflect().Descriptor().FullName())

		err := putItem(s.db, tableName, map[string]types.AttributeValue{
			"persistence_id": &types.AttributeValueMemberS{Value: event.GetPersistenceId()},
			"sequence_number": &types.AttributeValueMemberN{Value: strconv.FormatUint(event.GetSequenceNumber(), 10)},
			"is_deleted": &types.AttributeValueMemberBOOL{Value: event.GetIsDeleted()},
			"event_bytes": &types.AttributeValueMemberS{Value: string(eventBytes)},
			"eventManifest": &types.AttributeValueMemberS{Value: string(eventManifest)},
			"state_bytes": &types.AttributeValueMemberS{Value: string(stateBytes)},
			"state_manifest": &types.AttributeValueMemberS{Value: string(stateManifest)},
			"timestamp": &types.AttributeValueMemberN{Value: strconv.FormatInt(event.GetTimestamp(), 10)},
			"shard": &types.AttributeValueMemberN{Value: strconv.FormatUint(event.GetShard(), 10)},
		})
		if err != nil {
			panic("Got error calling PutItem: " + err.Error())
		} else {
			fmt.Println("PutItem succeeded")
		}
	}

	return nil
}

func putItem(d *dynamodb.Client, tableName string, item map[string]types.AttributeValue) error {
	_, err := d.PutItem(context.TODO(), &dynamodb.PutItemInput{
		Item:      item,
		TableName: aws.String(tableName),
	})
	return err
}