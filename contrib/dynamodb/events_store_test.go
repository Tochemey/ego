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
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/tochemey/ego/v5/egopb"
	"github.com/tochemey/ego/v5/persistence"
)

func TestEventsStoreTransactionLimit(t *testing.T) {
	ctx := context.Background()
	store := NewEventsStore(createEventsTable(t), testClient)

	// 99 events and the marker fill one transaction
	require.NoError(t, store.WriteEvents(ctx, newJournal(t, "account-1", 1, 99)))

	err := store.WriteEvents(ctx, newJournal(t, "account-2", 1, 100))
	require.ErrorIs(t, err, ErrTooManyEvents)

	events, err := store.ReplayEvents(ctx, "account-2", 1, 100, 100)
	require.NoError(t, err)
	assert.Empty(t, events)
}

func TestEventsStoreEdgeCases(t *testing.T) {
	ctx := context.Background()
	store := NewEventsStore(createEventsTable(t), testClient)

	t.Run("an empty write stores nothing", func(t *testing.T) {
		require.NoError(t, store.WriteEvents(ctx, nil))
	})

	t.Run("an empty range, a zero limit and a zero bound read and delete nothing", func(t *testing.T) {
		require.NoError(t, store.WriteEvents(ctx, newJournal(t, "account-1", 1, 3)))

		events, err := store.ReplayEvents(ctx, "account-1", 3, 2, 10)
		require.NoError(t, err)
		assert.Empty(t, events)

		events, err = store.ReplayEvents(ctx, "account-1", 1, 3, 0)
		require.NoError(t, err)
		assert.Empty(t, events)

		require.NoError(t, store.DeleteEvents(ctx, "account-1", 0))

		events, err = store.ReplayEvents(ctx, "account-1", 0, 3, 10)
		require.NoError(t, err)
		assert.Len(t, events, 3)
	})

	t.Run("keeps the type of an event of a message with no field set", func(t *testing.T) {
		payload, err := anypb.New(new(emptypb.Empty))
		require.NoError(t, err)

		event := &egopb.Event{PersistenceId: "account-empty", SequenceNumber: 1, Event: payload, Timestamp: 100, Shard: 1}
		require.NoError(t, store.WriteEvents(ctx, []*egopb.Event{event}))

		latest, err := store.GetLatestEvent(ctx, "account-empty")
		require.NoError(t, err)
		assert.True(t, proto.Equal(event, latest), "got %v", latest)
	})

	t.Run("stores an event whose payload has no value", func(t *testing.T) {
		event := &egopb.Event{PersistenceId: "account-nil", SequenceNumber: 1, Event: &anypb.Any{TypeUrl: "type.googleapis.com/account.Opened"}, Timestamp: 100, Shard: 1}
		require.NoError(t, store.WriteEvents(ctx, []*egopb.Event{event}))

		latest, err := store.GetLatestEvent(ctx, "account-nil")
		require.NoError(t, err)
		assert.True(t, proto.Equal(event, latest), "got %v", latest)
	})

	t.Run("replays events spread over several result pages", func(t *testing.T) {
		// DynamoDB returns at most 1 MB per page: 30 events of 40 KB span two pages
		journal := newJournal(t, "account-large", 1, 30)
		for _, event := range journal {
			event.Event.Value = make([]byte, 40*1024)
		}

		require.NoError(t, store.WriteEvents(ctx, journal))

		events, err := store.ReplayEvents(ctx, "account-large", 1, 30, 30)
		require.NoError(t, err)
		assert.Len(t, events, 30)
	})

	t.Run("deletes more events than one batch request holds", func(t *testing.T) {
		require.NoError(t, store.WriteEvents(ctx, newJournal(t, "account-many", 1, 60)))

		require.NoError(t, store.DeleteEvents(ctx, "account-many", 60))

		latest, err := store.GetLatestEvent(ctx, "account-many")
		require.NoError(t, err)
		assert.Nil(t, latest)
	})
}

func TestEventsStorePersistenceIDs(t *testing.T) {
	ctx := context.Background()

	t.Run("lists a journal that does not start at sequence number 1, and keeps a deleted one", func(t *testing.T) {
		store := NewEventsStore(createEventsTable(t), testClient)
		require.NoError(t, store.WriteEvents(ctx, newJournal(t, "account-migrated", 50, 2)))
		require.NoError(t, store.WriteEvents(ctx, newJournal(t, "account-deleted", 1, 2)))
		require.NoError(t, store.DeleteEvents(ctx, "account-deleted", 2))

		persistenceIDs, token, err := store.PersistenceIDs(ctx, 10, "")
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"account-migrated", "account-deleted"}, persistenceIDs)
		assert.Empty(t, token)
	})

	t.Run("pages through persistence IDs spread over many shards", func(t *testing.T) {
		store := NewEventsStore(createEventsTable(t), testClient)

		expected := make([]string, 0, 12)
		for index := range 12 {
			persistenceID := fmt.Sprintf("account-%d", index)
			expected = append(expected, persistenceID)
			require.NoError(t, store.WriteEvents(ctx, newJournal(t, persistenceID, 1, 1)))
		}

		var (
			listed []string
			token  string
		)

		for range len(expected) + 1 {
			page, next, err := store.PersistenceIDs(ctx, 5, token)
			require.NoError(t, err)
			listed = append(listed, page...)

			if next == "" {
				break
			}

			token = next
		}

		assert.ElementsMatch(t, expected, listed)

		page, next, err := store.PersistenceIDs(ctx, 0, "")
		require.NoError(t, err)
		assert.Empty(t, page)
		assert.Empty(t, next)
	})

	t.Run("rejects an invalid page token", func(t *testing.T) {
		store := NewEventsStore(createEventsTable(t), testClient)
		for _, token := range []string{"no-separator", "x/account-1", "4096/account-1", "1/"} {
			_, _, err := store.PersistenceIDs(ctx, 10, token)
			assert.ErrorContains(t, err, "invalid page token", token)
		}
	})
}

func TestEventsStoreDecodingFailures(t *testing.T) {
	ctx := context.Background()
	tableName := createEventsTable(t)
	store := NewEventsStore(tableName, testClient)

	// an event whose payload is not binary and whose timestamp is not a number
	_, err := testClient.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			partitionKey:            stringValue("account-1"),
			sequenceNumberAttribute: unsignedValue(1),
			eventPayloadAttribute:   stringValue("not binary"),
			timestampAttribute:      stringValue("not a number"),
			shardNumberAttribute:    unsignedValue(1),
			shardKeyAttribute:       stringValue(fixedWidth(uint64(1)) + "account-1"),
		},
	})
	require.NoError(t, err)

	_, err = store.ReplayEvents(ctx, "account-1", 1, 1, 10)
	assert.ErrorContains(t, err, "failed to decode the events")

	_, err = store.GetLatestEvent(ctx, "account-1")
	assert.ErrorContains(t, err, "failed to decode the events")

	_, err = store.EventsByShard(ctx, 1, 0, 10)
	assert.ErrorContains(t, err, "failed to decode the events")

	_, err = store.ShardOffsets(ctx)
	assert.ErrorContains(t, err, "failed to read the shard offsets")
}

func TestEventsStoreFailures(t *testing.T) {
	ctx := context.Background()
	store := NewEventsStore(missingTable, testClient)

	assert.Error(t, store.Connect(ctx))
	assert.NoError(t, store.Disconnect(ctx))
	assert.ErrorContains(t, store.WriteEvents(ctx, newJournal(t, "account-1", 1, 1)), "failed to write the events")
	assert.ErrorContains(t, store.DeleteEvents(ctx, "account-1", 1), "failed to delete the events")

	_, err := store.ReplayEvents(ctx, "account-1", 1, 1, 10)
	assert.ErrorContains(t, err, "failed to replay the events")

	_, err = store.GetLatestEvent(ctx, "account-1")
	assert.ErrorContains(t, err, "failed to read the latest event")

	_, _, err = store.PersistenceIDs(ctx, 10, "")
	assert.ErrorContains(t, err, "failed to list the persistence IDs")

	_, err = store.EventsByShard(ctx, 1, 0, 10)
	assert.ErrorContains(t, err, "failed to read the events of shard 1")

	_, err = store.ShardOffsets(ctx)
	assert.ErrorContains(t, err, "failed to read the shard offsets")
}

// newJournal builds count events of the persistence ID from the given sequence
// number on, stamped with their sequence number.
func newJournal(t *testing.T, persistenceID string, from uint64, count uint64) []*egopb.Event {
	t.Helper()

	events := make([]*egopb.Event, 0, count)
	for sequenceNumber := from; sequenceNumber < from+count; sequenceNumber++ {
		payload, err := anypb.New(wrapperspb.UInt64(sequenceNumber))
		require.NoError(t, err)

		events = append(events, &egopb.Event{
			PersistenceId:  persistenceID,
			SequenceNumber: sequenceNumber,
			Event:          payload,
			Timestamp:      int64(sequenceNumber) * 100,
			Shard:          persistence.ShardFor(persistenceID),
		})
	}

	return events
}
