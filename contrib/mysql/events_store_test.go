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

package mysql

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/tochemey/ego/v5/egopb"
	"github.com/tochemey/ego/v5/persistence"
)

// looseIndexScan is how EXPLAIN reports a GROUP BY or DISTINCT served by one seek per group.
const looseIndexScan = "Using index for group-by"

func TestEventsStoreConnection(t *testing.T) {
	ctx := context.Background()
	store := NewEventsStore(testDB)

	require.NoError(t, store.Connect(ctx))
	require.NoError(t, store.Disconnect(ctx))
	require.NoError(t, store.Ping(ctx), "Disconnect must leave the application's handle open")
}

func TestWriteEvents(t *testing.T) {
	ctx := context.Background()
	store := NewEventsStore(testDB)

	t.Run("an empty write stores nothing", func(t *testing.T) {
		resetTables(t)
		require.NoError(t, store.WriteEvents(ctx, nil))

		offsets, err := store.ShardOffsets(ctx)
		require.NoError(t, err)
		assert.Empty(t, offsets)
	})

	t.Run("a write larger than one statement is stored in full", func(t *testing.T) {
		resetTables(t)
		count := insertEvents.rowsPerStatement() + 1
		require.NoError(t, store.WriteEvents(ctx, manyEvents(t, "account-1", count)))

		latest, err := store.GetLatestEvent(ctx, "account-1")
		require.NoError(t, err)
		require.NotNil(t, latest)
		assert.EqualValues(t, count, latest.GetSequenceNumber())
	})

	t.Run("a write larger than one statement stores nothing when its last statement fails", func(t *testing.T) {
		resetTables(t)
		stored := newEvent(t, "account-2", 1, 1, 1)
		require.NoError(t, store.WriteEvents(ctx, []*egopb.Event{stored}))

		events := manyEvents(t, "account-1", insertEvents.rowsPerStatement())
		events = append(events, stored)
		require.Error(t, store.WriteEvents(ctx, events))

		latest, err := store.GetLatestEvent(ctx, "account-1")
		require.NoError(t, err)
		assert.Nil(t, latest)
	})

	t.Run("an event of an empty message keeps its type", func(t *testing.T) {
		resetTables(t)
		payload, err := anypb.New(new(emptypb.Empty))
		require.NoError(t, err)

		event := &egopb.Event{PersistenceId: "account-1", SequenceNumber: 1, Event: payload, Timestamp: 1, Shard: 1}
		require.NoError(t, store.WriteEvents(ctx, []*egopb.Event{event}))

		latest, err := store.GetLatestEvent(ctx, "account-1")
		require.NoError(t, err)
		assert.True(t, proto.Equal(event, latest), "got %v", latest)
	})

	t.Run("persistence IDs that differ only by case are distinct", func(t *testing.T) {
		resetTables(t)
		require.NoError(t, store.WriteEvents(ctx, []*egopb.Event{newEvent(t, "account-1", 1, 1, 1)}))
		require.NoError(t, store.WriteEvents(ctx, []*egopb.Event{newEvent(t, "ACCOUNT-1", 1, 1, 1)}))

		persistenceIDs, _, err := store.PersistenceIDs(ctx, 10, "")
		require.NoError(t, err)
		assert.Equal(t, []string{"ACCOUNT-1", "account-1"}, persistenceIDs)
	})
}

func TestPersistenceIDsOfAnEmptyPage(t *testing.T) {
	resetTables(t)
	store := NewEventsStore(testDB)
	require.NoError(t, store.WriteEvents(context.Background(), []*egopb.Event{newEvent(t, "account-1", 1, 1, 1)}))

	persistenceIDs, nextPageToken, err := store.PersistenceIDs(context.Background(), 0, "")
	require.NoError(t, err)
	assert.Empty(t, persistenceIDs)
	assert.Empty(t, nextPageToken)
}

func TestShardOffsetsCoverEveryShard(t *testing.T) {
	resetTables(t)
	ctx := context.Background()
	store := NewEventsStore(testDB)

	lastShard := persistence.NumberOfShards - 1
	require.NoError(t, store.WriteEvents(ctx, []*egopb.Event{newEvent(t, "account-1", 1, 0, 100)}))
	require.NoError(t, store.WriteEvents(ctx, []*egopb.Event{newEvent(t, "account-2", 1, lastShard, 200)}))

	offsets, err := store.ShardOffsets(ctx)
	require.NoError(t, err)
	assert.Equal(t, map[uint64]int64{0: 100, lastShard: 200}, offsets)
}

func TestEventsStoreQueriesUseTheIndexes(t *testing.T) {
	resetTables(t)
	store := NewEventsStore(testDB)

	// a journal spread over many shards, so that a shard is a small part of it
	const (
		entities        = 200
		eventsPerEntity = 10
		shards          = 64
	)

	journal := make([]*egopb.Event, 0, entities*eventsPerEntity)
	for entity := range entities {
		for sequenceNumber := uint64(1); sequenceNumber <= eventsPerEntity; sequenceNumber++ {
			journal = append(journal, newEvent(t, fmt.Sprintf("account-%d", entity), sequenceNumber, uint64(entity%shards), int64(sequenceNumber)))
		}
	}

	require.NoError(t, store.WriteEvents(context.Background(), journal))

	_, err := testDB.ExecContext(context.Background(), "ANALYZE TABLE events_store")
	require.NoError(t, err)

	t.Run("the shard offsets seek the newest entry of every shard", func(t *testing.T) {
		assert.Contains(t, strings.Join(explain(t, shardOffsetsQuery), " "), looseIndexScan)
	})

	t.Run("the persistence IDs seek one entry per persistence ID", func(t *testing.T) {
		assert.Contains(t, strings.Join(explain(t, persistenceIDsQuery, "", 10), " "), looseIndexScan)
	})

	t.Run("a shard is read in index order without sorting", func(t *testing.T) {
		assert.NotContains(t, strings.Join(explain(t, eventsByShardQuery, 1, 0, 10), " "), "filesort")
	})
}

func TestEventsStoreErrorsAreReported(t *testing.T) {
	ctx := context.Background()
	store := NewEventsStore(newClosedDB(t))

	assert.Error(t, store.Connect(ctx))
	assert.Error(t, store.Ping(ctx))
	assert.ErrorContains(t, store.WriteEvents(ctx, []*egopb.Event{newEvent(t, "account-1", 1, 1, 1)}), "mysql: failed to write the events")
	assert.ErrorContains(t, store.WriteEvents(ctx, manyEvents(t, "account-1", insertEvents.rowsPerStatement()+1)), "mysql: failed to write the events")
	assert.ErrorContains(t, store.DeleteEvents(ctx, "account-1", 1), "mysql: failed to delete the events")

	_, err := store.ReplayEvents(ctx, "account-1", 1, 10, 10)
	assert.ErrorContains(t, err, "mysql: failed to replay the events")

	_, err = store.GetLatestEvent(ctx, "account-1")
	assert.ErrorContains(t, err, "mysql: failed to read the latest event")

	_, _, err = store.PersistenceIDs(ctx, 10, "")
	assert.ErrorContains(t, err, "mysql: failed to list the persistence IDs")

	_, err = store.EventsByShard(ctx, 1, 0, 10)
	assert.ErrorContains(t, err, "mysql: failed to read the shard events")

	_, err = store.ShardOffsets(ctx)
	assert.ErrorContains(t, err, "mysql: failed to read the shard offsets")
}
