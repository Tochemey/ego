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

package sqlite

import (
	"context"
	"database/sql"
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

func TestEventsStoreConnection(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	store := NewEventsStore(db)

	require.NoError(t, store.Connect(ctx))
	require.NoError(t, store.Ping(ctx))
	require.NoError(t, store.Disconnect(ctx))

	// the handle stays open for its owner
	require.NoError(t, db.PingContext(ctx))
}

func TestWriteAndReplayEvents(t *testing.T) {
	ctx := context.Background()
	store := NewEventsStore(newTestDB(t))

	events := []*egopb.Event{
		newEvent(t, "account-1", 1, 3, 10),
		newEvent(t, "account-1", 2, 3, 20),
		newEvent(t, "account-1", 3, 3, 30),
	}
	require.NoError(t, store.WriteEvents(ctx, events))

	t.Run("an entity's events are replayed in sequence order with their payload", func(t *testing.T) {
		replayed, err := store.ReplayEvents(ctx, "account-1", 1, 3, 10)
		require.NoError(t, err)
		require.Len(t, replayed, len(events))

		for index, event := range replayed {
			assert.True(t, proto.Equal(events[index], event))
		}
	})

	t.Run("the replay honours the sequence bounds and the limit", func(t *testing.T) {
		replayed, err := store.ReplayEvents(ctx, "account-1", 2, 3, 10)
		require.NoError(t, err)
		require.Len(t, replayed, 2)
		assert.EqualValues(t, 2, replayed[0].GetSequenceNumber())

		replayed, err = store.ReplayEvents(ctx, "account-1", 1, 3, 1)
		require.NoError(t, err)
		require.Len(t, replayed, 1)
		assert.EqualValues(t, 1, replayed[0].GetSequenceNumber())
	})

	t.Run("an unknown entity replays nothing", func(t *testing.T) {
		replayed, err := store.ReplayEvents(ctx, "unknown", 1, 3, 10)
		require.NoError(t, err)
		assert.Empty(t, replayed)
	})

	t.Run("the latest event has the highest sequence number", func(t *testing.T) {
		latest, err := store.GetLatestEvent(ctx, "account-1")
		require.NoError(t, err)
		assert.True(t, proto.Equal(events[2], latest))
	})

	t.Run("an unknown entity has no latest event", func(t *testing.T) {
		latest, err := store.GetLatestEvent(ctx, "unknown")
		require.NoError(t, err)
		assert.Nil(t, latest)
	})
}

func TestWriteEventsIsAtomic(t *testing.T) {
	ctx := context.Background()

	t.Run("an empty write stores nothing", func(t *testing.T) {
		store := NewEventsStore(newTestDB(t))
		require.NoError(t, store.WriteEvents(ctx, nil))
	})

	t.Run("an event stored already fails the whole write", func(t *testing.T) {
		store := NewEventsStore(newTestDB(t))
		require.NoError(t, store.WriteEvents(ctx, []*egopb.Event{newEvent(t, "account-1", 1, 3, 10)}))

		err := store.WriteEvents(ctx, []*egopb.Event{
			newEvent(t, "account-1", 2, 3, 20),
			newEvent(t, "account-1", 1, 3, 10),
		})
		require.Error(t, err)

		latest, err := store.GetLatestEvent(ctx, "account-1")
		require.NoError(t, err)
		assert.EqualValues(t, 1, latest.GetSequenceNumber())
	})

	t.Run("a write larger than one statement is stored in full", func(t *testing.T) {
		store := NewEventsStore(newTestDB(t))
		events := manyEvents(t, "account-1", insertEvents.rowsPerStatement()*2+1)

		require.NoError(t, store.WriteEvents(ctx, events))

		replayed, err := store.ReplayEvents(ctx, "account-1", 1, uint64(len(events)), uint64(len(events)))
		require.NoError(t, err)
		assert.Len(t, replayed, len(events))
	})

	t.Run("a write larger than one statement stores nothing when its last statement fails", func(t *testing.T) {
		store := NewEventsStore(newTestDB(t))
		events := manyEvents(t, "account-1", insertEvents.rowsPerStatement()+1)
		events[len(events)-1] = events[0]

		require.Error(t, store.WriteEvents(ctx, events))

		latest, err := store.GetLatestEvent(ctx, "account-1")
		require.NoError(t, err)
		assert.Nil(t, latest)
	})
}

func TestEventPayloads(t *testing.T) {
	ctx := context.Background()
	store := NewEventsStore(newTestDB(t))

	t.Run("an event of an empty message keeps its type", func(t *testing.T) {
		payload, err := anypb.New(new(emptypb.Empty))
		require.NoError(t, err)
		require.Empty(t, payload.GetValue())

		event := &egopb.Event{PersistenceId: "account-1", SequenceNumber: 1, Event: payload}
		require.NoError(t, store.WriteEvents(ctx, []*egopb.Event{event}))

		latest, err := store.GetLatestEvent(ctx, "account-1")
		require.NoError(t, err)
		assert.Equal(t, payload.GetTypeUrl(), latest.GetEvent().GetTypeUrl())
		assert.Empty(t, latest.GetEvent().GetValue())
	})

	t.Run("a payload is read back without its message type being known", func(t *testing.T) {
		payload := &anypb.Any{TypeUrl: "type.googleapis.com/unknown.Message", Value: []byte{1, 2, 3}}
		event := &egopb.Event{PersistenceId: "account-2", SequenceNumber: 1, Event: payload}
		require.NoError(t, store.WriteEvents(ctx, []*egopb.Event{event}))

		latest, err := store.GetLatestEvent(ctx, "account-2")
		require.NoError(t, err)
		assert.True(t, proto.Equal(payload, latest.GetEvent()))
	})
}

func TestDeleteEvents(t *testing.T) {
	ctx := context.Background()
	store := NewEventsStore(newTestDB(t))
	require.NoError(t, store.WriteEvents(ctx, manyEvents(t, "account-1", 3)))

	require.NoError(t, store.DeleteEvents(ctx, "account-1", 2))

	replayed, err := store.ReplayEvents(ctx, "account-1", 1, 3, 10)
	require.NoError(t, err)
	require.Len(t, replayed, 1)
	assert.EqualValues(t, 3, replayed[0].GetSequenceNumber())
}

func TestPersistenceIDs(t *testing.T) {
	ctx := context.Background()
	store := NewEventsStore(newTestDB(t))

	for _, persistenceID := range []string{"c", "a", "b"} {
		require.NoError(t, store.WriteEvents(ctx, manyEvents(t, persistenceID, 2)))
	}

	ids, token, err := store.PersistenceIDs(ctx, 2, "")
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, ids)
	assert.Equal(t, "b", token)

	ids, token, err = store.PersistenceIDs(ctx, 2, token)
	require.NoError(t, err)
	assert.Equal(t, []string{"c"}, ids)
	assert.Empty(t, token, "a page shorter than its size is the last one")

	ids, token, err = store.PersistenceIDs(ctx, 0, "")
	require.NoError(t, err)
	assert.Empty(t, ids)
	assert.Empty(t, token)
}

func TestShardQueries(t *testing.T) {
	ctx := context.Background()
	store := NewEventsStore(newTestDB(t))

	t.Run("an empty journal has no shard offset", func(t *testing.T) {
		offsets, err := store.ShardOffsets(ctx)
		require.NoError(t, err)
		assert.Empty(t, offsets)
	})

	require.NoError(t, store.WriteEvents(ctx, []*egopb.Event{
		newEvent(t, "account-b", 1, 7, 100),
		newEvent(t, "account-a", 1, 7, 100),
		newEvent(t, "account-a", 2, 7, 200),
	}))
	require.NoError(t, store.WriteEvents(ctx, []*egopb.Event{newEvent(t, "account-c", 1, 0, 50)}))
	require.NoError(t, store.WriteEvents(ctx, []*egopb.Event{
		newEvent(t, "account-d", 1, persistence.NumberOfShards-1, 70),
	}))

	t.Run("every shard holding events maps to its newest timestamp", func(t *testing.T) {
		offsets, err := store.ShardOffsets(ctx)
		require.NoError(t, err)
		assert.Equal(t, map[uint64]int64{0: 50, 7: 200, persistence.NumberOfShards - 1: 70}, offsets)
	})

	t.Run("a shard is read from an inclusive timestamp in timestamp, sequence and persistence ID order", func(t *testing.T) {
		events, err := store.EventsByShard(ctx, 7, 100, 10)
		require.NoError(t, err)
		require.Len(t, events, 3)

		assert.Equal(t, "account-a", events[0].GetPersistenceId())
		assert.Equal(t, "account-b", events[1].GetPersistenceId())
		assert.EqualValues(t, 2, events[2].GetSequenceNumber())
	})

	t.Run("a shard read honours its lower bound and limit", func(t *testing.T) {
		events, err := store.EventsByShard(ctx, 7, 101, 10)
		require.NoError(t, err)
		require.Len(t, events, 1)
		assert.EqualValues(t, 200, events[0].GetTimestamp())

		events, err = store.EventsByShard(ctx, 7, 0, 1)
		require.NoError(t, err)
		assert.Len(t, events, 1)
	})

	t.Run("an unknown shard is empty", func(t *testing.T) {
		events, err := store.EventsByShard(ctx, 9, 0, 10)
		require.NoError(t, err)
		assert.Empty(t, events)
	})
}

func TestEventsStoreQueriesUseTheIndexes(t *testing.T) {
	db := newTestDB(t)

	queries := map[string][]any{
		replayEventsQuery:   {"account-1", 1, 10, 10},
		latestEventQuery:    {"account-1"},
		eventsByShardQuery:  {1, 0, 10},
		deleteEventsQuery:   {"account-1", 1},
		persistenceIDsQuery: {"", 10},
		shardOffsetsQuery:   {persistence.NumberOfShards - 1},
	}

	for query, args := range queries {
		plan := queryPlan(t, db, query, args...)
		assert.NotContains(t, plan, "SCAN events_store", "%s reads the whole journal:\n%s", query, plan)
		assert.NotContains(t, plan, "TEMP B-TREE", "%s sorts in memory:\n%s", query, plan)
	}

	// the newest timestamp of a shard is one seek in the shard index, run once per shard
	plan := queryPlan(t, db, shardOffsetsQuery, persistence.NumberOfShards-1)
	assert.Contains(t, plan, "SEARCH events_store USING COVERING INDEX idx_events_store_shard_timestamp")
	assert.Equal(t, 1, strings.Count(plan, "CORRELATED SCALAR SUBQUERY"), plan)
}

func TestEventsStoreErrorsAreReported(t *testing.T) {
	ctx := context.Background()
	store := NewEventsStore(newClosedDB(t))

	assert.Error(t, store.Connect(ctx))
	assert.Error(t, store.Ping(ctx))
	assert.ErrorContains(t, store.WriteEvents(ctx, []*egopb.Event{newEvent(t, "account-1", 1, 1, 1)}), "failed to write the events")
	assert.ErrorContains(t, store.WriteEvents(ctx, manyEvents(t, "account-1", insertEvents.rowsPerStatement()+1)), "failed to write the events")
	assert.ErrorContains(t, store.DeleteEvents(ctx, "account-1", 1), "failed to delete the events")

	_, err := store.ReplayEvents(ctx, "account-1", 1, 10, 10)
	assert.ErrorContains(t, err, "failed to replay the events")

	_, err = store.GetLatestEvent(ctx, "account-1")
	assert.ErrorContains(t, err, "failed to read the latest event")

	_, _, err = store.PersistenceIDs(ctx, 10, "")
	assert.ErrorContains(t, err, "failed to list the persistence IDs")

	_, err = store.EventsByShard(ctx, 1, 0, 10)
	assert.ErrorContains(t, err, "failed to read the shard events")

	_, err = store.ShardOffsets(ctx)
	assert.ErrorContains(t, err, "failed to read the shard offsets")
}

func TestEventsStoreScanErrorsAreReported(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	store := NewEventsStore(db)

	// values SQLite keeps as text, since they are not numbers, cannot be scanned into a flag or a timestamp
	_, err := db.ExecContext(ctx, "INSERT INTO events_store ("+eventColumns+") VALUES ('account-1', 1, 'maybe', x'', '', 'later', 1, '', 0)")
	require.NoError(t, err)

	_, err = store.ReplayEvents(ctx, "account-1", 0, 10, 10)
	assert.Error(t, err)

	_, err = store.ShardOffsets(ctx)
	assert.Error(t, err)
}

// manyEvents creates count events of the persistence ID, numbered from one.
func manyEvents(t testing.TB, persistenceID string, count int) []*egopb.Event {
	t.Helper()

	events := make([]*egopb.Event, count)
	for index := range events {
		events[index] = newEvent(t, persistenceID, uint64(index+1), 1, int64(index+1))
	}

	return events
}

// queryPlan returns the plan SQLite runs the query with, one step per line.
func queryPlan(t *testing.T, db *sql.DB, query string, args ...any) string {
	t.Helper()

	rows, err := db.QueryContext(context.Background(), "EXPLAIN QUERY PLAN "+query, args...)
	require.NoError(t, err)
	defer rows.Close()

	var plan strings.Builder
	for rows.Next() {
		var (
			id, parent, unused int
			detail             string
		)

		require.NoError(t, rows.Scan(&id, &parent, &unused, &detail))
		fmt.Fprintln(&plan, detail)
	}

	require.NoError(t, rows.Err())
	return plan.String()
}
