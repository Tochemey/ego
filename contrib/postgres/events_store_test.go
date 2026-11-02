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

package postgres

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/tochemey/ego/v5/egopb"
	"github.com/tochemey/ego/v5/test/data/testpb"
)

const (
	benchmarkBatchSize = 100
	benchmarkShard     = 7
	benchmarkJournal   = 10000
)

func TestEventsStore(t *testing.T) {
	ctx := context.Background()
	store := NewEventsStore(testPool)

	t.Run("connects, pings and disconnects", func(t *testing.T) {
		require.NoError(t, store.Connect(ctx))
		require.NoError(t, store.Ping(ctx))
		require.NoError(t, store.Disconnect(ctx))
		require.NoError(t, testPool.Ping(ctx), "the store must leave the pool open")
	})

	t.Run("replays the events it wrote", func(t *testing.T) {
		resetTables(t)
		empty, err := anypb.New(&testpb.AccountCreated{})
		require.NoError(t, err)

		first := newEvent(t, "account-1", 1, 100)
		first.Event = empty
		second := newEvent(t, "account-1", 2, 200)
		second.EncryptionKeyId = "key-1"
		second.IsEncrypted = true
		third := newEvent(t, "account-1", 3, 300)
		third.IsDeleted = true
		require.NoError(t, store.WriteEvents(ctx, []*egopb.Event{first, second, third}))

		replayed, err := store.ReplayEvents(ctx, "account-1", 1, 3, 10)
		require.NoError(t, err)
		require.Len(t, replayed, 3)
		assert.True(t, proto.Equal(first, replayed[0]))
		assert.True(t, proto.Equal(second, replayed[1]))
		assert.True(t, proto.Equal(third, replayed[2]))

		ranged, err := store.ReplayEvents(ctx, "account-1", 2, 3, 1)
		require.NoError(t, err)
		require.Len(t, ranged, 1)
		assert.EqualValues(t, 2, ranged[0].GetSequenceNumber())
	})

	t.Run("writes nothing for no event", func(t *testing.T) {
		require.NoError(t, store.WriteEvents(ctx, nil))
	})

	t.Run("rejects a write holding a stored event", func(t *testing.T) {
		resetTables(t)
		require.NoError(t, store.WriteEvents(ctx, []*egopb.Event{newEvent(t, "account-1", 1, 100)}))

		err := store.WriteEvents(ctx, []*egopb.Event{newEvent(t, "account-1", 2, 200), newEvent(t, "account-1", 1, 100)})
		require.Error(t, err)

		latest, err := store.GetLatestEvent(ctx, "account-1")
		require.NoError(t, err)
		assert.EqualValues(t, 1, latest.GetSequenceNumber(), "the rejected write must store nothing")
	})

	t.Run("reads the latest event", func(t *testing.T) {
		resetTables(t)
		latest, err := store.GetLatestEvent(ctx, "account-1")
		require.NoError(t, err)
		assert.Nil(t, latest)

		expected := newEvent(t, "account-1", 2, 200)
		require.NoError(t, store.WriteEvents(ctx, []*egopb.Event{newEvent(t, "account-1", 1, 100), expected}))

		latest, err = store.GetLatestEvent(ctx, "account-1")
		require.NoError(t, err)
		assert.True(t, proto.Equal(expected, latest))
	})

	t.Run("deletes the events up to a sequence number", func(t *testing.T) {
		resetTables(t)
		require.NoError(t, store.WriteEvents(ctx, []*egopb.Event{
			newEvent(t, "account-1", 1, 100),
			newEvent(t, "account-1", 2, 200),
			newEvent(t, "account-1", 3, 300),
		}))
		require.NoError(t, store.DeleteEvents(ctx, "account-1", 2))

		remaining, err := store.ReplayEvents(ctx, "account-1", 1, 3, 10)
		require.NoError(t, err)
		require.Len(t, remaining, 1)
		assert.EqualValues(t, 3, remaining[0].GetSequenceNumber())
	})

	t.Run("pages through the persistence IDs", func(t *testing.T) {
		resetTables(t)
		require.NoError(t, store.WriteEvents(ctx, []*egopb.Event{
			newEvent(t, "account-1", 1, 100),
			newEvent(t, "account-1", 2, 200),
			newEvent(t, "account-2", 1, 100),
			newEvent(t, "account-3", 1, 100),
		}))

		page, token, err := store.PersistenceIDs(ctx, 2, "")
		require.NoError(t, err)
		assert.Equal(t, []string{"account-1", "account-2"}, page)
		assert.Equal(t, "account-2", token)

		page, token, err = store.PersistenceIDs(ctx, 2, token)
		require.NoError(t, err)
		assert.Equal(t, []string{"account-3"}, page)
		assert.Empty(t, token)

		page, token, err = store.PersistenceIDs(ctx, 0, "")
		require.NoError(t, err)
		assert.Empty(t, page)
		assert.Empty(t, token)
	})

	t.Run("reads a shard from a timestamp on", func(t *testing.T) {
		resetTables(t)
		events := []*egopb.Event{
			newEvent(t, "account-b", 1, 100),
			newEvent(t, "account-a", 2, 200),
			newEvent(t, "account-b", 2, 200),
			newEvent(t, "account-a", 1, 200),
			newEvent(t, "account-c", 1, 300),
		}
		for _, event := range events {
			event.Shard = benchmarkShard
		}
		require.NoError(t, store.WriteEvents(ctx, events))

		read, err := store.EventsByShard(ctx, benchmarkShard, 200, 3)
		require.NoError(t, err)
		require.Len(t, read, 3)

		// the lower bound is inclusive and ties are ordered by sequence number, then persistence ID
		assert.Equal(t, "account-a", read[0].GetPersistenceId())
		assert.EqualValues(t, 1, read[0].GetSequenceNumber())
		assert.Equal(t, "account-a", read[1].GetPersistenceId())
		assert.EqualValues(t, 2, read[1].GetSequenceNumber())
		assert.Equal(t, "account-b", read[2].GetPersistenceId())

		other, err := store.EventsByShard(ctx, benchmarkShard+1, 0, 10)
		require.NoError(t, err)
		assert.Empty(t, other)
	})

	t.Run("maps every shard to its latest timestamp", func(t *testing.T) {
		resetTables(t)
		offsets, err := store.ShardOffsets(ctx)
		require.NoError(t, err)
		assert.Empty(t, offsets)

		first := newEvent(t, "account-1", 1, 100)
		second := newEvent(t, "account-1", 2, 300)
		other := newEvent(t, "account-2", 1, 200)
		other.Shard = first.GetShard() + 1
		require.NoError(t, store.WriteEvents(ctx, []*egopb.Event{first, second, other}))

		offsets, err = store.ShardOffsets(ctx)
		require.NoError(t, err)
		assert.Equal(t, map[uint64]int64{first.GetShard(): 300, other.GetShard(): 200}, offsets)
	})
}

func TestEventsStoreFailures(t *testing.T) {
	ctx := context.Background()

	t.Run("on an unreachable database", func(t *testing.T) {
		store := NewEventsStore(closedPool(t))

		require.Error(t, store.Connect(ctx))
		require.Error(t, store.WriteEvents(ctx, []*egopb.Event{newEvent(t, "account-1", 1, 100)}))
		require.Error(t, store.DeleteEvents(ctx, "account-1", 1))

		_, err := store.ReplayEvents(ctx, "account-1", 1, 2, 10)
		require.Error(t, err)

		_, err = store.GetLatestEvent(ctx, "account-1")
		require.Error(t, err)

		_, _, err = store.PersistenceIDs(ctx, 10, "")
		require.Error(t, err)

		_, err = store.EventsByShard(ctx, 1, 0, 10)
		require.Error(t, err)

		_, err = store.ShardOffsets(ctx)
		require.Error(t, err)
	})

	t.Run("on a row it cannot read", func(t *testing.T) {
		resetTables(t)
		const insertEvent = "INSERT INTO events_store " +
			"(persistence_id, sequence_number, event_payload, event_manifest, timestamp, shard_number) " +
			"VALUES ($1, $2, '', '', 100, $3)"

		// a negative shard or sequence number does not fit the unsigned fields of an event
		insertRow(t, insertEvent, "account-1", 1, -1)
		insertRow(t, insertEvent, "account-2", -1, benchmarkShard)

		store := NewEventsStore(testPool)
		_, err := store.ReplayEvents(ctx, "account-1", 1, 2, 10)
		require.Error(t, err)

		_, err = store.EventsByShard(ctx, benchmarkShard, 0, 10)
		require.Error(t, err)
	})
}

// BenchmarkWriteEvents measures one write of benchmarkBatchSize events.
func BenchmarkWriteEvents(b *testing.B) {
	ctx := context.Background()
	resetTables(b)
	store := NewEventsStore(testPool)

	batches := make([][]*egopb.Event, b.N)
	for index := range batches {
		persistenceID := fmt.Sprintf("account-%d", index)
		batch := make([]*egopb.Event, benchmarkBatchSize)
		for sequence := range batch {
			batch[sequence] = newEvent(b, persistenceID, uint64(sequence+1), int64(sequence+1))
		}

		batches[index] = batch
	}

	b.ReportAllocs()
	b.ResetTimer()

	for index := range b.N {
		if err := store.WriteEvents(ctx, batches[index]); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkEventsByShard measures one read of benchmarkBatchSize events of a
// shard holding benchmarkJournal events.
func BenchmarkEventsByShard(b *testing.B) {
	ctx := context.Background()
	resetTables(b)
	store := NewEventsStore(testPool)

	journal := make([]*egopb.Event, benchmarkJournal)
	for index := range journal {
		journal[index] = newEvent(b, fmt.Sprintf("account-%d", index), 1, int64(index))
		journal[index].Shard = benchmarkShard
	}

	require.NoError(b, store.WriteEvents(ctx, journal))

	b.ReportAllocs()
	b.ResetTimer()

	for index := range b.N {
		fromTimestamp := int64(index % (benchmarkJournal - benchmarkBatchSize))
		events, err := store.EventsByShard(ctx, benchmarkShard, fromTimestamp, benchmarkBatchSize)
		if err != nil {
			b.Fatal(err)
		}

		if len(events) != benchmarkBatchSize {
			b.Fatalf("read %d events, want %d", len(events), benchmarkBatchSize)
		}
	}
}
