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

package migration

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/tochemey/goakt/v4/log"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/tochemey/ego/v5/egopb"
	mockspersistence "github.com/tochemey/ego/v5/mocks/persistence"
	"github.com/tochemey/ego/v5/persistence"
	testpb "github.com/tochemey/ego/v5/test/data/testpb"
	"github.com/tochemey/ego/v5/testkit"
)

// legacyShard is the shard every event of the legacy test journals is on.
const legacyShard = uint64(9)

// connectedEventsStore returns a connected events store disconnected when the test ends.
func connectedEventsStore(t *testing.T) *testkit.EventStore {
	t.Helper()
	ctx := context.Background()

	store := testkit.NewEventsStore()
	require.NoError(t, store.Connect(ctx))
	t.Cleanup(func() { _ = store.Disconnect(ctx) })
	return store
}

// legacyEvent builds an event of the persistence ID on legacyShard.
func legacyEvent(t *testing.T, persistenceID string, sequenceNumber uint64, timestamp int64) *egopb.Event {
	t.Helper()

	event, err := anypb.New(&testpb.AccountCredited{AccountId: persistenceID})
	require.NoError(t, err)

	return &egopb.Event{
		PersistenceId:  persistenceID,
		SequenceNumber: sequenceNumber,
		Event:          event,
		Timestamp:      timestamp,
		Shard:          legacyShard,
	}
}

// journalOf returns every event of the persistence ID in the store.
func journalOf(t *testing.T, store *testkit.EventStore, persistenceID string) []*egopb.Event {
	t.Helper()

	events, err := store.ReplayEvents(context.Background(), persistenceID, 1, 100, 100)
	require.NoError(t, err)
	return events
}

func TestJournalMigrator(t *testing.T) {
	ctx := context.Background()

	t.Run("copies every event on the shard of its persistence ID with increasing timestamps", func(t *testing.T) {
		source := connectedEventsStore(t)
		target := connectedEventsStore(t)
		require.NoError(t, source.WriteEvents(ctx, []*egopb.Event{
			legacyEvent(t, "account-1", 1, 100),
			// stamped before its previous event by a clock that went back
			legacyEvent(t, "account-1", 2, 90),
			legacyEvent(t, "account-1", 3, 300),
		}))
		require.NoError(t, source.WriteEvents(ctx, []*egopb.Event{legacyEvent(t, "account-2", 1, 50)}))

		migrator := NewJournalMigrator(source, target, WithPageSize(2), WithLogger(log.DiscardLogger))
		require.NoError(t, migrator.Run(ctx))

		account1 := journalOf(t, target, "account-1")
		require.Len(t, account1, 3)
		assert.Equal(t, []int64{100, 101, 300}, []int64{account1[0].GetTimestamp(), account1[1].GetTimestamp(), account1[2].GetTimestamp()})
		for _, event := range account1 {
			assert.Equal(t, persistence.ShardFor("account-1"), event.GetShard())
		}

		account2 := journalOf(t, target, "account-2")
		require.Len(t, account2, 1)
		assert.Equal(t, persistence.ShardFor("account-2"), account2[0].GetShard())
		assert.EqualValues(t, 50, account2[0].GetTimestamp())

		// the source journal is left untouched
		assert.Equal(t, legacyShard, journalOf(t, source, "account-1")[0].GetShard())
	})

	t.Run("stamps each event after the previous migrated one, across pages", func(t *testing.T) {
		source := connectedEventsStore(t)
		target := connectedEventsStore(t)
		// both later events are stamped before the first by a clock that went back
		require.NoError(t, source.WriteEvents(ctx, []*egopb.Event{
			legacyEvent(t, "account-1", 1, 100),
			legacyEvent(t, "account-1", 2, 90),
			legacyEvent(t, "account-1", 3, 95),
		}))

		migrator := NewJournalMigrator(source, target, WithPageSize(1), WithLogger(log.DiscardLogger))
		require.NoError(t, migrator.Run(ctx))

		account1 := journalOf(t, target, "account-1")
		require.Len(t, account1, 3)
		assert.Equal(t, []int64{100, 101, 102}, []int64{account1[0].GetTimestamp(), account1[1].GetTimestamp(), account1[2].GetTimestamp()})
	})

	t.Run("resumes an entity after the last event the target holds", func(t *testing.T) {
		source := connectedEventsStore(t)
		target := connectedEventsStore(t)
		require.NoError(t, source.WriteEvents(ctx, []*egopb.Event{
			legacyEvent(t, "account-1", 1, 100),
			legacyEvent(t, "account-1", 2, 200),
			legacyEvent(t, "account-1", 3, 150),
		}))

		// an interrupted run copied the first two events already
		copied := legacyEvent(t, "account-1", 1, 100)
		copied.Shard = persistence.ShardFor("account-1")
		second := legacyEvent(t, "account-1", 2, 200)
		second.Shard = copied.GetShard()
		require.NoError(t, target.WriteEvents(ctx, []*egopb.Event{copied, second}))

		require.NoError(t, NewJournalMigrator(source, target, WithLogger(log.DiscardLogger)).Run(ctx))

		account1 := journalOf(t, target, "account-1")
		require.Len(t, account1, 3)
		assert.EqualValues(t, 201, account1[2].GetTimestamp())
	})

	t.Run("copies nothing more when the target holds the whole journal", func(t *testing.T) {
		source := connectedEventsStore(t)
		target := connectedEventsStore(t)
		require.NoError(t, source.WriteEvents(ctx, []*egopb.Event{legacyEvent(t, "account-1", 1, 100)}))

		migrator := NewJournalMigrator(source, target, WithLogger(log.DiscardLogger))
		require.NoError(t, migrator.Run(ctx))
		require.NoError(t, migrator.Run(ctx))

		assert.Len(t, journalOf(t, target, "account-1"), 1)
	})

	t.Run("walks every page of persistence IDs", func(t *testing.T) {
		source := connectedEventsStore(t)
		target := connectedEventsStore(t)
		for _, persistenceID := range []string{"account-1", "account-2", "account-3"} {
			require.NoError(t, source.WriteEvents(ctx, []*egopb.Event{legacyEvent(t, persistenceID, 1, 100)}))
		}

		require.NoError(t, NewJournalMigrator(source, target, WithPageSize(1), WithLogger(log.DiscardLogger)).Run(ctx))

		for _, persistenceID := range []string{"account-1", "account-2", "account-3"} {
			assert.Len(t, journalOf(t, target, persistenceID), 1)
		}
	})
}

func TestJournalMigratorFailures(t *testing.T) {
	ctx := context.Background()

	run := func(source JournalReader, target persistence.EventsStore) error {
		return NewJournalMigrator(source, target, WithLogger(log.DiscardLogger)).Run(ctx)
	}

	t.Run("when the source is not reachable", func(t *testing.T) {
		source := new(mockspersistence.EventsStore)
		source.EXPECT().Ping(mock.Anything).Return(assert.AnError)

		err := run(source, new(mockspersistence.EventsStore))
		require.ErrorIs(t, err, assert.AnError)
		assert.ErrorContains(t, err, "source journal not reachable")
	})

	t.Run("when the target is not reachable", func(t *testing.T) {
		source := new(mockspersistence.EventsStore)
		source.EXPECT().Ping(mock.Anything).Return(nil)
		target := new(mockspersistence.EventsStore)
		target.EXPECT().Ping(mock.Anything).Return(assert.AnError)

		err := run(source, target)
		require.ErrorIs(t, err, assert.AnError)
		assert.ErrorContains(t, err, "target events store not reachable")
	})

	t.Run("when the persistence IDs cannot be listed", func(t *testing.T) {
		source := new(mockspersistence.EventsStore)
		source.EXPECT().Ping(mock.Anything).Return(nil)
		source.EXPECT().PersistenceIDs(mock.Anything, mock.Anything, "").Return(nil, "", assert.AnError)
		target := new(mockspersistence.EventsStore)
		target.EXPECT().Ping(mock.Anything).Return(nil)

		err := run(source, target)
		require.ErrorIs(t, err, assert.AnError)
		assert.ErrorContains(t, err, "failed to list persistence IDs")
	})

	// entityFailure sets up a source and a target that both reach the copy of
	// account-1.
	entityFailure := func() (*mockspersistence.EventsStore, *mockspersistence.EventsStore) {
		source := new(mockspersistence.EventsStore)
		source.EXPECT().Ping(mock.Anything).Return(nil)
		source.EXPECT().PersistenceIDs(mock.Anything, mock.Anything, "").Return([]string{"account-1"}, "", nil)
		target := new(mockspersistence.EventsStore)
		target.EXPECT().Ping(mock.Anything).Return(nil)
		return source, target
	}

	t.Run("when the latest event of the source cannot be read", func(t *testing.T) {
		source, target := entityFailure()
		source.EXPECT().GetLatestEvent(mock.Anything, "account-1").Return(nil, assert.AnError)

		err := run(source, target)
		require.ErrorIs(t, err, assert.AnError)
		assert.ErrorContains(t, err, "failed to copy the journal of account-1")
	})

	t.Run("when the latest event of the target cannot be read", func(t *testing.T) {
		source, target := entityFailure()
		source.EXPECT().GetLatestEvent(mock.Anything, "account-1").Return(legacyEvent(t, "account-1", 2, 100), nil)
		target.EXPECT().GetLatestEvent(mock.Anything, "account-1").Return(nil, assert.AnError)

		require.ErrorIs(t, run(source, target), assert.AnError)
	})

	t.Run("when the events cannot be replayed", func(t *testing.T) {
		source, target := entityFailure()
		source.EXPECT().GetLatestEvent(mock.Anything, "account-1").Return(legacyEvent(t, "account-1", 2, 100), nil)
		target.EXPECT().GetLatestEvent(mock.Anything, "account-1").Return(nil, nil)
		source.EXPECT().ReplayEvents(mock.Anything, "account-1", uint64(1), uint64(2), mock.Anything).Return(nil, assert.AnError)

		require.ErrorIs(t, run(source, target), assert.AnError)
	})

	t.Run("when the events cannot be written", func(t *testing.T) {
		source, target := entityFailure()
		source.EXPECT().GetLatestEvent(mock.Anything, "account-1").Return(legacyEvent(t, "account-1", 1, 100), nil)
		target.EXPECT().GetLatestEvent(mock.Anything, "account-1").Return(nil, nil)
		source.EXPECT().ReplayEvents(mock.Anything, "account-1", uint64(1), uint64(1), mock.Anything).Return(
			[]*egopb.Event{legacyEvent(t, "account-1", 1, 100)}, nil)
		target.EXPECT().WriteEvents(mock.Anything, mock.Anything).Return(assert.AnError)

		require.ErrorIs(t, run(source, target), assert.AnError)
	})

	t.Run("stops an entity whose journal ends before its latest event", func(t *testing.T) {
		source, target := entityFailure()
		source.EXPECT().GetLatestEvent(mock.Anything, "account-1").Return(legacyEvent(t, "account-1", 2, 100), nil)
		target.EXPECT().GetLatestEvent(mock.Anything, "account-1").Return(nil, nil)
		source.EXPECT().ReplayEvents(mock.Anything, "account-1", uint64(1), uint64(2), mock.Anything).Return(nil, nil)

		require.NoError(t, run(source, target))
		target.AssertNotCalled(t, "WriteEvents", mock.Anything, mock.Anything)
	})
}
