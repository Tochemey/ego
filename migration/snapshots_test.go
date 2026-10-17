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
	testpb "github.com/tochemey/ego/v5/test/data/testpb"
	"github.com/tochemey/ego/v5/testkit"
)

// connectedSnapshotStore returns a connected snapshot store disconnected when the test ends.
func connectedSnapshotStore(t *testing.T) *testkit.SnapshotStore {
	t.Helper()
	ctx := context.Background()

	store := testkit.NewSnapshotStore()
	require.NoError(t, store.Connect(ctx))
	t.Cleanup(func() { _ = store.Disconnect(ctx) })
	return store
}

// accountSnapshot builds a snapshot of the persistence ID at the sequence number.
func accountSnapshot(t *testing.T, persistenceID string, sequenceNumber uint64) *egopb.Snapshot {
	t.Helper()

	state, err := anypb.New(&testpb.Account{AccountId: persistenceID, AccountBalance: float64(sequenceNumber)})
	require.NoError(t, err)
	return &egopb.Snapshot{PersistenceId: persistenceID, SequenceNumber: sequenceNumber, State: state}
}

// snapshotSource is a SnapshotReader over a mocked snapshot store that lists
// the given persistence IDs.
type snapshotSource struct {
	*mockspersistence.SnapshotStore
	pagedLister
}

// mockedSnapshotSource returns a snapshotSource listing account-1.
func mockedSnapshotSource() snapshotSource {
	return snapshotSource{
		SnapshotStore: new(mockspersistence.SnapshotStore),
		pagedLister:   pagedLister{persistenceIDs: []string{"account-1"}},
	}
}

func TestSnapshotMigrator(t *testing.T) {
	ctx := context.Background()

	t.Run("copies the latest snapshot of every entity of the snapshot store", func(t *testing.T) {
		source := connectedSnapshotStore(t)
		target := connectedSnapshotStore(t)
		require.NoError(t, source.WriteSnapshot(ctx, accountSnapshot(t, "account-1", 3)))
		require.NoError(t, source.WriteSnapshot(ctx, accountSnapshot(t, "account-1", 7)))
		require.NoError(t, source.WriteSnapshot(ctx, accountSnapshot(t, "account-2", 2)))
		require.NoError(t, source.WriteSnapshot(ctx, accountSnapshot(t, "account-3", 1)))

		migrator := NewSnapshotMigrator(source, target, WithPageSize(1), WithParallelism(2), WithLogger(log.DiscardLogger))
		require.NoError(t, migrator.Run(ctx))

		for persistenceID, sequenceNumber := range map[string]uint64{"account-1": 7, "account-2": 2, "account-3": 1} {
			latest, err := target.GetLatestSnapshot(ctx, persistenceID)
			require.NoError(t, err)
			assert.EqualValues(t, sequenceNumber, latest.GetSequenceNumber(), persistenceID)
		}
	})

	t.Run("copies the snapshot of an entity whose events were all deleted", func(t *testing.T) {
		// an entity idle since its last snapshot has no event left once its
		// retention policy ran: the copy must not depend on the journal
		source := connectedSnapshotStore(t)
		target := connectedSnapshotStore(t)
		require.NoError(t, source.WriteSnapshot(ctx, accountSnapshot(t, "account-9", 5)))

		require.NoError(t, NewSnapshotMigrator(source, target, WithLogger(log.DiscardLogger)).Run(ctx))

		latest, err := target.GetLatestSnapshot(ctx, "account-9")
		require.NoError(t, err)
		assert.EqualValues(t, 5, latest.GetSequenceNumber())
	})

	t.Run("leaves a target snapshot that is as recent alone", func(t *testing.T) {
		source := mockedSnapshotSource()
		target := new(mockspersistence.SnapshotStore)
		source.EXPECT().Ping(mock.Anything).Return(nil)
		target.EXPECT().Ping(mock.Anything).Return(nil)
		source.EXPECT().GetLatestSnapshot(mock.Anything, "account-1").Return(accountSnapshot(t, "account-1", 4), nil)
		target.EXPECT().GetLatestSnapshot(mock.Anything, "account-1").Return(accountSnapshot(t, "account-1", 4), nil)

		require.NoError(t, NewSnapshotMigrator(source, target, WithLogger(log.DiscardLogger)).Run(ctx))
		target.AssertNotCalled(t, "WriteSnapshot", mock.Anything, mock.Anything)
	})

	t.Run("skips a persistence ID whose snapshots were deleted meanwhile", func(t *testing.T) {
		source := mockedSnapshotSource()
		target := new(mockspersistence.SnapshotStore)
		source.EXPECT().Ping(mock.Anything).Return(nil)
		target.EXPECT().Ping(mock.Anything).Return(nil)
		source.EXPECT().GetLatestSnapshot(mock.Anything, "account-1").Return(nil, nil)

		require.NoError(t, NewSnapshotMigrator(source, target, WithLogger(log.DiscardLogger)).Run(ctx))
		target.AssertNotCalled(t, "GetLatestSnapshot", mock.Anything, mock.Anything)
	})
}

func TestSnapshotMigratorFailures(t *testing.T) {
	ctx := context.Background()

	// stores returns a mocked source listing account-1 and a mocked target,
	// both reachable.
	stores := func() (snapshotSource, *mockspersistence.SnapshotStore) {
		source := mockedSnapshotSource()
		source.EXPECT().Ping(mock.Anything).Return(nil).Maybe()
		target := new(mockspersistence.SnapshotStore)
		target.EXPECT().Ping(mock.Anything).Return(nil).Maybe()
		return source, target
	}

	run := func(source SnapshotReader, target *mockspersistence.SnapshotStore) error {
		return NewSnapshotMigrator(source, target, WithLogger(log.DiscardLogger)).Run(ctx)
	}

	t.Run("when the source is not reachable", func(t *testing.T) {
		source := mockedSnapshotSource()
		source.EXPECT().Ping(mock.Anything).Return(assert.AnError)

		err := run(source, new(mockspersistence.SnapshotStore))
		require.ErrorIs(t, err, assert.AnError)
		assert.ErrorContains(t, err, "source snapshot store not reachable")
	})

	t.Run("when the target is not reachable", func(t *testing.T) {
		source := mockedSnapshotSource()
		source.EXPECT().Ping(mock.Anything).Return(nil)
		target := new(mockspersistence.SnapshotStore)
		target.EXPECT().Ping(mock.Anything).Return(assert.AnError)

		err := run(source, target)
		require.ErrorIs(t, err, assert.AnError)
		assert.ErrorContains(t, err, "target snapshot store not reachable")
	})

	t.Run("when the source snapshot cannot be read", func(t *testing.T) {
		source, target := stores()
		source.EXPECT().GetLatestSnapshot(mock.Anything, "account-1").Return(nil, assert.AnError)

		err := run(source, target)
		require.ErrorIs(t, err, assert.AnError)
		assert.ErrorContains(t, err, "failed to copy the snapshot of account-1")
	})

	t.Run("when the target snapshot cannot be read", func(t *testing.T) {
		source, target := stores()
		source.EXPECT().GetLatestSnapshot(mock.Anything, "account-1").Return(accountSnapshot(t, "account-1", 1), nil)
		target.EXPECT().GetLatestSnapshot(mock.Anything, "account-1").Return(nil, assert.AnError)

		require.ErrorIs(t, run(source, target), assert.AnError)
	})

	t.Run("when the snapshot cannot be written", func(t *testing.T) {
		source, target := stores()
		source.EXPECT().GetLatestSnapshot(mock.Anything, "account-1").Return(accountSnapshot(t, "account-1", 1), nil)
		target.EXPECT().GetLatestSnapshot(mock.Anything, "account-1").Return(nil, nil)
		target.EXPECT().WriteSnapshot(mock.Anything, mock.Anything).Return(assert.AnError)

		require.ErrorIs(t, run(source, target), assert.AnError)
	})
}
