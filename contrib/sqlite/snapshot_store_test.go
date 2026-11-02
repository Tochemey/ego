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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/tochemey/ego/v5/egopb"
)

func TestSnapshotStoreConnection(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	store := NewSnapshotStore(db)

	require.NoError(t, store.Connect(ctx))
	require.NoError(t, store.Ping(ctx))
	require.NoError(t, store.Disconnect(ctx))
	require.NoError(t, db.PingContext(ctx))
}

func TestSnapshots(t *testing.T) {
	ctx := context.Background()
	store := NewSnapshotStore(newTestDB(t))

	snapshots := make([]*egopb.Snapshot, 3)
	for index := range snapshots {
		snapshots[index] = newSnapshot(t, "account-1", uint64(index+1))
		require.NoError(t, store.WriteSnapshot(ctx, snapshots[index]))
	}

	t.Run("the latest snapshot has the highest sequence number", func(t *testing.T) {
		latest, err := store.GetLatestSnapshot(ctx, "account-1")
		require.NoError(t, err)
		assert.True(t, proto.Equal(snapshots[2], latest))
	})

	t.Run("an unknown entity has no snapshot", func(t *testing.T) {
		latest, err := store.GetLatestSnapshot(ctx, "unknown")
		require.NoError(t, err)
		assert.Nil(t, latest)
	})

	t.Run("a snapshot of the same sequence number replaces the stored one", func(t *testing.T) {
		replacement := newSnapshot(t, "account-1", 3)
		replacement.State = newPayload(t, "replacement")
		replacement.IsEncrypted = false
		require.NoError(t, store.WriteSnapshot(ctx, replacement))

		latest, err := store.GetLatestSnapshot(ctx, "account-1")
		require.NoError(t, err)
		assert.True(t, proto.Equal(replacement, latest))
	})

	t.Run("snapshots are deleted up to a sequence number", func(t *testing.T) {
		require.NoError(t, store.DeleteSnapshots(ctx, "account-1", 2))

		latest, err := store.GetLatestSnapshot(ctx, "account-1")
		require.NoError(t, err)
		assert.EqualValues(t, 3, latest.GetSequenceNumber())

		require.NoError(t, store.DeleteSnapshots(ctx, "account-1", 3))

		latest, err = store.GetLatestSnapshot(ctx, "account-1")
		require.NoError(t, err)
		assert.Nil(t, latest)
	})
}

func TestSnapshotStoreErrorsAreReported(t *testing.T) {
	ctx := context.Background()
	store := NewSnapshotStore(newClosedDB(t))

	assert.Error(t, store.Connect(ctx))
	assert.Error(t, store.Ping(ctx))
	assert.ErrorContains(t, store.WriteSnapshot(ctx, newSnapshot(t, "account-1", 1)), "failed to write the snapshot")
	assert.ErrorContains(t, store.DeleteSnapshots(ctx, "account-1", 1), "failed to delete the snapshots")

	_, err := store.GetLatestSnapshot(ctx, "account-1")
	assert.ErrorContains(t, err, "failed to read the latest snapshot")
}

// newSnapshot creates a snapshot of the persistence ID at the given sequence number.
func newSnapshot(t *testing.T, persistenceID string, sequenceNumber uint64) *egopb.Snapshot {
	t.Helper()

	return &egopb.Snapshot{
		PersistenceId:   persistenceID,
		SequenceNumber:  sequenceNumber,
		State:           newPayload(t, persistenceID),
		Timestamp:       int64(sequenceNumber) * 10,
		EncryptionKeyId: "key-1",
		IsEncrypted:     true,
	}
}
