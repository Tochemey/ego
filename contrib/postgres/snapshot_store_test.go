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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/tochemey/ego/v5/egopb"
)

func TestSnapshotStore(t *testing.T) {
	ctx := context.Background()
	store := NewSnapshotStore(testPool)

	t.Run("connects, pings and disconnects", func(t *testing.T) {
		require.NoError(t, store.Connect(ctx))
		require.NoError(t, store.Ping(ctx))
		require.NoError(t, store.Disconnect(ctx))
		require.NoError(t, testPool.Ping(ctx), "the store must leave the pool open")
	})

	t.Run("reads the latest snapshot it wrote", func(t *testing.T) {
		resetTables(t)
		latest, err := store.GetLatestSnapshot(ctx, "account-1")
		require.NoError(t, err)
		assert.Nil(t, latest)

		first := &egopb.Snapshot{PersistenceId: "account-1", SequenceNumber: 1, State: newState(t, "account-1", 10), Timestamp: 100}
		second := &egopb.Snapshot{
			PersistenceId:   "account-1",
			SequenceNumber:  2,
			State:           newState(t, "account-1", 20),
			Timestamp:       200,
			EncryptionKeyId: "key-1",
			IsEncrypted:     true,
		}
		require.NoError(t, store.WriteSnapshot(ctx, first))
		require.NoError(t, store.WriteSnapshot(ctx, second))

		latest, err = store.GetLatestSnapshot(ctx, "account-1")
		require.NoError(t, err)
		assert.True(t, proto.Equal(second, latest))
	})

	t.Run("replaces a snapshot at the same sequence number", func(t *testing.T) {
		resetTables(t)
		require.NoError(t, store.WriteSnapshot(ctx, &egopb.Snapshot{PersistenceId: "account-1", SequenceNumber: 1, State: newState(t, "account-1", 10), Timestamp: 100}))

		replacement := &egopb.Snapshot{PersistenceId: "account-1", SequenceNumber: 1, State: newState(t, "account-1", 15), Timestamp: 150}
		require.NoError(t, store.WriteSnapshot(ctx, replacement))

		latest, err := store.GetLatestSnapshot(ctx, "account-1")
		require.NoError(t, err)
		assert.True(t, proto.Equal(replacement, latest))
	})

	t.Run("deletes the snapshots up to a sequence number", func(t *testing.T) {
		resetTables(t)
		for sequence := uint64(1); sequence <= 3; sequence++ {
			require.NoError(t, store.WriteSnapshot(ctx, &egopb.Snapshot{PersistenceId: "account-1", SequenceNumber: sequence, State: newState(t, "account-1", 1), Timestamp: 100}))
		}

		require.NoError(t, store.DeleteSnapshots(ctx, "account-1", 3))
		latest, err := store.GetLatestSnapshot(ctx, "account-1")
		require.NoError(t, err)
		assert.Nil(t, latest)
	})
}

func TestSnapshotStoreFailures(t *testing.T) {
	ctx := context.Background()
	store := NewSnapshotStore(closedPool(t))

	require.Error(t, store.Connect(ctx))
	require.Error(t, store.WriteSnapshot(ctx, &egopb.Snapshot{PersistenceId: "account-1"}))
	require.Error(t, store.DeleteSnapshots(ctx, "account-1", 1))

	_, err := store.GetLatestSnapshot(ctx, "account-1")
	require.Error(t, err)
}
