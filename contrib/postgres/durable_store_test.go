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
	"github.com/tochemey/ego/v5/persistence"
)

func TestDurableStore(t *testing.T) {
	ctx := context.Background()
	store := NewDurableStore(testPool)

	t.Run("connects, pings and disconnects", func(t *testing.T) {
		require.NoError(t, store.Connect(ctx))
		require.NoError(t, store.Ping(ctx))
		require.NoError(t, store.Disconnect(ctx))
		require.NoError(t, testPool.Ping(ctx), "the store must leave the pool open")
	})

	t.Run("reads the state it wrote", func(t *testing.T) {
		resetTables(t)
		latest, err := store.GetLatestState(ctx, "account-1")
		require.NoError(t, err)
		assert.Nil(t, latest)

		state := &egopb.DurableState{
			PersistenceId:  "account-1",
			VersionNumber:  1,
			ResultingState: newState(t, "account-1", 10),
			Timestamp:      100,
			Shard:          persistence.ShardFor("account-1"),
		}
		require.NoError(t, store.WriteState(ctx, state))

		state.VersionNumber = 2
		state.ResultingState = newState(t, "account-1", 20)
		require.NoError(t, store.WriteState(ctx, state))

		latest, err = store.GetLatestState(ctx, "account-1")
		require.NoError(t, err)
		assert.True(t, proto.Equal(state, latest))
	})

	t.Run("keeps a tombstone on a versioned delete", func(t *testing.T) {
		resetTables(t)
		require.NoError(t, store.WriteState(ctx, &egopb.DurableState{
			PersistenceId:  "account-1",
			VersionNumber:  3,
			ResultingState: newState(t, "account-1", 10),
			Timestamp:      100,
			Shard:          persistence.ShardFor("account-1"),
		}))

		require.NoError(t, store.DeleteState(ctx, "account-1", 4))
		tombstone, err := store.GetLatestState(ctx, "account-1")
		require.NoError(t, err)
		require.NotNil(t, tombstone)
		assert.EqualValues(t, 4, tombstone.GetVersionNumber())
		assert.Nil(t, tombstone.GetResultingState())
		assert.Equal(t, persistence.ShardFor("account-1"), tombstone.GetShard())
		assert.Positive(t, tombstone.GetTimestamp())

		// a persistence ID with no state gets a tombstone too
		require.NoError(t, store.DeleteState(ctx, "account-2", 1))
		tombstone, err = store.GetLatestState(ctx, "account-2")
		require.NoError(t, err)
		assert.EqualValues(t, 1, tombstone.GetVersionNumber())
	})

	t.Run("removes the row on an unversioned delete", func(t *testing.T) {
		resetTables(t)
		require.NoError(t, store.WriteState(ctx, &egopb.DurableState{PersistenceId: "account-1", VersionNumber: 1, ResultingState: newState(t, "account-1", 10)}))
		require.NoError(t, store.DeleteState(ctx, "account-1", 0))

		latest, err := store.GetLatestState(ctx, "account-1")
		require.NoError(t, err)
		assert.Nil(t, latest)

		require.NoError(t, store.DeleteState(ctx, "account-1", 0), "deleting no state is not an error")
	})
}

func TestDurableStoreFailures(t *testing.T) {
	ctx := context.Background()
	store := NewDurableStore(closedPool(t))

	require.Error(t, store.Connect(ctx))
	require.Error(t, store.WriteState(ctx, &egopb.DurableState{PersistenceId: "account-1"}))
	require.Error(t, store.DeleteState(ctx, "account-1", 0))
	require.Error(t, store.DeleteState(ctx, "account-1", 1))

	_, err := store.GetLatestState(ctx, "account-1")
	require.Error(t, err)
}
