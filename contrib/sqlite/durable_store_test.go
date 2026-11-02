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
	"github.com/tochemey/ego/v5/persistence"
)

func TestDurableStoreConnection(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	store := NewDurableStore(db)

	require.NoError(t, store.Connect(ctx))
	require.NoError(t, store.Ping(ctx))
	require.NoError(t, store.Disconnect(ctx))
	require.NoError(t, db.PingContext(ctx))
}

func TestDurableStates(t *testing.T) {
	ctx := context.Background()
	store := NewDurableStore(newTestDB(t))

	t.Run("an unknown entity has no state", func(t *testing.T) {
		state, err := store.GetLatestState(ctx, "unknown")
		require.NoError(t, err)
		assert.Nil(t, state)
	})

	t.Run("a state replaces the previous one", func(t *testing.T) {
		require.NoError(t, store.WriteState(ctx, newDurableState(t, "account-1", 1)))

		second := newDurableState(t, "account-1", 2)
		second.ResultingState = newPayload(t, "second")
		require.NoError(t, store.WriteState(ctx, second))

		state, err := store.GetLatestState(ctx, "account-1")
		require.NoError(t, err)
		assert.True(t, proto.Equal(second, state))
	})

	t.Run("a deletion with a version leaves a tombstone without state", func(t *testing.T) {
		require.NoError(t, store.WriteState(ctx, newDurableState(t, "account-2", 4)))
		require.NoError(t, store.DeleteState(ctx, "account-2", 5))

		state, err := store.GetLatestState(ctx, "account-2")
		require.NoError(t, err)
		require.NotNil(t, state)
		assert.Equal(t, "account-2", state.GetPersistenceId())
		assert.EqualValues(t, 5, state.GetVersionNumber())
		assert.Nil(t, state.GetResultingState())
		assert.Equal(t, persistence.ShardFor("account-2"), state.GetShard())
		assert.Positive(t, state.GetTimestamp())
	})

	t.Run("a deletion with a version of an entity without state leaves a tombstone", func(t *testing.T) {
		require.NoError(t, store.DeleteState(ctx, "account-3", 2))

		state, err := store.GetLatestState(ctx, "account-3")
		require.NoError(t, err)
		require.NotNil(t, state)
		assert.EqualValues(t, 2, state.GetVersionNumber())
		assert.Nil(t, state.GetResultingState())
		assert.Equal(t, persistence.ShardFor("account-3"), state.GetShard())
	})

	t.Run("a state written after a tombstone replaces it", func(t *testing.T) {
		next := newDurableState(t, "account-2", 6)
		require.NoError(t, store.WriteState(ctx, next))

		state, err := store.GetLatestState(ctx, "account-2")
		require.NoError(t, err)
		assert.True(t, proto.Equal(next, state))
	})

	t.Run("a deletion with version zero removes the record", func(t *testing.T) {
		require.NoError(t, store.DeleteState(ctx, "account-1", 0))

		state, err := store.GetLatestState(ctx, "account-1")
		require.NoError(t, err)
		assert.Nil(t, state)
	})

	t.Run("deleting the state of an unknown entity is not an error", func(t *testing.T) {
		assert.NoError(t, store.DeleteState(ctx, "unknown", 0))
	})
}

func TestDurableStoreErrorsAreReported(t *testing.T) {
	ctx := context.Background()
	store := NewDurableStore(newClosedDB(t))

	assert.Error(t, store.Connect(ctx))
	assert.Error(t, store.Ping(ctx))
	assert.ErrorContains(t, store.WriteState(ctx, newDurableState(t, "account-1", 1)), "failed to write the durable state")
	assert.ErrorContains(t, store.DeleteState(ctx, "account-1", 0), "failed to delete the durable state")
	assert.ErrorContains(t, store.DeleteState(ctx, "account-1", 1), "failed to delete the durable state")

	_, err := store.GetLatestState(ctx, "account-1")
	assert.ErrorContains(t, err, "failed to read the durable state")
}

// newDurableState creates a durable state of the persistence ID at the given version.
func newDurableState(t *testing.T, persistenceID string, version uint64) *egopb.DurableState {
	t.Helper()

	return &egopb.DurableState{
		PersistenceId:  persistenceID,
		VersionNumber:  version,
		ResultingState: newPayload(t, persistenceID),
		Timestamp:      int64(version) * 10,
		Shard:          persistence.ShardFor(persistenceID),
	}
}
