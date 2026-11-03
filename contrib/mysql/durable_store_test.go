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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tochemey/ego/v5/egopb"
	"github.com/tochemey/ego/v5/persistence"
)

func TestDurableStoreConnection(t *testing.T) {
	ctx := context.Background()
	store := NewDurableStore(testDB)

	require.NoError(t, store.Connect(ctx))
	require.NoError(t, store.Disconnect(ctx))
	require.NoError(t, store.Ping(ctx), "Disconnect must leave the application's handle open")
}

func TestDurableStoreTombstones(t *testing.T) {
	resetTables(t)
	ctx := context.Background()
	store := NewDurableStore(testDB)
	before := time.Now().UnixNano()

	for _, persistenceID := range []string{"account-with-state", "account-without-state"} {
		if persistenceID == "account-with-state" {
			state := &egopb.DurableState{PersistenceId: persistenceID, VersionNumber: 1, ResultingState: newPayload(t, persistenceID), Timestamp: 1, Shard: 7}
			require.NoError(t, store.WriteState(ctx, state))
		}

		require.NoError(t, store.DeleteState(ctx, persistenceID, 2))

		state, err := store.GetLatestState(ctx, persistenceID)
		require.NoError(t, err)
		require.NotNil(t, state)
		assert.Equal(t, persistence.ShardFor(persistenceID), state.GetShard())
		assert.GreaterOrEqual(t, state.GetTimestamp(), before)
	}
}

func TestDurableStoreErrorsAreReported(t *testing.T) {
	ctx := context.Background()
	store := NewDurableStore(newClosedDB(t))

	assert.Error(t, store.Connect(ctx))
	assert.Error(t, store.Ping(ctx))
	assert.ErrorContains(t, store.WriteState(ctx, &egopb.DurableState{PersistenceId: "account-1", VersionNumber: 1, ResultingState: newPayload(t, "account-1")}), "mysql: failed to write the durable state")
	assert.ErrorContains(t, store.DeleteState(ctx, "account-1", 0), "mysql: failed to delete the durable state")
	assert.ErrorContains(t, store.DeleteState(ctx, "account-1", 1), "mysql: failed to delete the durable state")

	_, err := store.GetLatestState(ctx, "account-1")
	assert.ErrorContains(t, err, "mysql: failed to read the durable state")
}
