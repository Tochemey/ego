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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/tochemey/ego/v5/egopb"
)

func TestSnapshotStoreConnection(t *testing.T) {
	ctx := context.Background()
	store := NewSnapshotStore(testDB)

	require.NoError(t, store.Connect(ctx))
	require.NoError(t, store.Disconnect(ctx))
	require.NoError(t, store.Ping(ctx), "Disconnect must leave the application's handle open")
}

func TestSnapshotOfTheSameSequenceNumberReplacesTheStoredOne(t *testing.T) {
	resetTables(t)
	ctx := context.Background()
	store := NewSnapshotStore(testDB)

	first := &egopb.Snapshot{PersistenceId: "account-1", SequenceNumber: 2, State: newPayload(t, "first"), Timestamp: 100}
	second := &egopb.Snapshot{PersistenceId: "account-1", SequenceNumber: 2, State: newPayload(t, "second"), Timestamp: 200, EncryptionKeyId: "key-1", IsEncrypted: true}
	require.NoError(t, store.WriteSnapshot(ctx, first))
	require.NoError(t, store.WriteSnapshot(ctx, second))

	snapshot, err := store.GetLatestSnapshot(ctx, "account-1")
	require.NoError(t, err)
	assert.True(t, proto.Equal(second, snapshot), "got %v", snapshot)
}

func TestSnapshotStoreErrorsAreReported(t *testing.T) {
	ctx := context.Background()
	store := NewSnapshotStore(newClosedDB(t))

	assert.Error(t, store.Connect(ctx))
	assert.Error(t, store.Ping(ctx))
	assert.ErrorContains(t, store.WriteSnapshot(ctx, &egopb.Snapshot{PersistenceId: "account-1", SequenceNumber: 1, State: newPayload(t, "account-1")}), "mysql: failed to write the snapshot")
	assert.ErrorContains(t, store.DeleteSnapshots(ctx, "account-1", 1), "mysql: failed to delete the snapshots")

	_, err := store.GetLatestSnapshot(ctx, "account-1")
	assert.ErrorContains(t, err, "mysql: failed to read the latest snapshot")
}
