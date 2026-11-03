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

package dynamodb

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/tochemey/ego/v5/egopb"
)

func TestSnapshotStore(t *testing.T) {
	ctx := context.Background()
	store := NewSnapshotStore(createSnapshotsTable(t), testClient)
	require.NoError(t, store.Connect(ctx))

	t.Run("replaces a snapshot written at the same sequence number", func(t *testing.T) {
		require.NoError(t, store.WriteSnapshot(ctx, newSnapshot(t, "account-1", 1, "opened")))
		replacement := newSnapshot(t, "account-1", 1, "credited")
		require.NoError(t, store.WriteSnapshot(ctx, replacement))

		snapshot, err := store.GetLatestSnapshot(ctx, "account-1")
		require.NoError(t, err)
		assert.True(t, proto.Equal(replacement, snapshot), "got %v", snapshot)
	})

	t.Run("keeps the type of a state of a message with no field set", func(t *testing.T) {
		state, err := anypb.New(new(emptypb.Empty))
		require.NoError(t, err)

		empty := &egopb.Snapshot{PersistenceId: "account-empty", SequenceNumber: 1, State: state, Timestamp: 100}
		require.NoError(t, store.WriteSnapshot(ctx, empty))

		snapshot, err := store.GetLatestSnapshot(ctx, "account-empty")
		require.NoError(t, err)
		assert.True(t, proto.Equal(empty, snapshot), "got %v", snapshot)
	})

	t.Run("stores a snapshot whose state has no value", func(t *testing.T) {
		valueless := &egopb.Snapshot{PersistenceId: "account-nil", SequenceNumber: 1, State: &anypb.Any{TypeUrl: "type.googleapis.com/account.State"}, Timestamp: 100}
		require.NoError(t, store.WriteSnapshot(ctx, valueless))

		snapshot, err := store.GetLatestSnapshot(ctx, "account-nil")
		require.NoError(t, err)
		assert.True(t, proto.Equal(valueless, snapshot), "got %v", snapshot)
	})

	t.Run("deletes more snapshots than one batch request holds", func(t *testing.T) {
		for sequenceNumber := uint64(1); sequenceNumber <= 30; sequenceNumber++ {
			require.NoError(t, store.WriteSnapshot(ctx, newSnapshot(t, "account-many", sequenceNumber, "state")))
		}

		require.NoError(t, store.DeleteSnapshots(ctx, "account-many", 30))

		snapshot, err := store.GetLatestSnapshot(ctx, "account-many")
		require.NoError(t, err)
		assert.Nil(t, snapshot)
	})

	require.NoError(t, store.Disconnect(ctx))
}

func TestSnapshotStoreFailures(t *testing.T) {
	ctx := context.Background()

	t.Run("fails on a missing table", func(t *testing.T) {
		store := NewSnapshotStore(missingTable, testClient)

		assert.Error(t, store.Connect(ctx))
		assert.ErrorContains(t, store.WriteSnapshot(ctx, newSnapshot(t, "account-1", 1, "opened")), "failed to write the snapshot")
		assert.ErrorContains(t, store.DeleteSnapshots(ctx, "account-1", 1), "failed to delete the snapshots")

		_, err := store.GetLatestSnapshot(ctx, "account-1")
		assert.ErrorContains(t, err, "failed to read the latest snapshot")
	})

	t.Run("fails to decode a malformed snapshot", func(t *testing.T) {
		tableName := createSnapshotsTable(t)
		_, err := testClient.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				partitionKey:            stringValue("account-1"),
				sequenceNumberAttribute: unsignedValue(1),
				statePayloadAttribute:   stringValue("not binary"),
			},
		})
		require.NoError(t, err)

		_, err = NewSnapshotStore(tableName, testClient).GetLatestSnapshot(ctx, "account-1")
		assert.ErrorContains(t, err, "failed to decode the snapshot")
	})
}

// newSnapshot builds a snapshot of the persistence ID whose state is a string value.
func newSnapshot(t *testing.T, persistenceID string, sequenceNumber uint64, value string) *egopb.Snapshot {
	t.Helper()

	state, err := anypb.New(wrapperspb.String(value))
	require.NoError(t, err)

	return &egopb.Snapshot{
		PersistenceId:   persistenceID,
		SequenceNumber:  sequenceNumber,
		State:           state,
		Timestamp:       int64(sequenceNumber) * 100,
		EncryptionKeyId: "key-1",
		IsEncrypted:     true,
	}
}
