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
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/tochemey/ego/v5/egopb"
	"github.com/tochemey/ego/v5/test/data/testpb"
)

// schemaFiles are the scripts of the resources directory every test database is created with.
var schemaFiles = []string{
	"eventstore_sqlite.sql",
	"snapshotstore_sqlite.sql",
	"durablestore_sqlite.sql",
	"offsetstore_sqlite.sql",
}

// newTestDB opens a database in a directory the test removes when it ends and creates every table on it.
func newTestDB(tb testing.TB) *sql.DB {
	tb.Helper()

	db, err := NewDB(context.Background(), &Config{DBPath: filepath.Join(tb.TempDir(), "ego.db")})
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = db.Close() })

	for _, file := range schemaFiles {
		script, err := os.ReadFile(filepath.Join("resources", file))
		require.NoError(tb, err)

		_, err = db.ExecContext(context.Background(), string(script))
		require.NoError(tb, err)
	}

	return db
}

// newClosedDB returns a closed handle, on which every statement fails.
func newClosedDB(t *testing.T) *sql.DB {
	t.Helper()

	db := newTestDB(t)
	require.NoError(t, db.Close())
	return db
}

// newPayload packs an account with the given ID into an Any.
func newPayload(t testing.TB, accountID string) *anypb.Any {
	t.Helper()

	payload, err := anypb.New(&testpb.Account{AccountId: accountID, AccountBalance: 100})
	require.NoError(t, err)
	return payload
}

// newEvent creates an event of the persistence ID with the given sequence number, shard and timestamp.
func newEvent(t testing.TB, persistenceID string, sequenceNumber, shard uint64, timestamp int64) *egopb.Event {
	t.Helper()

	return &egopb.Event{
		PersistenceId:   persistenceID,
		SequenceNumber:  sequenceNumber,
		Event:           newPayload(t, persistenceID),
		Timestamp:       timestamp,
		Shard:           shard,
		EncryptionKeyId: "key-1",
		IsEncrypted:     true,
	}
}

// newOffset creates the offset of the persistence ID's event with the given sequence number.
func newOffset(shard uint64, persistenceID string, sequenceNumber uint64, timestamp, consumedAt int64) *egopb.EntityOffset {
	return &egopb.EntityOffset{
		ShardNumber:    shard,
		PersistenceId:  persistenceID,
		SequenceNumber: sequenceNumber,
		Timestamp:      timestamp,
		ConsumedAt:     consumedAt,
	}
}
