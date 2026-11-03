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
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/tochemey/ego/v5/egopb"
	"github.com/tochemey/ego/v5/projection"
)

func TestStoresShareOneHandle(t *testing.T) {
	resetTables(t)
	ctx := context.Background()

	events := NewEventsStore(testDB)
	snapshots := NewSnapshotStore(testDB)
	states := NewDurableStore(testDB)
	offsets := NewOffsetStore(testDB)

	for _, store := range []interface {
		Connect(ctx context.Context) error
	}{events, snapshots, states, offsets} {
		require.NoError(t, store.Connect(ctx))
	}

	require.NoError(t, events.WriteEvents(ctx, []*egopb.Event{newEvent(t, "account-1", 1, 1, 100)}))
	require.NoError(t, snapshots.WriteSnapshot(ctx, &egopb.Snapshot{PersistenceId: "account-1", SequenceNumber: 1, State: newPayload(t, "account-1")}))
	require.NoError(t, states.WriteState(ctx, &egopb.DurableState{PersistenceId: "account-2", VersionNumber: 1, ResultingState: newPayload(t, "account-2")}))
	require.NoError(t, offsets.WriteOffsets(ctx, projectionName, []*egopb.EntityOffset{newOffset(1, "account-1", 1, 100, 1000)}))

	// the application queries the same handle
	var rows int
	require.NoError(t, testDB.QueryRowContext(ctx,
		"SELECT (SELECT COUNT(*) FROM events_store) + (SELECT COUNT(*) FROM snapshots_store) + "+
			"(SELECT COUNT(*) FROM states_store) + (SELECT COUNT(*) FROM offsets_store)").Scan(&rows))
	assert.Equal(t, 4, rows)

	// disconnecting the stores leaves the handle to its owner
	for _, store := range []interface {
		Disconnect(ctx context.Context) error
	}{events, snapshots, states, offsets} {
		require.NoError(t, store.Disconnect(ctx))
	}

	require.NoError(t, testDB.PingContext(ctx))
}

func TestTransactionalHandler(t *testing.T) {
	resetTables(t)
	ctx := context.Background()
	offsets := NewOffsetStore(testDB)

	_, err := testDB.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS balances (account_id VARCHAR(255) PRIMARY KEY, events BIGINT NOT NULL)")
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = testDB.ExecContext(ctx, "DROP TABLE balances") })

	handler := &countingHandler{db: testDB, offsets: offsets}
	var _ projection.TransactionalHandler = handler

	t.Run("the effect and the offset are committed together", func(t *testing.T) {
		offset := newOffset(1, "account-1", 1, 100, 1000)
		require.NoError(t, handler.Handle(ctx, "account-1", newPayload(t, "account-1"), 1, offset))

		assert.Equal(t, 1, handler.count(t, "account-1"))

		recorded, err := offsets.ReadEntityOffset(ctx, projectionName, 1, "account-1")
		require.NoError(t, err)
		assert.EqualValues(t, 1, recorded.GetSequenceNumber())
	})

	t.Run("a failed effect records no offset", func(t *testing.T) {
		handler.fail = true
		offset := newOffset(1, "account-1", 2, 200, 2000)
		require.Error(t, handler.Handle(ctx, "account-1", newPayload(t, "account-1"), 2, offset))

		assert.Equal(t, 1, handler.count(t, "account-1"))

		recorded, err := offsets.ReadEntityOffset(ctx, projectionName, 1, "account-1")
		require.NoError(t, err)
		assert.EqualValues(t, 1, recorded.GetSequenceNumber())
	})

	t.Run("a failed offset write is reported", func(t *testing.T) {
		tx, err := testDB.BeginTx(ctx, nil)
		require.NoError(t, err)
		require.NoError(t, tx.Rollback())

		err = offsets.WriteOffsetsTx(ctx, tx, projectionName, []*egopb.EntityOffset{newOffset(1, "account-1", 3, 300, 3000)})
		assert.ErrorContains(t, err, "mysql: failed to write the offsets")
	})
}

// errEffectFailed is the error countingHandler returns when told to fail.
var errEffectFailed = errors.New("the effect failed")

// countingHandler counts the events of each account in the balances table and records
// each event's offset in the same transaction.
type countingHandler struct {
	// db is the handle shared with the offset store.
	db *sql.DB
	// offsets records the offsets within the handler's transaction.
	offsets *OffsetStore
	// fail makes Handle fail after applying its effect, before committing.
	fail bool
}

// Handle applies the event and records its offset in one transaction.
func (h *countingHandler) Handle(ctx context.Context, persistenceID string, _ *anypb.Any, _ uint64, offset *egopb.EntityOffset) error {
	tx, err := h.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx,
		"INSERT INTO balances (account_id, events) VALUES (?, 1) ON DUPLICATE KEY UPDATE events = events + 1",
		persistenceID); err != nil {
		return err
	}

	if err := h.offsets.WriteOffsetsTx(ctx, tx, projectionName, []*egopb.EntityOffset{offset}); err != nil {
		return err
	}

	if h.fail {
		return errEffectFailed
	}

	return tx.Commit()
}

// count returns the number of events applied to the account.
func (h *countingHandler) count(t *testing.T, accountID string) int {
	t.Helper()

	var count int
	require.NoError(t, h.db.QueryRowContext(context.Background(), "SELECT events FROM balances WHERE account_id = ?", accountID).Scan(&count))
	return count
}

func BenchmarkWriteEvents(b *testing.B) {
	resetTables(b)
	ctx := context.Background()
	store := NewEventsStore(testDB)

	const eventsPerWrite = 100

	payload := newPayload(b, "account")
	events := make([]*egopb.Event, eventsPerWrite)
	for index := range events {
		events[index] = &egopb.Event{SequenceNumber: uint64(index + 1), Event: payload, Shard: uint64(index % 8), Timestamp: int64(index)}
	}

	b.ReportAllocs()

	for iteration := 0; b.Loop(); iteration++ {
		// every write is the next entity's first events
		persistenceID := fmt.Sprintf("account-%d", iteration)
		for _, event := range events {
			event.PersistenceId = persistenceID
		}

		if err := store.WriteEvents(ctx, events); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportMetric(float64(b.N*eventsPerWrite)/b.Elapsed().Seconds(), "events/s")
}

func BenchmarkEventsByShard(b *testing.B) {
	resetTables(b)
	ctx := context.Background()
	store := NewEventsStore(testDB)

	const (
		journalSize = 10_000
		shards      = 8
		pageSize    = 100
	)

	events := make([]*egopb.Event, journalSize)
	for index := range events {
		events[index] = newEvent(b, fmt.Sprintf("account-%d", index%500), uint64(index/500+1), uint64(index%shards), int64(index))
	}

	require.NoError(b, store.WriteEvents(ctx, events))

	b.ReportAllocs()

	for b.Loop() {
		page, err := store.EventsByShard(ctx, 3, journalSize/2, pageSize)
		if err != nil || len(page) != pageSize {
			b.Fatal(err, len(page))
		}
	}
}
