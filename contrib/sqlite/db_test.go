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
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tochemey/ego/v5/egopb"
)

func TestNewDB(t *testing.T) {
	ctx := context.Background()

	t.Run("the database path is required", func(t *testing.T) {
		db, err := NewDB(ctx, &Config{})
		require.ErrorIs(t, err, ErrDBPathRequired)
		assert.Nil(t, db)
	})

	t.Run("a database in a missing directory cannot be opened", func(t *testing.T) {
		db, err := NewDB(ctx, &Config{DBPath: filepath.Join(t.TempDir(), "missing", "ego.db")})
		require.Error(t, err)
		assert.Nil(t, db)
	})

	t.Run("a path with URI characters opens the file at that path", func(t *testing.T) {
		directory := filepath.Join(t.TempDir(), "odd?name#with%41")
		require.NoError(t, os.Mkdir(directory, 0o700))
		path := filepath.Join(directory, "ego.db")

		db, err := NewDB(ctx, &Config{DBPath: path})
		require.NoError(t, err)

		_, err = db.ExecContext(ctx, "CREATE TABLE probe (id INTEGER)")
		require.NoError(t, err)
		require.NoError(t, db.Close())

		assert.FileExists(t, path)
	})

	t.Run("every connection runs with the writer settings", func(t *testing.T) {
		db := newTestDB(t)

		var journal string
		require.NoError(t, db.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&journal))
		assert.Equal(t, "wal", journal)

		// synchronous=NORMAL is reported as 1
		var synchronous int
		require.NoError(t, db.QueryRowContext(ctx, "PRAGMA synchronous").Scan(&synchronous))
		assert.Equal(t, 1, synchronous)

		var busyTimeout int64
		require.NoError(t, db.QueryRowContext(ctx, "PRAGMA busy_timeout").Scan(&busyTimeout))
		assert.Equal(t, defaultBusyTimeout.Milliseconds(), busyTimeout)
	})

	t.Run("the busy timeout is configurable", func(t *testing.T) {
		db, err := NewDB(ctx, &Config{DBPath: filepath.Join(t.TempDir(), "ego.db"), BusyTimeout: time.Second})
		require.NoError(t, err)
		defer func() { _ = db.Close() }()

		var busyTimeout int64
		require.NoError(t, db.QueryRowContext(ctx, "PRAGMA busy_timeout").Scan(&busyTimeout))
		assert.EqualValues(t, 1000, busyTimeout)
	})
}

func TestInsertStatement(t *testing.T) {
	statement := insertStatement{prefix: "INSERT INTO t (a, b) VALUES ", columns: 2, suffix: " ON CONFLICT DO NOTHING"}

	assert.Equal(t, "INSERT INTO t (a, b) VALUES (?, ?) ON CONFLICT DO NOTHING", statement.query(1))
	assert.Equal(t, "INSERT INTO t (a, b) VALUES (?, ?), (?, ?), (?, ?) ON CONFLICT DO NOTHING", statement.query(3))
	assert.Equal(t, maxBoundParameters/2, statement.rowsPerStatement())
}

func TestConcurrentWritersWaitForEachOther(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	events := NewEventsStore(db)
	offsets := NewOffsetStore(db)

	const (
		writers   = 8
		perWriter = 50
	)

	var wg sync.WaitGroup
	errs := make(chan error, writers)

	for writer := range writers {
		wg.Go(func() {
			persistenceID := fmt.Sprintf("writer-%d", writer)
			for sequence := range uint64(perWriter) {
				event := newEvent(t, persistenceID, sequence+1, uint64(writer), int64(sequence))
				if err := events.WriteEvents(ctx, []*egopb.Event{event}); err != nil {
					errs <- err
					return
				}

				offset := newOffset(uint64(writer), persistenceID, sequence+1, int64(sequence), int64(sequence))
				if err := offsets.WriteOffsets(ctx, "projection", []*egopb.EntityOffset{offset}); err != nil {
					errs <- err
					return
				}
			}
		})
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err, "a concurrent writer must wait for the lock rather than fail")
	}

	var count int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM events_store").Scan(&count))
	assert.Equal(t, writers*perWriter, count)
}
