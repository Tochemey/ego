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

// Package sqlite provides the eGo stores backed by SQLite: an events store, a
// snapshot store, a durable state store and an offset store.
//
// Every store runs on a *sql.DB the application opens once with NewDB and
// shares between the stores and its own queries. The stores never close it:
// the application closes it after the engine stops. The tables are created by
// the scripts in the module's resources directory.
//
// Payloads are stored as the type URL and the bytes of their anypb.Any, so a
// store never needs the payload's message type to read it back.
package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	// register the pure Go SQLite driver, which needs no cgo
	_ "modernc.org/sqlite"
)

const (
	// driverName is the name modernc.org/sqlite registers with database/sql.
	driverName = "sqlite"
	// defaultBusyTimeout is how long a connection waits for the write lock when Config.BusyTimeout is not set.
	defaultBusyTimeout = 5 * time.Second
	// journalMode is write-ahead logging: readers keep reading while a write is in flight.
	journalMode = "WAL"
	// synchronousMode flushes at checkpoints rather than on every commit, which write-ahead
	// logging makes durable across a process crash; a power loss can lose the latest commits.
	synchronousMode = "NORMAL"
	// transactionLock makes every transaction take the write lock when it begins, so a
	// writer waits for the busy timeout instead of failing when another writer holds the lock.
	transactionLock = "immediate"
	// maxBoundParameters is the largest number of parameters SQLite binds in one statement.
	maxBoundParameters = 32766
	// maxPreallocatedRows caps the capacity reserved for the rows of a read, whatever its limit.
	maxPreallocatedRows = 1024
)

// ErrDBPathRequired is returned by NewDB when Config.DBPath is empty.
var ErrDBPathRequired = errors.New("sqlite: the database path is required")

// Config defines how NewDB opens the SQLite database.
type Config struct {
	// DBPath is the path of the database file, for instance "/var/lib/ego/ego.db".
	// The file is created when it does not exist; its directory must exist and be writable. Required.
	DBPath string
	// BusyTimeout is how long a connection waits for the write lock another connection holds
	// before failing. SQLite allows one writer at a time, so concurrent writes wait in turn.
	// Defaults to 5 seconds.
	BusyTimeout time.Duration
}

// NewDB opens the SQLite database the stores share and verifies it is reachable.
//
// Every connection runs in write-ahead logging mode with synchronous=NORMAL and the
// configured busy timeout, and every transaction begins IMMEDIATE, taking the write lock
// upfront. SQLite allows a single writer: with these settings concurrent writers wait for
// each other instead of failing with SQLITE_BUSY, and readers are never blocked by a writer.
//
// The returned handle belongs to the caller. Pass it to NewEventsStore, NewSnapshotStore,
// NewDurableStore and NewOffsetStore, use it for the application's own queries and
// transactions, and close it once the stores are no longer used.
func NewDB(ctx context.Context, config *Config) (*sql.DB, error) {
	if config.DBPath == "" {
		return nil, ErrDBPathRequired
	}

	busyTimeout := config.BusyTimeout
	if busyTimeout <= 0 {
		busyTimeout = defaultBusyTimeout
	}

	db, err := sql.Open(driverName, dataSourceName(config.DBPath, busyTimeout))
	if err != nil {
		return nil, fmt.Errorf("sqlite: failed to open %s: %w", config.DBPath, err)
	}

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sqlite: failed to open %s: %w", config.DBPath, err)
	}

	return db, nil
}

// execer runs a statement on a database handle or within a transaction.
type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// scanner reads the columns of one row, from a *sql.Row or a *sql.Rows.
type scanner interface {
	Scan(dest ...any) error
}

// insertStatement is a multi-row INSERT into one table.
type insertStatement struct {
	// prefix is the statement up to its VALUES keyword.
	prefix string
	// columns is the number of columns each row binds.
	columns int
	// suffix follows the rows, for instance a conflict clause.
	suffix string
}

// write inserts the rows whose column values args lists, row after row. Rows that fit
// in one statement are inserted by that statement alone; more are inserted by several
// statements within one transaction, so the write stays atomic.
func (x insertStatement) write(ctx context.Context, db *sql.DB, args []any) error {
	if len(args) <= x.rowsPerStatement()*x.columns {
		return x.exec(ctx, db, args)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	defer func() { _ = tx.Rollback() }()

	if err := x.exec(ctx, tx, args); err != nil {
		return err
	}

	return tx.Commit()
}

// exec inserts the rows whose column values args lists with as few statements as
// SQLite's parameter limit allows, run by the given handle or transaction.
func (x insertStatement) exec(ctx context.Context, exec execer, args []any) error {
	chunk := x.rowsPerStatement() * x.columns
	for start := 0; start < len(args); start += chunk {
		end := min(start+chunk, len(args))
		if _, err := exec.ExecContext(ctx, x.query((end-start)/x.columns), args[start:end]...); err != nil {
			return err
		}
	}

	return nil
}

// rowsPerStatement is the number of rows one statement inserts at most.
func (x insertStatement) rowsPerStatement() int {
	return maxBoundParameters / x.columns
}

// query builds the statement inserting the given number of rows.
func (x insertStatement) query(rows int) string {
	row := "(" + strings.Repeat("?, ", x.columns-1) + "?)"

	var builder strings.Builder
	builder.Grow(len(x.prefix) + rows*(len(row)+2) + len(x.suffix))
	builder.WriteString(x.prefix)

	for index := range rows {
		if index > 0 {
			builder.WriteString(", ")
		}

		builder.WriteString(row)
	}

	builder.WriteString(x.suffix)
	return builder.String()
}

// dataSourceName builds the modernc.org/sqlite connection string of the database file
// with the pragmas every connection runs with. The path is escaped, so a ?, # or % in
// it names the file rather than starting the parameters or an escape sequence.
func dataSourceName(path string, busyTimeout time.Duration) string {
	return "file:" + (&url.URL{Path: path}).EscapedPath() +
		"?_pragma=journal_mode(" + journalMode + ")" +
		"&_pragma=synchronous(" + synchronousMode + ")" +
		"&_pragma=busy_timeout(" + strconv.FormatInt(busyTimeout.Milliseconds(), 10) + ")" +
		"&_txlock=" + transactionLock
}

// collect scans every row of a query with scan, reserving room for capacity rows.
func collect[T any](rows *sql.Rows, err error, capacity uint64, scan func(scanner) (T, error)) ([]T, error) {
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	items := make([]T, 0, min(capacity, maxPreallocatedRows))
	for rows.Next() {
		item, err := scan(rows)
		if err != nil {
			return nil, err
		}

		items = append(items, item)
	}

	return items, rows.Err()
}

// collectShardTimestamps reads the rows of a query returning a shard number and a timestamp
// into a map from shard number to timestamp.
func collectShardTimestamps(rows *sql.Rows, err error) (map[uint64]int64, error) {
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	timestamps := make(map[uint64]int64)
	for rows.Next() {
		var (
			shardNumber uint64
			timestamp   int64
		)

		if err := rows.Scan(&shardNumber, &timestamp); err != nil {
			return nil, err
		}

		timestamps[shardNumber] = timestamp
	}

	return timestamps, rows.Err()
}

// blob returns payload, or an empty payload when it is nil: SQLite stores a nil slice as NULL.
func blob(payload []byte) []byte {
	if payload == nil {
		return []byte{}
	}

	return payload
}
