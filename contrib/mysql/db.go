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

// Package mysql provides the eGo stores backed by MySQL 8.0.19 or later: an
// events store, a snapshot store, a durable state store and an offset store.
//
// Every store runs on a *sql.DB the application opens with the
// github.com/go-sql-driver/mysql driver, for instance
// sql.Open("mysql", "ego:secret@tcp(localhost:3306)/ego?interpolateParams=true"),
// and shares between the stores and its own queries. The stores never close
// it: the application closes it after the engine stops. With
// interpolateParams=true the driver sends a statement and its arguments in one
// round trip instead of preparing it first. The tables are created by the
// scripts in the module's resources directory.
//
// Payloads are stored as the type URL and the bytes of their anypb.Any, so a
// store never needs the payload's message type to read it back.
package mysql

import (
	"context"
	"database/sql"
	"strings"
)

const (
	// maxPlaceholders is the largest number of placeholders MySQL binds in one statement.
	maxPlaceholders = 65535
	// maxPreallocatedRows caps the capacity reserved for the rows of a read, whatever its limit.
	maxPreallocatedRows = 1024
)

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
	// suffix follows the rows, for instance a duplicate key clause.
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
// MySQL's placeholder limit allows, run by the given handle or transaction.
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
	return maxPlaceholders / x.columns
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

// inTransaction runs statements within one transaction, committed when statements succeeds.
func inTransaction(ctx context.Context, db *sql.DB, statements func(tx *sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	defer func() { _ = tx.Rollback() }()

	if err := statements(tx); err != nil {
		return err
	}

	return tx.Commit()
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

// blob returns payload, or an empty payload when it is nil: the driver sends a nil slice as
// NULL, which the NOT NULL payload columns reject.
func blob(payload []byte) []byte {
	if payload == nil {
		return []byte{}
	}

	return payload
}
