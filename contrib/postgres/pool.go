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

// Package postgres implements the eGo events, snapshot, durable state and
// offset stores on PostgreSQL with pgx.
//
// The stores run on a pool the application provides, built with pgxpool.New
// or pgxpool.NewWithConfig, or any other Pool implementation such as an
// instrumented wrapper. The application owns the pool: no store ever closes
// it, so one pool can back the four stores and the application's own queries.
// The tables are defined by the SQL files of the module's resources folder.
package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// maxPreallocatedRows bounds the capacity a read reserves up front from its
// limit, so a large limit does not allocate memory the result never fills.
const maxPreallocatedRows = 1024

// Pool is the part of *pgxpool.Pool the stores use.
//
// The stores accept any Pool: a *pgxpool.Pool built with pgxpool.New or
// pgxpool.NewWithConfig, or a wrapper that adds tracing or metrics. The
// application owns the pool and closes it once every store using it has been
// disconnected; no store ever closes it.
type Pool interface {
	// Exec runs a statement that returns no rows.
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	// Query runs a statement that returns rows.
	Query(ctx context.Context, sql string, arguments ...any) (pgx.Rows, error)
	// QueryRow runs a statement that returns at most one row.
	QueryRow(ctx context.Context, sql string, arguments ...any) pgx.Row
	// Ping verifies the database is reachable.
	Ping(ctx context.Context) error
}

// enforce the interface implementation
var _ Pool = (*pgxpool.Pool)(nil)

// connect verifies the database of a store is reachable.
func connect(ctx context.Context, pool Pool) error {
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("postgres: failed to reach the database: %w", err)
	}

	return nil
}

// capacity returns the capacity to reserve for a read of at most limit rows.
func capacity(limit uint64) int {
	return int(min(limit, maxPreallocatedRows))
}
