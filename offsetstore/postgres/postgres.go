/*
 * MIT License
 *
 * Copyright (c) 2022-2024 Tochemey
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"

	"github.com/tochemey/gopack/postgres"

	"github.com/tochemey/ego/v3/egopb"
	"github.com/tochemey/ego/v3/internal/telemetry"
	"github.com/tochemey/ego/v3/offsetstore"
)

var (
	columns = []string{
		"projection_name",
		"shard_number",
		"current_offset",
		"timestamp",
	}

	tableName = "offsets_store"
)

// offsetRow represent the offset entry in the offset store
type offsetRow struct {
	// ProjectionName is the projection name
	ProjectionName string
	// Shard Number
	ShardNumber uint64
	// Value is the current offset
	CurrentOffset int64
	// Specifies the last update time
	Timestamp int64
}

// OffsetStore implements the OffsetStore interface
// and helps persist events in a Postgres database
type OffsetStore struct {
	db postgres.Postgres
	sb sq.StatementBuilderType
	// insertBatchSize represents the chunk of data to bulk insert.
	// This helps avoid the postgres 65535 parameter limit.
	// This is necessary because Postgres uses a 32-bit int for binding input parameters and
	// is not able to track anything larger.
	// Note: Change this value when you know the size of data to bulk insert at once. Otherwise, you
	// might encounter the postgres 65535 parameter limit error.
	insertBatchSize int
	// hold the connection state to avoid multiple connection of the same instance
	connected *atomic.Bool
}

// ensure the complete implementation of the OffsetStore interface
var _ offsetstore.OffsetStore = (*OffsetStore)(nil)

// NewOffsetStore creates an instance of OffsetStore
func NewOffsetStore(config *postgres.Config) *OffsetStore {
	// create the underlying db connection
	db := postgres.New(config)
	return &OffsetStore{
		db:              db,
		sb:              sq.StatementBuilder.PlaceholderFormat(sq.Dollar),
		insertBatchSize: 500,
		connected:       atomic.NewBool(false),
	}
}

// Connect connects to the underlying postgres database
func (x *OffsetStore) Connect(ctx context.Context) error {
	// add a span context
	ctx, span := telemetry.SpanContext(ctx, "offsetStore.Connect")
	defer span.End()
	// check whether this instance of the journal is connected or not
	if x.connected.Load() {
		return nil
	}

	// connect to the underlying db
	if err := x.db.Connect(ctx); err != nil {
		return err
	}

	// set the connection status
	x.connected.Store(true)

	return nil
}

// Disconnect disconnects from the underlying postgres database
func (x *OffsetStore) Disconnect(ctx context.Context) error {
	// add a span context
	ctx, span := telemetry.SpanContext(ctx, "offsetStore.Disconnect")
	defer span.End()

	// check whether this instance of the journal is connected or not
	if !x.connected.Load() {
		return nil
	}

	// disconnect the underlying database
	if err := x.db.Disconnect(ctx); err != nil {
		return err
	}
	// set the connection status
	x.connected.Store(false)

	return nil
}

// WriteOffset writes an offset into the offset store
func (x *OffsetStore) WriteOffset(ctx context.Context, offset *egopb.Offset) error {
	// add a span context
	ctx, span := telemetry.SpanContext(ctx, "offsetStore.WriteOffset")
	defer span.End()

	// check whether this instance of the offset store is connected or not
	if !x.connected.Load() {
		return errors.New("offset store is not connected")
	}

	// make sure the record is defined
	if offset == nil || proto.Equal(offset, new(egopb.Offset)) {
		return errors.New("offset record is not defined")
	}

	// let us begin a database transaction to make sure we atomically write those events into the database
	tx, err := x.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	// return the error in case we are unable to get a database transaction
	if err != nil {
		return fmt.Errorf("failed to obtain a database transaction: %w", err)
	}

	var (
		query string
		args  []any
	)

	// remove existing offset
	deleteBuilder := x.sb.
		Delete(tableName).
		Where(sq.Eq{"projection_name": offset.GetProjectionName()}).
		Where(sq.Eq{"shard_number": offset.GetShardNumber()})

	// get the SQL statement to run
	query, args, err = deleteBuilder.ToSql()
	// handle the error while generating the SQL
	if err != nil {
		return fmt.Errorf("unable to build sql delete statement: %w", err)
	}

	// execute the query
	_, execErr := tx.ExecContext(ctx, query, args...)
	if execErr != nil {
		// attempt to roll back the transaction and log the error in case there is an error
		if err = tx.Rollback(); err != nil {
			return fmt.Errorf("unable to rollback db transaction: %w", err)
		}
		// return the main error
		return fmt.Errorf("failed to record events: %w", execErr)
	}

	// create the insert statement
	insertBuilder := x.sb.
		Insert(tableName).
		Columns(columns...).
		Values(
			offset.GetProjectionName(),
			offset.GetShardNumber(),
			offset.GetValue(),
			offset.GetTimestamp())

	// get the SQL statement to run
	query, args, err = insertBuilder.ToSql()
	// handle the error while generating the SQL
	if err != nil {
		return fmt.Errorf("unable to build sql insert statement: %w", err)
	}

	// insert into the table
	_, execErr = tx.ExecContext(ctx, query, args...)
	if execErr != nil {
		// attempt to roll back the transaction and log the error in case there is an error
		if err = tx.Rollback(); err != nil {
			return fmt.Errorf("unable to rollback db transaction: %w", err)
		}
		// return the main error
		return fmt.Errorf("failed to record events: %w", execErr)
	}

	// commit the transaction
	if commitErr := tx.Commit(); commitErr != nil {
		// return the commit error in case there is one
		return fmt.Errorf("failed to record events: %w", commitErr)
	}
	// every looks good
	return nil
}

// GetCurrentOffset returns the current offset of a given projection id
func (x *OffsetStore) GetCurrentOffset(ctx context.Context, projectionID *egopb.ProjectionId) (currentOffset *egopb.Offset, err error) {
	// add a span context
	ctx, span := telemetry.SpanContext(ctx, "offsetStore.GetCurrentOffset")
	defer span.End()

	// check whether this instance of the offset store is connected or not
	if !x.connected.Load() {
		return nil, errors.New("offset store is not connected")
	}

	// create the SQL statement
	statement := x.sb.
		Select(columns...).
		From(tableName).
		Where(sq.Eq{"projection_name": projectionID.GetProjectionName()}).
		Where(sq.Eq{"shard_number": projectionID.GetShardNumber()})

	// get the sql statement and the arguments
	query, args, err := statement.ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build the select sql statement: %w", err)
	}

	row := new(offsetRow)
	err = x.db.Select(ctx, row, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch the current offset from the database: %w", err)
	}

	return &egopb.Offset{
		ShardNumber:    row.ShardNumber,
		ProjectionName: row.ProjectionName,
		Value:          row.CurrentOffset,
		Timestamp:      row.Timestamp,
	}, nil
}

// ResetOffset resets the offset of given projection to a given value across all shards
func (x *OffsetStore) ResetOffset(ctx context.Context, projectionName string, value int64) error {
	// add a span context
	ctx, span := telemetry.SpanContext(ctx, "offsetStore.ResetOffset")
	defer span.End()

	// check whether this instance of the offset store is connected or not
	if !x.connected.Load() {
		return errors.New("offset store is not connected")
	}

	// let us begin a database transaction to make sure we atomically write those events into the database
	tx, err := x.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	// return the error in case we are unable to get a database transaction
	if err != nil {
		return fmt.Errorf("failed to obtain a database transaction: %w", err)
	}

	// define the current timestamp
	timestamp := time.Now().UnixMilli()

	// create the sql statement
	statement := x.sb.
		Update(tableName).
		Set("current_offset", value).
		Set("timestamp", timestamp).
		Where(sq.Eq{"projection_name": projectionName})

	// get the SQL statement to run
	query, args, err := statement.ToSql()
	// handle the error while generating the SQL
	if err != nil {
		return fmt.Errorf("unable to build sql insert statement: %w", err)
	}

	// insert into the table
	_, execErr := tx.ExecContext(ctx, query, args...)
	if execErr != nil {
		// attempt to roll back the transaction and log the error in case there is an error
		if err = tx.Rollback(); err != nil {
			return fmt.Errorf("unable to rollback db transaction: %w", err)
		}
		// return the main error
		return fmt.Errorf("failed to record events: %w", execErr)
	}

	// commit the transaction
	if commitErr := tx.Commit(); commitErr != nil {
		// return the commit error in case there is one
		return fmt.Errorf("failed to record events: %w", commitErr)
	}
	// every looks good
	return nil
}

// Ping verifies a connection to the database is still alive, establishing a connection if necessary.
func (x *OffsetStore) Ping(ctx context.Context) error {
	// add a span context
	ctx, span := telemetry.SpanContext(ctx, "offsetStore.Ping")
	defer span.End()

	// check whether we are connected or not
	if !x.connected.Load() {
		return x.Connect(ctx)
	}

	return nil
}
