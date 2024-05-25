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

package memory

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/go-memdb"
	"github.com/pkg/errors"
	"go.uber.org/atomic"

	"github.com/tochemey/ego/v2/egopb"
	"github.com/tochemey/ego/v2/internal/telemetry"
	"github.com/tochemey/ego/v2/offsetstore"
)

// OffsetStore implements the offset store interface
// NOTE: NOT RECOMMENDED FOR PRODUCTION CODE because all records are in memory and there is no durability.
// This is recommended for tests or PoC
type OffsetStore struct {
	// specifies the underlying database
	db *memdb.MemDB
	// this is only useful for tests
	KeepRecordsAfterDisconnect bool
	// hold the connection state to avoid multiple connection of the same instance
	connected *atomic.Bool
}

var _ offsetstore.OffsetStore = &OffsetStore{}

// NewOffsetStore creates an instance of OffsetStore
func NewOffsetStore() *OffsetStore {
	return &OffsetStore{
		KeepRecordsAfterDisconnect: false,
		connected:                  atomic.NewBool(false),
	}
}

// Connect connects to the offset store
func (x *OffsetStore) Connect(ctx context.Context) error {
	// add a span context
	_, span := telemetry.SpanContext(ctx, "OffsetStore.Connect")
	defer span.End()

	// check whether this instance of the journal is connected or not
	if x.connected.Load() {
		return nil
	}

	// create an instance of the database
	db, err := memdb.NewMemDB(offsetSchema)
	// handle the eventual error
	if err != nil {
		return err
	}
	// set the journal store underlying database
	x.db = db

	// set the connection status
	x.connected.Store(true)

	return nil
}

// Disconnect disconnects the offset store
func (x *OffsetStore) Disconnect(ctx context.Context) error {
	// add a span context
	_, span := telemetry.SpanContext(ctx, "OffsetStore.Disconnect")
	defer span.End()

	// check whether this instance of the journal is connected or not
	if !x.connected.Load() {
		return nil
	}

	// clear all records
	if !x.KeepRecordsAfterDisconnect {
		// spawn a db transaction for read-only
		txn := x.db.Txn(true)

		// free memory resource
		if _, err := txn.DeleteAll(offsetTableName, offsetPK); err != nil {
			txn.Abort()
			return errors.Wrap(err, "failed to free memory resource")
		}
		txn.Commit()
	}
	// set the connection status
	x.connected.Store(false)

	return nil
}

// Ping verifies a connection to the database is still alive, establishing a connection if necessary.
func (x *OffsetStore) Ping(ctx context.Context) error {
	// add a span context
	spanCtx, span := telemetry.SpanContext(ctx, "OffsetStore.Ping")
	defer span.End()

	// check whether we are connected or not
	if !x.connected.Load() {
		return x.Connect(spanCtx)
	}

	return nil
}

// WriteOffset writes an offset to the offset store
func (x *OffsetStore) WriteOffset(ctx context.Context, offset *egopb.Offset) error {
	// add a span context
	_, span := telemetry.SpanContext(ctx, "OffsetStore.WriteOffset")
	defer span.End()

	// check whether this instance of the journal is connected or not
	if !x.connected.Load() {
		return errors.New("offset store is not connected")
	}

	// spawn a db transaction
	txn := x.db.Txn(true)

	// create an offset row
	record := &offsetRow{
		Ordering:       uuid.NewString(),
		ProjectionName: offset.GetProjectionName(),
		ShardNumber:    offset.GetShardNumber(),
		Value:          offset.GetValue(),
		Timestamp:      offset.GetTimestamp(),
	}

	// persist the record
	if err := txn.Insert(offsetTableName, record); err != nil {
		// abort the transaction
		txn.Abort()
		// return the error
		return errors.Wrap(err, "failed to persist offset record on to the offset store")
	}
	// commit the transaction
	txn.Commit()

	return nil
}

// GetCurrentOffset return the offset of a projection
func (x *OffsetStore) GetCurrentOffset(ctx context.Context, projectionID *egopb.ProjectionId) (current *egopb.Offset, err error) {
	// add a span context
	_, span := telemetry.SpanContext(ctx, "OffsetStore.GetCurrentOffset")
	defer span.End()

	// check whether this instance of the journal is connected or not
	if !x.connected.Load() {
		return nil, errors.New("offset store is not connected")
	}

	// spawn a db transaction for read-only
	txn := x.db.Txn(false)
	defer txn.Abort()
	// let us fetch the last record
	raw, err := txn.Last(offsetTableName, rowIndex, projectionID.GetProjectionName(), projectionID.GetShardNumber())
	if err != nil {
		// if the error is not found then return nil
		if errors.Is(err, memdb.ErrNotFound) {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "failed to get the current offset for shard=%d given projection=%s",
			projectionID.GetShardNumber(), projectionID.GetProjectionName())
	}

	// no record found
	if raw == nil {
		return nil, nil
	}

	// cast the record
	if offsetRow, ok := raw.(*offsetRow); ok {
		current = &egopb.Offset{
			ShardNumber:    offsetRow.ShardNumber,
			ProjectionName: offsetRow.ProjectionName,
			Value:          offsetRow.Value,
			Timestamp:      offsetRow.Timestamp,
		}
		return
	}

	return nil, fmt.Errorf("failed to get the current offset for shard=%d given projection=%s",
		projectionID.GetShardNumber(), projectionID.GetProjectionName())
}

// ResetOffset resets the offset of given projection to a given value across all shards
func (x *OffsetStore) ResetOffset(ctx context.Context, projectionName string, value int64) error {
	// add a span context
	_, span := telemetry.SpanContext(ctx, "offsetStore.ResetOffset")
	defer span.End()

	// check whether this instance of the offset store is connected or not
	if !x.connected.Load() {
		return errors.New("offset store is not connected")
	}

	// spawn a db transaction for read-only
	txn := x.db.Txn(false)
	// fetch all the records for the given projection
	it, err := txn.Get(offsetTableName, projectionNameIndex, projectionName)
	// handle the error
	if err != nil {
		// abort the transaction
		txn.Abort()
		return errors.Wrap(err, "failed to fetch the list of shard number")
	}

	// loop over the records
	var offsetRows []*offsetRow
	for row := it.Next(); row != nil; row = it.Next() {
		if journal, ok := row.(*offsetRow); ok {
			offsetRows = append(offsetRows, journal)
		}
	}
	//  let us abort the transaction after fetching the matching records
	txn.Abort()

	// update the records
	ts := time.Now().UnixMilli()
	for _, row := range offsetRows {
		row.Value = value
		row.Timestamp = ts
	}

	// spawn a db write transaction
	txn = x.db.Txn(true)
	// iterate the list of offset rows and update the values
	for _, row := range offsetRows {
		// persist the record
		if err := txn.Insert(offsetTableName, row); err != nil {
			// abort the transaction
			txn.Abort()
			// return the error
			return errors.Wrap(err, "failed to persist offset record on to the offset store")
		}
	}

	// commit the transaction
	txn.Commit()

	return nil
}
