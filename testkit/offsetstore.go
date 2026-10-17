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

package testkit

import (
	"cmp"
	"context"
	"slices"
	"sync"

	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"

	"github.com/tochemey/ego/v5/egopb"
	"github.com/tochemey/ego/v5/offsetstore"
)

// OffsetStore is an in-memory offsetstore.OffsetStore for tests. It keeps every
// offset written, as a SQL-backed store does, until DeleteOffsetsBefore,
// ResetOffset or DeleteOffset removes it.
type OffsetStore struct {
	// mutex guards offsets and startOffsets.
	mutex sync.RWMutex
	// offsets holds the offsets of every projection, keyed by projection name.
	offsets map[string][]*egopb.EntityOffset
	// startOffsets holds the start offset ResetOffset recorded, keyed by projection name.
	startOffsets map[string]int64
	// connected reports whether Connect was called.
	connected *atomic.Bool
}

// enforces compilation error
var _ offsetstore.OffsetStore = (*OffsetStore)(nil)

// NewOffsetStore creates an empty, disconnected OffsetStore.
func NewOffsetStore() *OffsetStore {
	return &OffsetStore{
		offsets:      make(map[string][]*egopb.EntityOffset),
		startOffsets: make(map[string]int64),
		connected:    atomic.NewBool(false),
	}
}

// Connect connects the store. Connecting a connected store does nothing.
func (x *OffsetStore) Connect(context.Context) error {
	x.connected.Store(true)
	return nil
}

// Disconnect drops every offset and disconnects the store.
func (x *OffsetStore) Disconnect(context.Context) error {
	if !x.connected.Load() {
		return nil
	}

	x.mutex.Lock()
	x.offsets = make(map[string][]*egopb.EntityOffset)
	x.startOffsets = make(map[string]int64)
	x.mutex.Unlock()

	x.connected.Store(false)
	return nil
}

// Ping connects the store when it is not connected.
func (x *OffsetStore) Ping(ctx context.Context) error {
	if !x.connected.Load() {
		return x.Connect(ctx)
	}
	return nil
}

// WriteOffsets stores a copy of every given offset of the projection.
func (x *OffsetStore) WriteOffsets(_ context.Context, projectionName string, offsets []*egopb.EntityOffset) error {
	x.mutex.Lock()
	defer x.mutex.Unlock()

	for _, offset := range offsets {
		x.offsets[projectionName] = append(x.offsets[projectionName], proto.CloneOf(offset))
	}
	return nil
}

// ReadOffsets returns at most limit offsets of the projection on the shard, the newest timestamp first.
func (x *OffsetStore) ReadOffsets(_ context.Context, projectionName string, shardNumber uint64, limit uint64) ([]*egopb.EntityOffset, error) {
	x.mutex.RLock()
	defer x.mutex.RUnlock()

	var offsets []*egopb.EntityOffset
	for _, offset := range x.offsets[projectionName] {
		if offset.GetShardNumber() == shardNumber {
			offsets = append(offsets, proto.CloneOf(offset))
		}
	}

	slices.SortStableFunc(offsets, func(left, right *egopb.EntityOffset) int {
		return cmp.Compare(right.GetTimestamp(), left.GetTimestamp())
	})

	if uint64(len(offsets)) > limit {
		offsets = offsets[:limit]
	}
	return offsets, nil
}

// ReadEntityOffset returns the offset of the persistence ID with the highest sequence number, or nil.
func (x *OffsetStore) ReadEntityOffset(_ context.Context, projectionName string, shardNumber uint64, persistenceID string) (*egopb.EntityOffset, error) {
	x.mutex.RLock()
	defer x.mutex.RUnlock()

	var latest *egopb.EntityOffset
	for _, offset := range x.offsets[projectionName] {
		if offset.GetShardNumber() != shardNumber || offset.GetPersistenceId() != persistenceID {
			continue
		}

		if latest == nil || offset.GetSequenceNumber() > latest.GetSequenceNumber() {
			latest = offset
		}
	}

	if latest == nil {
		return nil, nil
	}
	return proto.CloneOf(latest), nil
}

// LatestOffsets returns the newest offset timestamp of the projection on every shard it has an offset on.
func (x *OffsetStore) LatestOffsets(_ context.Context, projectionName string) (map[uint64]int64, error) {
	x.mutex.RLock()
	defer x.mutex.RUnlock()

	latest := make(map[uint64]int64)
	for _, offset := range x.offsets[projectionName] {
		if current, ok := latest[offset.GetShardNumber()]; !ok || offset.GetTimestamp() > current {
			latest[offset.GetShardNumber()] = offset.GetTimestamp()
		}
	}
	return latest, nil
}

// DeleteOffsetsBefore deletes the offsets of the projection on the shard that are older than both bounds.
func (x *OffsetStore) DeleteOffsetsBefore(_ context.Context, projectionName string, shardNumber uint64, before, consumedBefore int64) error {
	x.mutex.Lock()
	defer x.mutex.Unlock()

	x.offsets[projectionName] = slices.DeleteFunc(x.offsets[projectionName], func(offset *egopb.EntityOffset) bool {
		return offset.GetShardNumber() == shardNumber &&
			offset.GetTimestamp() < before &&
			offset.GetConsumedAt() < consumedBefore
	})
	return nil
}

// ResetOffset deletes every offset of the projection and records value as its start offset.
func (x *OffsetStore) ResetOffset(_ context.Context, projectionName string, value int64) error {
	x.mutex.Lock()
	defer x.mutex.Unlock()

	delete(x.offsets, projectionName)
	x.startOffsets[projectionName] = value
	return nil
}

// StartOffset returns the start offset ResetOffset recorded for the projection, or zero.
func (x *OffsetStore) StartOffset(_ context.Context, projectionName string) (int64, error) {
	x.mutex.RLock()
	defer x.mutex.RUnlock()

	return x.startOffsets[projectionName], nil
}

// DeleteOffset deletes every offset and the start offset of the projection.
func (x *OffsetStore) DeleteOffset(_ context.Context, projectionName string) error {
	x.mutex.Lock()
	defer x.mutex.Unlock()

	delete(x.offsets, projectionName)
	delete(x.startOffsets, projectionName)
	return nil
}
