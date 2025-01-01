/*
 * MIT License
 *
 * Copyright (c) 2022-2025 Arsene Tochemey Gandote
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

package testkit

import (
	"context"
	"sync"
	"time"

	"go.uber.org/atomic"

	"github.com/tochemey/ego/v3/egopb"
	"github.com/tochemey/ego/v3/offsetstore"
)

type OffsetKey struct {
	ProjectionName string
	ShardNumber    uint64
}
type OffsetStore struct {
	db        *sync.Map
	connected *atomic.Bool
}

var _ offsetstore.OffsetStore = (*OffsetStore)(nil)

func NewOffsetStore() *OffsetStore {
	return &OffsetStore{
		db:        &sync.Map{},
		connected: atomic.NewBool(false),
	}
}

func (x *OffsetStore) Connect(context.Context) error {
	if x.connected.Load() {
		return nil
	}
	x.connected.Store(true)
	return nil
}

func (x *OffsetStore) Disconnect(context.Context) error {
	if !x.connected.Load() {
		return nil
	}
	x.db.Range(func(key interface{}, value interface{}) bool {
		x.db.Delete(key)
		return true
	})
	x.connected.Store(false)
	return nil
}

func (x *OffsetStore) Ping(ctx context.Context) error {
	if !x.connected.Load() {
		return x.Connect(ctx)
	}
	return nil
}

func (x *OffsetStore) WriteOffset(_ context.Context, offset *egopb.Offset) error {
	key := OffsetKey{
		ProjectionName: offset.GetProjectionName(),
		ShardNumber:    offset.GetShardNumber(),
	}
	x.db.Store(key, offset)
	return nil
}

func (x *OffsetStore) GetCurrentOffset(_ context.Context, projectionID *egopb.ProjectionId) (currentOffset *egopb.Offset, err error) {
	key := OffsetKey{
		ProjectionName: projectionID.GetProjectionName(),
		ShardNumber:    projectionID.GetShardNumber(),
	}
	value, ok := x.db.Load(key)
	if !ok {
		return nil, nil
	}
	return value.(*egopb.Offset), nil
}

func (x *OffsetStore) ResetOffset(_ context.Context, projectionName string, value int64) error {
	ts := time.Now().UnixMilli()
	x.db.Range(func(k interface{}, v interface{}) bool {
		key := v.(OffsetKey)
		val := v.(*egopb.Offset)
		if key.ProjectionName == projectionName {
			val.Value = value
			val.Timestamp = ts
			x.db.Store(key, val)
		}
		return true
	})
	return nil
}
