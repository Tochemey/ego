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
	"errors"
	"sync"

	"go.uber.org/atomic"

	"github.com/tochemey/ego/v3/egopb"
	"github.com/tochemey/ego/v3/persistence"
)

// StateStore keep in memory every durable state actor
// NOTE: NOT RECOMMENDED FOR PRODUCTION CODE because all records are in memory and there is no durability.
// This is recommended for tests or PoC
type StateStore struct {
	db        *sync.Map
	connected *atomic.Bool
}

// enforce compilation error
var _ persistence.StateStore = (*StateStore)(nil)

// NewStateStore creates an instance StateStore
func NewStateStore() *StateStore {
	return &StateStore{
		db:        &sync.Map{},
		connected: atomic.NewBool(false),
	}
}

// Connect connects the durable store
// nolint
func (d *StateStore) Connect(ctx context.Context) error {
	if d.connected.Load() {
		return nil
	}
	d.connected.Store(true)
	return nil
}

// Disconnect disconnect the durable store
// nolint
func (d *StateStore) Disconnect(ctx context.Context) error {
	if !d.connected.Load() {
		return nil
	}
	d.db.Range(func(key interface{}, value interface{}) bool {
		d.db.Delete(key)
		return true
	})
	d.connected.Store(false)
	return nil
}

// Ping verifies a connection to the database is still alive, establishing a connection if necessary.
func (d *StateStore) Ping(ctx context.Context) error {
	if !d.connected.Load() {
		return d.Connect(ctx)
	}
	return nil
}

// WriteState persist durable state for a given persistenceID.
// nolint
func (d *StateStore) WriteState(ctx context.Context, state *egopb.DurableState) error {
	if !d.connected.Load() {
		return errors.New("durable store is not connected")
	}
	d.db.Store(state.GetPersistenceId(), state)
	return nil
}

// GetLatestState fetches the latest durable state
// nolint
func (d *StateStore) GetLatestState(ctx context.Context, persistenceID string) (*egopb.DurableState, error) {
	if !d.connected.Load() {
		return nil, errors.New("durable store is not connected")
	}
	value, ok := d.db.Load(persistenceID)
	if !ok {
		return nil, nil
	}
	return value.(*egopb.DurableState), nil
}
