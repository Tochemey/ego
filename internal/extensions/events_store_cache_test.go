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

package extensions

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tochemey/ego/v4/egopb"
	"github.com/tochemey/ego/v4/persistence"
)

// countingEventsStore counts the ShardOffsets round trips made to it and
// answers with the offsets it was given.
type countingEventsStore struct {
	persistence.EventsStore
	calls   atomic.Int32
	offsets map[uint64]int64
}

func (s *countingEventsStore) ShardOffsets(context.Context) (map[uint64]int64, error) {
	s.calls.Add(1)
	return s.offsets, nil
}

func (s *countingEventsStore) WriteEvents(context.Context, []*egopb.Event) error {
	return nil
}

// TestCachedEventsStoreShardOffsets asserts that ShardOffsets round trips are
// shared by every caller on the node within the cache lifetime, that a local
// write invalidates the cache, and that everything else reaches the store.
func TestCachedEventsStoreShardOffsets(t *testing.T) {
	ctx := context.Background()
	store := &countingEventsStore{offsets: map[uint64]int64{1: 10}}
	cached := newCachedEventsStore(store)

	// Many concurrent pollers, one round trip.
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			offsets, err := cached.ShardOffsets(ctx)
			assert.NoError(t, err)
			assert.EqualValues(t, 10, offsets[1])
		}()
	}
	wg.Wait()
	assert.EqualValues(t, 1, store.calls.Load())

	// Within the cache lifetime a poller reads the cached answer.
	_, err := cached.ShardOffsets(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 1, store.calls.Load())

	// A local write means the journal changed: the next poll goes to the store.
	require.NoError(t, cached.WriteEvents(ctx, nil))
	_, err = cached.ShardOffsets(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 2, store.calls.Load())

	// Once the cache lifetime has elapsed the next poll goes to the store too.
	time.Sleep(shardOffsetsCacheLifetime + 50*time.Millisecond)
	_, err = cached.ShardOffsets(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 3, store.calls.Load())
}
