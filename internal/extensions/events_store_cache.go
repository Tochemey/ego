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
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/tochemey/ego/v4/egopb"
	"github.com/tochemey/ego/v4/persistence"
)

const (
	// shardOffsetsCacheLifetime is how long a ShardOffsets answer is served
	// to every runner on the node before the store is asked again. It bounds
	// both the polling load, at a few round trips per second however many
	// runners the node hosts, and the extra delay before events persisted by
	// a peer node are noticed.
	shardOffsetsCacheLifetime = 250 * time.Millisecond
	// shardOffsetsFlightKey groups the concurrent ShardOffsets callers that
	// share one round trip to the store.
	shardOffsetsFlightKey = "shard-offsets"
)

// cachedEventsStore serves ShardOffsets from an answer shared by every runner
// on the node, so the polling load on the store does not grow with the number
// of projections and sagas hosted here. A local write, or a runner nudged by
// one, invalidates the answer, so events persisted on this node are noticed
// by the next pull; events persisted by peers are noticed once the answer
// expires. Every other call reaches the store untouched.
type cachedEventsStore struct {
	persistence.EventsStore

	// group merges the callers that ask the store while no fresh answer is
	// held, so they share one round trip.
	group singleflight.Group
	// mutex guards offsets and fetchedAt.
	mutex sync.Mutex
	// offsets is the last answer read from the store. It is never modified
	// once stored, only replaced, so callers may read it without the mutex.
	offsets map[uint64]int64
	// fetchedAt is when offsets was read; zero when there is no answer to
	// serve, at start and after a local write.
	fetchedAt time.Time
}

// newCachedEventsStore wraps a store so that its ShardOffsets answers are
// shared by every caller on the node.
func newCachedEventsStore(store persistence.EventsStore) *cachedEventsStore {
	return &cachedEventsStore{EventsStore: store}
}

// WriteEvents writes through to the store and drops the ShardOffsets answer,
// which the write has just made stale.
func (s *cachedEventsStore) WriteEvents(ctx context.Context, events []*egopb.Event) error {
	err := s.EventsStore.WriteEvents(ctx, events)
	s.InvalidateShardOffsets()
	return err
}

// InvalidateShardOffsets drops the shared ShardOffsets answer so the next
// caller reads the journal's current state. A runner calls it when it is
// nudged by events written on this node, whether or not that write went
// through this store.
func (s *cachedEventsStore) InvalidateShardOffsets() {
	s.mutex.Lock()
	s.fetchedAt = time.Time{}
	s.mutex.Unlock()
}

// ShardOffsets returns the shared answer while it is fresh and otherwise asks
// the store once on behalf of every caller waiting for it. The returned map is
// shared between callers and must not be modified.
func (s *cachedEventsStore) ShardOffsets(ctx context.Context) (map[uint64]int64, error) {
	if offsets, fresh := s.freshOffsets(); fresh {
		return offsets, nil
	}

	answer, err, _ := s.group.Do(shardOffsetsFlightKey, func() (any, error) {
		// A caller that queued behind a fetch which has just completed finds
		// the answer fresh and must not ask the store again.
		if offsets, fresh := s.freshOffsets(); fresh {
			return offsets, nil
		}

		offsets, err := s.EventsStore.ShardOffsets(ctx)
		if err != nil {
			return nil, err
		}

		s.mutex.Lock()
		s.offsets = offsets
		s.fetchedAt = time.Now()
		s.mutex.Unlock()

		return offsets, nil
	})
	if err != nil {
		return nil, err
	}

	return answer.(map[uint64]int64), nil
}

// freshOffsets returns the shared answer and whether it is still fresh.
func (s *cachedEventsStore) freshOffsets() (map[uint64]int64, bool) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.fetchedAt.IsZero() || time.Since(s.fetchedAt) >= shardOffsetsCacheLifetime {
		return nil, false
	}

	return s.offsets, true
}
