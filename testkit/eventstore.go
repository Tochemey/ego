/*
 * MIT License
 *
 * Copyright (c) 2023-2025 Tochemey
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
	"sort"
	"sync"

	goset "github.com/deckarep/golang-set/v2"
	"go.uber.org/atomic"

	"github.com/tochemey/ego/v3/egopb"
	"github.com/tochemey/ego/v3/persistence"
)

type EventKey struct {
	PersistenceID  string
	SequenceNumber uint64
}

type EventStore struct {
	db        *sync.Map
	connected *atomic.Bool
}

var _ persistence.EventsStore = (*EventStore)(nil)

func NewEventsStore() *EventStore {
	return &EventStore{
		db:        &sync.Map{},
		connected: atomic.NewBool(false),
	}
}

func (x *EventStore) Connect(_ context.Context) error {
	if x.connected.Load() {
		return nil
	}
	x.connected.Store(true)
	return nil
}

func (x *EventStore) Disconnect(_ context.Context) error {
	if !x.connected.Load() {
		return nil
	}
	x.db.Range(func(key interface{}, _ interface{}) bool {
		x.db.Delete(key)
		return true
	})
	x.connected.Store(false)
	return nil
}

func (x *EventStore) WriteEvents(_ context.Context, events []*egopb.Event) error {
	for _, event := range events {
		key := EventKey{
			PersistenceID:  event.GetPersistenceId(),
			SequenceNumber: event.GetSequenceNumber(),
		}
		x.db.Store(key, event)
	}
	return nil
}

func (x *EventStore) Ping(ctx context.Context) error {
	if !x.connected.Load() {
		return x.Connect(ctx)
	}
	return nil
}

func (x *EventStore) DeleteEvents(_ context.Context, persistenceID string, toSequenceNumber uint64) error {
	x.db.Range(func(key interface{}, _ interface{}) bool {
		k := key.(EventKey)
		if k.PersistenceID == persistenceID && k.SequenceNumber <= toSequenceNumber {
			x.db.Delete(key)
		}
		return true
	})
	return nil
}

func (x *EventStore) ReplayEvents(_ context.Context, persistenceID string, fromSequenceNumber, toSequenceNumber uint64, max uint64) ([]*egopb.Event, error) {
	var events []*egopb.Event
	collect := func(key, value interface{}) bool {
		k, ok := key.(EventKey)
		if !ok {
			return true // Skip if the key is not of type EventKey
		}

		if k.PersistenceID == persistenceID &&
			k.SequenceNumber >= fromSequenceNumber &&
			k.SequenceNumber <= toSequenceNumber {
			events = append(events, value.(*egopb.Event))
			if len(events) >= int(max) {
				return false // Stop further iteration
			}
		}
		return true
	}

	x.db.Range(collect)
	return events, nil
}

func (x *EventStore) GetLatestEvent(_ context.Context, persistenceID string) (*egopb.Event, error) {
	var events []*egopb.Event
	x.db.Range(func(key interface{}, value interface{}) bool {
		k := key.(EventKey)
		if k.PersistenceID == persistenceID {
			events = append(events, value.(*egopb.Event))
		}
		return true
	})

	sort.SliceStable(events, func(i, j int) bool {
		return events[i].SequenceNumber > events[j].SequenceNumber
	})

	if len(events) == 0 {
		return nil, nil
	}
	return events[0], nil
}

func (x *EventStore) PersistenceIDs(_ context.Context, pageSize uint64, pageToken string) (persistenceIDs []string, nextPageToken string, err error) {
	// step 1: collect unique PersistenceIDs using a map
	idSet := make(map[string]struct{})
	x.db.Range(func(key, _ interface{}) bool {
		k := key.(EventKey)
		idSet[k.PersistenceID] = struct{}{}
		return true
	})

	// step 2: convert the map to a slice and sort it
	keys := make([]string, 0, len(idSet))
	for id := range idSet {
		keys = append(keys, id)
	}
	sort.Strings(keys)

	// step 3: paginate the sorted keys
	startIndex := 0
	if pageToken != "" {
		// Find the index of the pageToken in the sorted keys
		for i, key := range keys {
			if key > pageToken {
				startIndex = i
				break
			}
		}
	}

	// Collect up to pageSize items starting from startIndex
	endIndex := startIndex + int(pageSize)
	if endIndex > len(keys) {
		endIndex = len(keys)
	}
	persistenceIDs = keys[startIndex:endIndex]

	// step 4: determine the nextPageToken
	switch {
	case endIndex < len(keys):
		nextPageToken = keys[endIndex]
	default:
		nextPageToken = ""
	}

	return persistenceIDs, nextPageToken, nil
}

func (x *EventStore) GetShardEvents(_ context.Context, shardNumber uint64, offset int64, max uint64) ([]*egopb.Event, int64, error) {
	var shardEvents []*egopb.Event
	x.db.Range(func(_ interface{}, value interface{}) bool {
		event := value.(*egopb.Event)
		if event.GetShard() == shardNumber {
			shardEvents = append(shardEvents, value.(*egopb.Event))
		}
		return true
	})

	if len(shardEvents) == 0 {
		return nil, 0, nil
	}

	var events []*egopb.Event
	for _, event := range shardEvents {
		if event.GetTimestamp() > offset {
			if len(events) <= int(max) {
				events = append(events, event)
			}
		}
	}

	if len(events) == 0 {
		return nil, 0, nil
	}

	sort.SliceStable(events, func(i, j int) bool {
		return events[i].GetTimestamp() <= events[j].GetTimestamp()
	})

	nextOffset := events[len(events)-1].GetTimestamp()
	return events, nextOffset, nil
}

func (x *EventStore) ShardNumbers(context.Context) ([]uint64, error) {
	shards := goset.NewSet[uint64]()
	x.db.Range(func(_ interface{}, value interface{}) bool {
		event := value.(*egopb.Event)
		shards.Add(event.GetShard())
		return true
	})
	return shards.ToSlice(), nil
}
