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
	"context"
	"fmt"
	"sort"
	"sync"

	"go.uber.org/atomic"

	"github.com/tochemey/ego/v5/egopb"
	"github.com/tochemey/ego/v5/persistence"
)

type EventKey struct {
	PersistenceID  string
	SequenceNumber uint64
}

type EventStore struct {
	db        *sync.Map
	connected *atomic.Bool
	// writeMutex makes every WriteEvents call atomic: its duplicate check and
	// its stores happen without another write in between.
	writeMutex sync.Mutex
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

// WriteEvents stores the events atomically. It stores nothing and returns an error when any
// of the events is already stored.
func (x *EventStore) WriteEvents(_ context.Context, events []*egopb.Event) error {
	x.writeMutex.Lock()
	defer x.writeMutex.Unlock()

	for _, event := range events {
		key := keyOf(event)
		if _, exists := x.db.Load(key); exists {
			return fmt.Errorf("event %d of %s is already stored", key.SequenceNumber, key.PersistenceID)
		}
	}

	for _, event := range events {
		x.db.Store(keyOf(event), event)
	}
	return nil
}

// keyOf returns the key the event is stored under.
func keyOf(event *egopb.Event) EventKey {
	return EventKey{
		PersistenceID:  event.GetPersistenceId(),
		SequenceNumber: event.GetSequenceNumber(),
	}
}

func (x *EventStore) Ping(ctx context.Context) error {
	if !x.connected.Load() {
		return x.Connect(ctx)
	}
	return nil
}

func (x *EventStore) DeleteEvents(_ context.Context, persistenceID string, toSequenceNumber uint64) error {
	x.db.Range(func(key interface{}, _ any) bool {
		k := key.(EventKey)
		if k.PersistenceID == persistenceID && k.SequenceNumber <= toSequenceNumber {
			x.db.Delete(key)
		}
		return true
	})
	return nil
}

func (x *EventStore) ReplayEvents(_ context.Context, persistenceID string, fromSequenceNumber, toSequenceNumber uint64, limit uint64) ([]*egopb.Event, error) {
	var events []*egopb.Event
	x.db.Range(func(key, value any) bool {
		k, ok := key.(EventKey)
		if !ok {
			return true
		}

		if k.PersistenceID == persistenceID &&
			k.SequenceNumber >= fromSequenceNumber &&
			k.SequenceNumber <= toSequenceNumber {
			events = append(events, value.(*egopb.Event))
		}
		return true
	})

	sort.SliceStable(events, func(i, j int) bool {
		return events[i].GetSequenceNumber() < events[j].GetSequenceNumber()
	})

	if len(events) > int(limit) {
		events = events[:int(limit)]
	}

	return events, nil
}

func (x *EventStore) GetLatestEvent(_ context.Context, persistenceID string) (*egopb.Event, error) {
	var events []*egopb.Event
	x.db.Range(func(key any, value any) bool {
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
	x.db.Range(func(key, _ any) bool {
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
	persistenceIDs, nextPageToken = pageOf(keys, pageSize, pageToken)
	return persistenceIDs, nextPageToken, nil
}

// EventsByShard returns at most limit events of the shard stamped at or after fromTimestamp,
// ordered by timestamp, then by sequence number, then by persistence ID.
func (x *EventStore) EventsByShard(_ context.Context, shardNumber uint64, fromTimestamp int64, limit uint64) ([]*egopb.Event, error) {
	var events []*egopb.Event
	x.db.Range(func(_ any, value any) bool {
		event := value.(*egopb.Event)
		if event.GetShard() == shardNumber && event.GetTimestamp() >= fromTimestamp {
			events = append(events, event)
		}
		return true
	})

	// The map iterates in no particular order: sort before cutting to the
	// limit, so a limited read returns the oldest events.
	sort.SliceStable(events, func(i, j int) bool {
		if events[i].GetTimestamp() != events[j].GetTimestamp() {
			return events[i].GetTimestamp() < events[j].GetTimestamp()
		}

		if events[i].GetSequenceNumber() != events[j].GetSequenceNumber() {
			return events[i].GetSequenceNumber() < events[j].GetSequenceNumber()
		}
		return events[i].GetPersistenceId() < events[j].GetPersistenceId()
	})

	if uint64(len(events)) > limit {
		events = events[:limit]
	}

	return events, nil
}

func (x *EventStore) ShardOffsets(context.Context) (map[uint64]int64, error) {
	offsets := make(map[uint64]int64)
	x.db.Range(func(_ any, value any) bool {
		event := value.(*egopb.Event)
		if event.GetTimestamp() > offsets[event.GetShard()] {
			offsets[event.GetShard()] = event.GetTimestamp()
		}
		return true
	})
	return offsets, nil
}

// pageOf returns the page of the sorted persistence IDs that starts after pageToken, the last
// persistence ID of the previous page, and the token of the next page: empty for the last one.
func pageOf(sortedIDs []string, pageSize uint64, pageToken string) (persistenceIDs []string, nextPageToken string) {
	startIndex := 0
	if pageToken != "" {
		startIndex = sort.Search(len(sortedIDs), func(i int) bool { return sortedIDs[i] > pageToken })
	}

	endIndex := min(startIndex+int(pageSize), len(sortedIDs))
	if endIndex > startIndex && endIndex < len(sortedIDs) {
		nextPageToken = sortedIDs[endIndex-1]
	}

	return sortedIDs[startIndex:endIndex], nextPageToken
}
