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

package benchmark

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/tochemey/ego/v5"
	"github.com/tochemey/ego/v5/egopb"
	samplepb "github.com/tochemey/ego/v5/example/examplepb"
	"github.com/tochemey/ego/v5/persistence"
	"github.com/tochemey/ego/v5/testkit"
)

const (
	// sagaReactionTimeout bounds how long one event may take to reach every saga.
	sagaReactionTimeout = 30 * time.Second
	// sagaTimeout keeps the benchmark sagas running for the whole benchmark.
	sagaTimeout = time.Hour
	// sagaQuietWindow is how long the journal must see no shard read for the
	// sagas to count as caught up: a new saga first reads the shards that hold the
	// other sagas' status events, then reads them again to find late events after
	// five idle pulls of one second each, so the window spans more than those.
	sagaQuietWindow = 6 * time.Second
	// sagaCatchUpTimeout bounds how long the sagas may take to catch up.
	sagaCatchUpTimeout = 2 * time.Minute
)

// countingEventsStore wraps an events store and counts the journal reads that
// projections and sagas issue.
type countingEventsStore struct {
	persistence.EventsStore
	// shardOffsets counts ShardOffsets calls.
	shardOffsets atomic.Int64
	// eventsByShard counts EventsByShard calls.
	eventsByShard atomic.Int64
}

// ShardOffsets counts the call and delegates it.
func (x *countingEventsStore) ShardOffsets(ctx context.Context) (map[uint64]int64, error) {
	x.shardOffsets.Add(1)
	return x.EventsStore.ShardOffsets(ctx)
}

// EventsByShard counts the call and delegates it.
func (x *countingEventsStore) EventsByShard(ctx context.Context, shardNumber uint64, fromTimestamp int64, limit uint64) ([]*egopb.Event, error) {
	x.eventsByShard.Add(1)
	return x.EventsStore.EventsByShard(ctx, shardNumber, fromTimestamp, limit)
}

// reads returns the number of journal reads so far.
func (x *countingEventsStore) reads() int64 {
	return x.shardOffsets.Load() + x.eventsByShard.Load()
}

// reactionSaga is a saga that reports, for every account credit it sees, how
// long ago the credit's command was sent.
type reactionSaga struct {
	// id is the saga ID.
	id string
	// sentAt holds when the latest credit command was sent, in Unix nanoseconds.
	sentAt *atomic.Int64
	// reactions receives the reaction latency of every credit.
	reactions chan<- time.Duration
}

var _ ego.SagaBehavior = (*reactionSaga)(nil)

// ID returns the saga ID.
func (x *reactionSaga) ID() string { return x.id }

// InitialState returns an empty state.
func (x *reactionSaga) InitialState() ego.State { return new(samplepb.Account) }

// HandleEvent reports the reaction latency of an account credit and keeps the saga running.
func (x *reactionSaga) HandleEvent(_ context.Context, event ego.Event, _ ego.State) (*ego.SagaAction, error) {
	if _, ok := event.(*samplepb.AccountCredited); ok {
		x.reactions <- time.Duration(time.Now().UnixNano() - x.sentAt.Load())
	}
	return nil, nil
}

// HandleResult keeps the saga running.
func (x *reactionSaga) HandleResult(context.Context, string, ego.State, ego.State) (*ego.SagaAction, error) {
	return nil, nil
}

// HandleError keeps the saga running.
func (x *reactionSaga) HandleError(context.Context, string, error, ego.State) (*ego.SagaAction, error) {
	return nil, nil
}

// ApplyEvent keeps the state unchanged.
func (x *reactionSaga) ApplyEvent(_ context.Context, _ ego.Event, state ego.State) (ego.State, error) {
	return state, nil
}

// Compensate has nothing to compensate.
func (x *reactionSaga) Compensate(context.Context, ego.State) ([]ego.SagaCommand, error) {
	return nil, nil
}

// MarshalBinary encodes the saga ID.
func (x *reactionSaga) MarshalBinary() ([]byte, error) {
	return json.Marshal(struct {
		ID string `json:"id"`
	}{ID: x.id})
}

// UnmarshalBinary decodes the saga ID.
func (x *reactionSaga) UnmarshalBinary(data []byte) error {
	aux := struct {
		ID string `json:"id"`
	}{}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	x.id = aux.ID
	return nil
}

// BenchmarkSagaFanout measures what running many sagas on one node costs. Every
// saga follows the whole journal, so each iteration writes one event and waits
// until every saga has seen it. It reports the journal reads one event costs
// and the time from a command to each saga's reaction.
func BenchmarkSagaFanout(b *testing.B) {
	for _, sagas := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("sagas=%d", sagas), func(b *testing.B) {
			benchmarkSagaFanout(b, sagas)
		})
	}
}

// benchmarkSagaFanout runs the fan-out benchmark with the given number of sagas.
func benchmarkSagaFanout(b *testing.B, sagas int) {
	ctx := context.Background()
	store := &countingEventsStore{EventsStore: connectStore(b)}

	offsetStore := testkit.NewOffsetStore()
	if err := offsetStore.Connect(ctx); err != nil {
		b.Fatalf("connect offset store: %v", err)
	}
	b.Cleanup(func() { _ = offsetStore.Disconnect(ctx) })

	env := setupEngine(b, store, ego.WithOffsetStore(offsetStore))
	entityID := env.spawnEntity(b)

	var sentAt atomic.Int64
	reactions := make(chan time.Duration, sagas)
	for range sagas {
		saga := &reactionSaga{id: uuid.NewString(), sentAt: &sentAt, reactions: reactions}
		if err := env.engine.Saga(ctx, saga, sagaTimeout); err != nil {
			b.Fatalf("start saga: %v", err)
		}
	}

	waitUntilQuiet(b, store)

	latencies := make([]time.Duration, 0, b.N*sagas)
	readsBefore := store.reads()
	b.ResetTimer()

	for range b.N {
		sentAt.Store(time.Now().UnixNano())
		if _, _, err := env.engine.SendCommand(ctx, entityID, &samplepb.CreditAccount{AccountId: entityID, Balance: 1}, 5*time.Second); err != nil {
			b.Fatalf("credit account: %v", err)
		}

		for range sagas {
			select {
			case latency := <-reactions:
				latencies = append(latencies, latency)
			case <-time.After(sagaReactionTimeout):
				b.Fatalf("an event did not reach every saga within %s", sagaReactionTimeout)
			}
		}
	}

	b.StopTimer()

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p50 := latencies[percentileIndex(len(latencies), 50)]
	p99 := latencies[percentileIndex(len(latencies), 99)]

	b.ReportMetric(float64(store.reads()-readsBefore)/float64(b.N), "journal-reads/event")
	b.ReportMetric(float64(p50.Microseconds())/1000, "p50-reaction-ms")
	b.ReportMetric(float64(p99.Microseconds())/1000, "p99-reaction-ms")
}

// waitUntilQuiet waits until the sagas have caught up with the journal: no
// shard read for sagaQuietWindow. The once-a-second shard-offsets lookup an
// idle node makes does not count. The measurement then shows the cost of new
// events only.
func waitUntilQuiet(b *testing.B, store *countingEventsStore) {
	b.Helper()

	deadline := time.Now().Add(sagaCatchUpTimeout)
	for time.Now().Before(deadline) {
		before := store.eventsByShard.Load()
		time.Sleep(sagaQuietWindow)
		if store.eventsByShard.Load() == before {
			return
		}
	}

	b.Fatalf("the sagas did not catch up with the journal within %s", sagaCatchUpTimeout)
}
