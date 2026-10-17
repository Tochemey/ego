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

package ego

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/tochemey/goakt/v4/log"
	noopmetric "go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/tochemey/ego/v5/egopb"
	mocksoffsetstore "github.com/tochemey/ego/v5/mocks/offsetstore"
	mockseventstore "github.com/tochemey/ego/v5/mocks/persistence"
	"github.com/tochemey/ego/v5/offsetstore"
	"github.com/tochemey/ego/v5/persistence"
	"github.com/tochemey/ego/v5/projection"
	testpb "github.com/tochemey/ego/v5/test/data/testpb"
	"github.com/tochemey/ego/v5/testkit"
)

const (
	// offsetsTestProjection names the projection of the offset tests.
	offsetsTestProjection = "offsets-test"
	// offsetsTestShard is the shard the offset tests write their events to.
	offsetsTestShard = uint64(3)
)

// delivery is one event handed to a projection handler.
type delivery struct {
	// persistenceID is the persistence ID of the event.
	persistenceID string
	// revision is the sequence number of the event.
	revision uint64
}

// deliveryLog is a projection handler that records every event it is given,
// in order.
type deliveryLog struct {
	// mutex guards deliveries.
	mutex sync.Mutex
	// deliveries holds the events handled so far, in order.
	deliveries []delivery
}

// enforces compilation error
var _ projection.Handler = (*deliveryLog)(nil)

// Handle records the event.
func (x *deliveryLog) Handle(_ context.Context, persistenceID string, _ *anypb.Any, revision uint64) error {
	x.mutex.Lock()
	defer x.mutex.Unlock()

	x.deliveries = append(x.deliveries, delivery{persistenceID: persistenceID, revision: revision})
	return nil
}

// handled returns a copy of the events handled so far.
func (x *deliveryLog) handled() []delivery {
	x.mutex.Lock()
	defer x.mutex.Unlock()

	return append([]delivery(nil), x.deliveries...)
}

// offsetsFixture is a runner over in-memory stores, driven one shard read at a
// time without its pull loop.
type offsetsFixture struct {
	// events is the journal the runner reads.
	events *testkit.EventStore
	// offsets is the offset store the runner writes.
	offsets *testkit.OffsetStore
	// handler records what the runner delivers.
	handler *deliveryLog
	// runner is the runner under test, marked running but without its loop.
	runner *projectionRunner
}

// newOffsetsFixture builds a fixture whose runner reads offsetsTestShard.
func newOffsetsFixture(t *testing.T) *offsetsFixture {
	t.Helper()
	ctx := context.Background()

	events := testkit.NewEventsStore()
	require.NoError(t, events.Connect(ctx))
	t.Cleanup(func() { _ = events.Disconnect(ctx) })

	offsets := testkit.NewOffsetStore()
	require.NoError(t, offsets.Connect(ctx))
	t.Cleanup(func() { _ = offsets.Disconnect(ctx) })

	handler := new(deliveryLog)
	runner := newProjectionRunner(offsetsTestProjection, handler, events, offsets, withLogger(log.DiscardLogger))
	runner.running.Store(true)

	return &offsetsFixture{events: events, offsets: offsets, handler: handler, runner: runner}
}

// journal writes one event of the persistence ID on offsetsTestShard.
func (x *offsetsFixture) journal(t *testing.T, persistenceID string, sequenceNumber uint64, timestamp int64) {
	t.Helper()

	event, err := anypb.New(&testpb.AccountCredited{AccountId: persistenceID})
	require.NoError(t, err)

	require.NoError(t, x.events.WriteEvents(context.Background(), []*egopb.Event{{
		PersistenceId:  persistenceID,
		SequenceNumber: sequenceNumber,
		Event:          event,
		Timestamp:      timestamp,
		Shard:          offsetsTestShard,
	}}))
}

// recordOffset writes an offset of the persistence ID as if the projection had
// processed that event before.
func (x *offsetsFixture) recordOffset(t *testing.T, persistenceID string, sequenceNumber uint64, timestamp int64) {
	t.Helper()

	require.NoError(t, x.offsets.WriteOffsets(context.Background(), offsetsTestProjection, []*egopb.EntityOffset{{
		ShardNumber:    offsetsTestShard,
		PersistenceId:  persistenceID,
		SequenceNumber: sequenceNumber,
		Timestamp:      timestamp,
		ConsumedAt:     timestamp,
	}}))
}

// read runs one read of offsetsTestShard.
func (x *offsetsFixture) read(t *testing.T) error {
	t.Helper()
	return x.runner.processShard(context.Background(), offsetsTestShard)
}

// state returns the runner's state of offsetsTestShard, or nil.
func (x *offsetsFixture) state() *shardState {
	x.runner.shardsMu.RLock()
	defer x.runner.shardsMu.RUnlock()
	return x.runner.shards[offsetsTestShard]
}

// errUncommitted is returned by a committingHandler set to fail.
var errUncommitted = errors.New("transaction rolled back")

// committingHandler is a projection.TransactionalHandler over an offset store:
// a successful Handle records the event and writes its offset, as one
// transaction would; a failing one writes nothing, as a rolled-back one does.
type committingHandler struct {
	// offsets is the store the handler writes the offsets to.
	offsets *testkit.OffsetStore
	// mutex guards committed and failures.
	mutex sync.Mutex
	// committed holds the events whose transaction committed, in order.
	committed []delivery
	// failures holds, per persistence ID, how many more attempts fail; a
	// negative count fails every attempt.
	failures map[string]int
}

// enforces compilation error
var _ projection.TransactionalHandler = (*committingHandler)(nil)

// Handle commits the event and its offset, unless the persistence ID is set to fail.
func (x *committingHandler) Handle(ctx context.Context, persistenceID string, _ *anypb.Any, revision uint64, offset *egopb.EntityOffset) error {
	x.mutex.Lock()
	defer x.mutex.Unlock()

	if remaining := x.failures[persistenceID]; remaining != 0 {
		if remaining > 0 {
			x.failures[persistenceID] = remaining - 1
		}
		return errUncommitted
	}

	if err := x.offsets.WriteOffsets(ctx, offsetsTestProjection, []*egopb.EntityOffset{offset}); err != nil {
		return err
	}

	x.committed = append(x.committed, delivery{persistenceID, revision})
	return nil
}

// committedEvents returns a copy of the committed events.
func (x *committingHandler) committedEvents() []delivery {
	x.mutex.Lock()
	defer x.mutex.Unlock()
	return slices.Clone(x.committed)
}

// newTransactionalFixture builds a fixture whose runner hands the events to a
// committingHandler over the fixture's offset store, with the given recovery.
func newTransactionalFixture(t *testing.T, recovery *projection.Recovery, failures map[string]int) (*offsetsFixture, *committingHandler) {
	t.Helper()

	fixture := newOffsetsFixture(t)
	handler := &committingHandler{offsets: fixture.offsets, failures: failures}
	fixture.runner = newTransactionalRunner(fixture, handler, recovery)
	return fixture, handler
}

// newTransactionalRunner builds a running runner over the fixture's stores that
// hands the events to the handler, as a restarted projection would.
func newTransactionalRunner(fixture *offsetsFixture, handler projection.TransactionalHandler, recovery *projection.Recovery) *projectionRunner {
	runner := newProjectionRunner(offsetsTestProjection, nil, fixture.events, fixture.offsets,
		withLogger(log.DiscardLogger),
		withRecoveryStrategy(recovery),
		withTransactionalHandler(handler))
	runner.running.Store(true)
	return runner
}

// replayRecorder is an events store that records the sequence number range of
// every ReplayEvents call.
type replayRecorder struct {
	*testkit.EventStore
	// mutex guards ranges.
	mutex sync.Mutex
	// ranges holds the from and to sequence numbers of every call, in order.
	ranges [][2]uint64
}

// ReplayEvents records the range and reads it from the underlying store.
func (x *replayRecorder) ReplayEvents(ctx context.Context, persistenceID string, fromSequenceNumber, toSequenceNumber uint64, limit uint64) ([]*egopb.Event, error) {
	x.mutex.Lock()
	x.ranges = append(x.ranges, [2]uint64{fromSequenceNumber, toSequenceNumber})
	x.mutex.Unlock()
	return x.EventStore.ReplayEvents(ctx, persistenceID, fromSequenceNumber, toSequenceNumber, limit)
}

// replayedRanges returns a copy of the recorded ranges.
func (x *replayRecorder) replayedRanges() [][2]uint64 {
	x.mutex.Lock()
	defer x.mutex.Unlock()
	return slices.Clone(x.ranges)
}

// settledPast returns a timestamp old enough for every read to return it, and
// older than the backtracking reach, so reads of it never backtrack.
func settledPast() int64 {
	return time.Now().Add(-10 * time.Minute).UnixNano()
}

// backtrackingPast returns a timestamp old enough for a normal read to return
// it, and within the reach of a backtracking read.
func backtrackingPast() int64 {
	return time.Now().Add(-time.Minute).UnixNano()
}

func TestProjectionRunnerReadsShard(t *testing.T) {
	ctx := context.Background()

	t.Run("delivers the events in order and records the last one of every persistence ID", func(t *testing.T) {
		fixture := newOffsetsFixture(t)
		base := settledPast()
		fixture.journal(t, "account-a", 1, base)
		fixture.journal(t, "account-b", 1, base+1)
		fixture.journal(t, "account-a", 2, base+2)

		require.NoError(t, fixture.read(t))

		assert.Equal(t, []delivery{{"account-a", 1}, {"account-b", 1}, {"account-a", 2}}, fixture.handler.handled())

		offsets, err := fixture.offsets.ReadOffsets(ctx, offsetsTestProjection, offsetsTestShard, 10)
		require.NoError(t, err)
		require.Len(t, offsets, 2)
		assert.Equal(t, "account-a", offsets[0].GetPersistenceId())
		assert.EqualValues(t, 2, offsets[0].GetSequenceNumber())
		assert.Equal(t, "account-b", offsets[1].GetPersistenceId())
		assert.EqualValues(t, base+2, fixture.state().latest)
	})

	t.Run("resets the idle passes of a shard with new events", func(t *testing.T) {
		fixture := newOffsetsFixture(t)
		base := settledPast()
		fixture.journal(t, "account-a", 1, base)
		state := newShardState(nil, base-1, defaultOffsetSettings().timeWindow)
		state.idlePasses = backtrackingIdlePasses - 1
		fixture.runner.shards[offsetsTestShard] = state

		require.NoError(t, fixture.read(t))

		assert.Len(t, fixture.handler.handled(), 1)
		assert.Zero(t, state.idlePasses)
	})

	t.Run("does nothing once the runner stopped", func(t *testing.T) {
		fixture := newOffsetsFixture(t)
		fixture.journal(t, "account-a", 1, settledPast())
		fixture.runner.running.Store(false)

		require.NoError(t, fixture.read(t))

		assert.Empty(t, fixture.handler.handled())
		assert.Nil(t, fixture.state())
	})

	t.Run("resumes among events sharing a timestamp without losing or repeating any", func(t *testing.T) {
		fixture := newOffsetsFixture(t)
		fixture.runner.maxBufferSize = 2
		base := settledPast()
		fixture.journal(t, "account-a", 1, base)
		fixture.journal(t, "account-b", 1, base+1)
		fixture.journal(t, "account-c", 1, base+1)

		// the first read stops among the events sharing base+1
		require.NoError(t, fixture.read(t))
		require.Len(t, fixture.handler.handled(), 2)
		assert.True(t, fixture.state().lastReadFull)

		// the next one resumes at base+1 and skips what it delivered already
		require.NoError(t, fixture.read(t))
		assert.ElementsMatch(t, []delivery{{"account-a", 1}, {"account-b", 1}, {"account-c", 1}}, fixture.handler.handled())
	})

	t.Run("delivers as many events sharing a timestamp as the buffer holds", func(t *testing.T) {
		fixture := newOffsetsFixture(t)
		fixture.runner.maxBufferSize = 2
		base := settledPast()
		fixture.journal(t, "account-a", 1, base)
		fixture.journal(t, "account-b", 1, base)

		// the first read delivers both events; the next one starts at their
		// timestamp, returns them again and finds nothing pending past them
		require.NoError(t, fixture.read(t))
		require.NoError(t, fixture.read(t))

		assert.Len(t, fixture.handler.handled(), 2)
		assert.False(t, fixture.state().lastReadFull)
	})

	t.Run("delivers an unknown entity among events sharing a timestamp that fill the buffer", func(t *testing.T) {
		fixture := newOffsetsFixture(t)
		fixture.runner.maxBufferSize = 2
		base := settledPast()
		fixture.journal(t, "account-a", 1, base)
		fixture.journal(t, "account-b", 1, base)
		fixture.journal(t, "account-c", 1, base)
		state := newShardState(nil, base, defaultOffsetSettings().timeWindow)
		state.remember("account-a", 1, base)
		fixture.runner.shards[offsetsTestShard] = state

		// the page is full and all at the position, but account-b is not
		// processed yet: the read advances
		require.NoError(t, fixture.read(t))

		assert.Equal(t, []delivery{{"account-b", 1}}, fixture.handler.handled())
	})

	t.Run("fails when more events share a timestamp than the buffer holds", func(t *testing.T) {
		fixture := newOffsetsFixture(t)
		fixture.runner.maxBufferSize = 2
		base := settledPast()
		fixture.journal(t, "account-a", 1, base)
		fixture.journal(t, "account-b", 1, base)
		fixture.journal(t, "account-c", 1, base)

		// the first read delivers two events; the next one starts at their
		// timestamp and returns them again, all processed, with the third
		// still pending past them: it cannot advance
		require.NoError(t, fixture.read(t))
		err := fixture.read(t)

		var runnerErr *projectionRunnerError
		require.ErrorAs(t, err, &runnerErr)
		assert.ErrorContains(t, err, "found 2 events on shard 3")
		assert.Nil(t, fixture.state(), "a failed read drops the shard state")
	})

	t.Run("records eGo's framework events without handing them to the handler", func(t *testing.T) {
		fixture := newOffsetsFixture(t)
		base := settledPast()
		frameworkEvent, err := anypb.New(&egopb.SagaStatusChanged{})
		require.NoError(t, err)
		require.NoError(t, fixture.events.WriteEvents(ctx, []*egopb.Event{{
			PersistenceId: "saga-1", SequenceNumber: 1, Event: frameworkEvent, Timestamp: base, Shard: offsetsTestShard,
		}}))
		fixture.journal(t, "saga-1", 2, base+1)

		require.NoError(t, fixture.read(t))

		assert.Equal(t, []delivery{{"saga-1", 2}}, fixture.handler.handled())
	})
}

func TestProjectionRunnerValidatesEvents(t *testing.T) {
	ctx := context.Background()

	t.Run("skips the events of a persistence ID it processed before a restart", func(t *testing.T) {
		fixture := newOffsetsFixture(t)
		base := settledPast()
		fixture.journal(t, "account-a", 1, base)
		fixture.journal(t, "account-a", 2, base+1)
		fixture.journal(t, "account-b", 1, base+2)
		// the projection processed account-a up to 2 before account-b's offset
		// pushed it out of the loaded offsets
		fixture.recordOffset(t, "account-a", 2, base+1)

		fixture.runner.startFloor = 0
		require.NoError(t, fixture.read(t))

		assert.Equal(t, []delivery{{"account-b", 1}}, fixture.handler.handled())
	})

	t.Run("loads the offset of a persistence ID it does not hold in memory", func(t *testing.T) {
		fixture := newOffsetsFixture(t)
		base := settledPast()
		fixture.recordOffset(t, "account-a", 5, base)
		fixture.journal(t, "account-a", 5, base)
		fixture.journal(t, "account-a", 6, base+1)

		// start the shard without any in-memory record of account-a
		fixture.runner.shards[offsetsTestShard] = newShardState(nil, base, defaultOffsetSettings().timeWindow)

		require.NoError(t, fixture.read(t))

		assert.Equal(t, []delivery{{"account-a", 6}}, fixture.handler.handled())
	})

	t.Run("accepts a later first event whose previous event no longer exists", func(t *testing.T) {
		fixture := newOffsetsFixture(t)
		// retention deleted events 1 to 9
		fixture.journal(t, "account-a", 10, settledPast())

		require.NoError(t, fixture.read(t))

		assert.Equal(t, []delivery{{"account-a", 10}}, fixture.handler.handled())
	})

	t.Run("accepts a later first event whose previous event is before the start", func(t *testing.T) {
		fixture := newOffsetsFixture(t)
		base := settledPast()
		fixture.journal(t, "account-a", 1, base-10)
		fixture.journal(t, "account-a", 2, base)
		fixture.runner.startFloor = base

		require.NoError(t, fixture.read(t))

		assert.Equal(t, []delivery{{"account-a", 2}}, fixture.handler.handled())
	})

	t.Run("replays the missed earlier events of a later first event", func(t *testing.T) {
		fixture := newOffsetsFixture(t)
		base := settledPast()
		// event 1 is behind the start, event 2 became visible after the
		// projection had passed its timestamp
		fixture.journal(t, "account-a", 1, base-10)
		fixture.journal(t, "account-a", 2, base+1)
		fixture.journal(t, "account-a", 3, base+5)
		fixture.runner.startFloor = base
		fixture.runner.shards[offsetsTestShard] = newShardState(nil, base+3, defaultOffsetSettings().timeWindow)

		require.NoError(t, fixture.read(t))

		// event 1 is recorded but not handled: it is older than the start
		assert.Equal(t, []delivery{{"account-a", 2}, {"account-a", 3}}, fixture.handler.handled())
		offset, err := fixture.offsets.ReadEntityOffset(ctx, offsetsTestProjection, offsetsTestShard, "account-a")
		require.NoError(t, err)
		assert.EqualValues(t, 3, offset.GetSequenceNumber())
	})

	t.Run("replays the missed events after a gap of a known persistence ID", func(t *testing.T) {
		fixture := newOffsetsFixture(t)
		base := settledPast()
		fixture.journal(t, "account-a", 1, base)
		fixture.journal(t, "account-b", 1, base+2)
		require.NoError(t, fixture.read(t))

		// account-a's event 2 lands behind the position, its event 3 after it
		fixture.journal(t, "account-a", 2, base+1)
		fixture.journal(t, "account-a", 3, base+3)
		require.NoError(t, fixture.read(t))

		assert.Equal(t, []delivery{{"account-a", 1}, {"account-b", 1}, {"account-a", 2}, {"account-a", 3}}, fixture.handler.handled())
	})

	t.Run("replays a gap longer than the buffer a buffer at a time", func(t *testing.T) {
		fixture := newOffsetsFixture(t)
		fixture.runner.maxBufferSize = 2
		recorder := &replayRecorder{EventStore: fixture.events}
		fixture.runner.eventsStore = recorder
		base := settledPast()
		fixture.journal(t, "account-a", 1, base)
		fixture.journal(t, "account-b", 1, base+10)
		require.NoError(t, fixture.read(t))

		// account-a's events 2 to 4 land behind the position, its event 5 after it
		for sequenceNumber := uint64(2); sequenceNumber <= 4; sequenceNumber++ {
			fixture.journal(t, "account-a", sequenceNumber, base+int64(sequenceNumber))
		}
		fixture.journal(t, "account-a", 5, base+11)
		require.NoError(t, fixture.read(t))

		assert.Equal(t, []delivery{{"account-a", 1}, {"account-b", 1}, {"account-a", 2}, {"account-a", 3}, {"account-a", 4}, {"account-a", 5}}, fixture.handler.handled())
		assert.Equal(t, [][2]uint64{{2, 3}, {4, 5}}, recorder.replayedRanges(), "the gap is read two events at a time")
	})

	t.Run("fails and keeps nothing when a missing event cannot be replayed", func(t *testing.T) {
		fixture := newOffsetsFixture(t)
		base := settledPast()
		fixture.journal(t, "account-a", 1, base)
		require.NoError(t, fixture.read(t))

		// event 2 of account-a is not in the store at all
		fixture.journal(t, "account-a", 3, base+1)
		err := fixture.read(t)

		require.Error(t, err)
		assert.ErrorContains(t, err, "1 of its events 2 to 3 are in the events store")
		var runnerErr *projectionRunnerError
		assert.False(t, errors.As(err, &runnerErr), "a missing event is retried, it does not stop the projection")
		assert.Nil(t, fixture.state())
		assert.Equal(t, []delivery{{"account-a", 1}}, fixture.handler.handled())
	})
}

func TestProjectionRunnerValidationStoreFailures(t *testing.T) {
	ctx := context.Background()
	base := settledPast()
	eventAny, err := anypb.New(&testpb.AccountCredited{})
	require.NoError(t, err)
	event := func(persistenceID string, sequenceNumber uint64, timestamp int64) *egopb.Event {
		return &egopb.Event{PersistenceId: persistenceID, SequenceNumber: sequenceNumber, Event: eventAny, Timestamp: timestamp, Shard: offsetsTestShard}
	}

	newRunner := func(eventsStore persistence.EventsStore, offsetStore offsetstore.OffsetStore) *projectionRunner {
		runner := newProjectionRunner(offsetsTestProjection, new(deliveryLog), eventsStore, offsetStore, withLogger(log.DiscardLogger))
		runner.running.Store(true)
		return runner
	}

	t.Run("fails when the offset of a persistence ID cannot be loaded", func(t *testing.T) {
		offsetStore := new(mocksoffsetstore.OffsetStore)
		offsetStore.EXPECT().ReadOffsets(mock.Anything, offsetsTestProjection, offsetsTestShard, uint64(offsetReadLimit)).Return(nil, nil)
		offsetStore.EXPECT().ReadEntityOffset(mock.Anything, offsetsTestProjection, offsetsTestShard, "account-a").Return(nil, assert.AnError)
		eventsStore := new(mockseventstore.EventsStore)
		eventsStore.EXPECT().EventsByShard(mock.Anything, offsetsTestShard, int64(0), mock.Anything).Return([]*egopb.Event{event("account-a", 1, base)}, nil)

		err := newRunner(eventsStore, offsetStore).processShard(ctx, offsetsTestShard)
		require.ErrorIs(t, err, assert.AnError)
	})

	t.Run("fails when the previous event of a later first event cannot be read", func(t *testing.T) {
		offsetStore := new(mocksoffsetstore.OffsetStore)
		offsetStore.EXPECT().ReadOffsets(mock.Anything, offsetsTestProjection, offsetsTestShard, uint64(offsetReadLimit)).Return(nil, nil)
		offsetStore.EXPECT().ReadEntityOffset(mock.Anything, offsetsTestProjection, offsetsTestShard, "account-a").Return(nil, nil)
		eventsStore := new(mockseventstore.EventsStore)
		eventsStore.EXPECT().EventsByShard(mock.Anything, offsetsTestShard, int64(0), mock.Anything).Return([]*egopb.Event{event("account-a", 2, base)}, nil)
		eventsStore.EXPECT().ReplayEvents(mock.Anything, "account-a", uint64(1), uint64(1), uint64(1)).Return(nil, assert.AnError)

		err := newRunner(eventsStore, offsetStore).processShard(ctx, offsetsTestShard)
		require.ErrorIs(t, err, assert.AnError)
	})

	t.Run("fails when the missed events cannot be replayed", func(t *testing.T) {
		offsetStore := new(mocksoffsetstore.OffsetStore)
		offsetStore.EXPECT().ReadOffsets(mock.Anything, offsetsTestProjection, offsetsTestShard, uint64(offsetReadLimit)).Return(nil, nil)
		offsetStore.EXPECT().ReadEntityOffset(mock.Anything, offsetsTestProjection, offsetsTestShard, "account-a").Return(
			&egopb.EntityOffset{ShardNumber: offsetsTestShard, PersistenceId: "account-a", SequenceNumber: 1, Timestamp: base - 1}, nil)
		eventsStore := new(mockseventstore.EventsStore)
		eventsStore.EXPECT().EventsByShard(mock.Anything, offsetsTestShard, int64(0), mock.Anything).Return([]*egopb.Event{event("account-a", 3, base)}, nil)
		eventsStore.EXPECT().ReplayEvents(mock.Anything, "account-a", uint64(2), uint64(3), uint64(2)).Return(nil, assert.AnError)

		err := newRunner(eventsStore, offsetStore).processShard(ctx, offsetsTestShard)
		require.ErrorIs(t, err, assert.AnError)
	})

	t.Run("fails when the replay is out of sequence", func(t *testing.T) {
		offsetStore := new(mocksoffsetstore.OffsetStore)
		offsetStore.EXPECT().ReadOffsets(mock.Anything, offsetsTestProjection, offsetsTestShard, uint64(offsetReadLimit)).Return(nil, nil)
		offsetStore.EXPECT().ReadEntityOffset(mock.Anything, offsetsTestProjection, offsetsTestShard, "account-a").Return(
			&egopb.EntityOffset{ShardNumber: offsetsTestShard, PersistenceId: "account-a", SequenceNumber: 1, Timestamp: base - 1}, nil)
		eventsStore := new(mockseventstore.EventsStore)
		eventsStore.EXPECT().EventsByShard(mock.Anything, offsetsTestShard, int64(0), mock.Anything).Return([]*egopb.Event{event("account-a", 4, base)}, nil)
		// a store answering with the right count but the wrong events
		eventsStore.EXPECT().ReplayEvents(mock.Anything, "account-a", uint64(2), uint64(4), uint64(3)).Return(
			[]*egopb.Event{event("account-a", 2, base-2), event("account-a", 4, base), event("account-a", 4, base)}, nil)
		offsetStore.EXPECT().WriteOffsets(mock.Anything, offsetsTestProjection, mock.Anything).Return(nil)

		err := newRunner(eventsStore, offsetStore).processShard(ctx, offsetsTestShard)
		require.Error(t, err)
		assert.ErrorContains(t, err, "its replay is out of sequence")
	})

	t.Run("fails when a replayed event cannot be validated", func(t *testing.T) {
		offsetStore := new(mocksoffsetstore.OffsetStore)
		offsetStore.EXPECT().ReadOffsets(mock.Anything, offsetsTestProjection, offsetsTestShard, uint64(offsetReadLimit)).Return(nil, nil)
		offsetStore.EXPECT().ReadEntityOffset(mock.Anything, offsetsTestProjection, offsetsTestShard, "account-a").Return(nil, nil)
		eventsStore := new(mockseventstore.EventsStore)
		eventsStore.EXPECT().EventsByShard(mock.Anything, offsetsTestShard, int64(0), mock.Anything).Return([]*egopb.Event{event("account-a", 2, base)}, nil)
		// the previous event is recent: the first event is rejected and replayed
		eventsStore.EXPECT().ReplayEvents(mock.Anything, "account-a", uint64(1), uint64(1), uint64(1)).Return([]*egopb.Event{event("account-a", 1, base)}, nil).Once()
		// a store answering the replay with an event of another persistence ID
		eventsStore.EXPECT().ReplayEvents(mock.Anything, "account-a", uint64(1), uint64(2), uint64(2)).Return(
			[]*egopb.Event{event("account-b", 3, base), event("account-a", 2, base)}, nil)
		offsetStore.EXPECT().ReadEntityOffset(mock.Anything, offsetsTestProjection, offsetsTestShard, "account-b").Return(nil, assert.AnError)

		err := newRunner(eventsStore, offsetStore).processShard(ctx, offsetsTestShard)
		require.ErrorIs(t, err, assert.AnError)
	})

	t.Run("fails when the handler rejects a replayed event", func(t *testing.T) {
		offsetStore := new(mocksoffsetstore.OffsetStore)
		offsetStore.EXPECT().ReadOffsets(mock.Anything, offsetsTestProjection, offsetsTestShard, uint64(offsetReadLimit)).Return(nil, nil)
		offsetStore.EXPECT().ReadEntityOffset(mock.Anything, offsetsTestProjection, offsetsTestShard, "account-a").Return(
			&egopb.EntityOffset{ShardNumber: offsetsTestShard, PersistenceId: "account-a", SequenceNumber: 1, Timestamp: base - 1}, nil)
		eventsStore := new(mockseventstore.EventsStore)
		eventsStore.EXPECT().EventsByShard(mock.Anything, offsetsTestShard, int64(0), mock.Anything).Return([]*egopb.Event{event("account-a", 3, base)}, nil)
		eventsStore.EXPECT().ReplayEvents(mock.Anything, "account-a", uint64(2), uint64(3), uint64(2)).Return(
			[]*egopb.Event{event("account-a", 2, base-1), event("account-a", 3, base)}, nil)

		runner := newProjectionRunner(offsetsTestProjection, &failingForHandler{persistenceID: "account-a"}, eventsStore, offsetStore, withLogger(log.DiscardLogger))
		runner.running.Store(true)

		var runnerErr *projectionRunnerError
		require.ErrorAs(t, runner.processShard(ctx, offsetsTestShard), &runnerErr)
	})

	t.Run("records the events processed before a failing handler", func(t *testing.T) {
		offsetStore := new(mocksoffsetstore.OffsetStore)
		offsetStore.EXPECT().ReadOffsets(mock.Anything, offsetsTestProjection, offsetsTestShard, uint64(offsetReadLimit)).Return(nil, nil)
		offsetStore.EXPECT().ReadEntityOffset(mock.Anything, offsetsTestProjection, offsetsTestShard, mock.Anything).Return(nil, nil)
		offsetStore.EXPECT().WriteOffsets(mock.Anything, offsetsTestProjection, mock.MatchedBy(func(offsets []*egopb.EntityOffset) bool {
			return len(offsets) == 1 && offsets[0].GetPersistenceId() == "account-a"
		})).Return(nil).Once()
		eventsStore := new(mockseventstore.EventsStore)
		eventsStore.EXPECT().EventsByShard(mock.Anything, offsetsTestShard, int64(0), mock.Anything).Return(
			[]*egopb.Event{event("account-a", 1, base), event("account-b", 1, base+1)}, nil)

		handler := &failingForHandler{persistenceID: "account-b"}
		runner := newProjectionRunner(offsetsTestProjection, handler, eventsStore, offsetStore, withLogger(log.DiscardLogger))
		runner.running.Store(true)

		err := runner.processShard(ctx, offsetsTestShard)
		var runnerErr *projectionRunnerError
		require.ErrorAs(t, err, &runnerErr)
		offsetStore.AssertExpectations(t)
	})

	t.Run("reports both a failing handler and a failing offset write", func(t *testing.T) {
		offsetStore := new(mocksoffsetstore.OffsetStore)
		offsetStore.EXPECT().ReadOffsets(mock.Anything, offsetsTestProjection, offsetsTestShard, uint64(offsetReadLimit)).Return(nil, nil)
		offsetStore.EXPECT().ReadEntityOffset(mock.Anything, offsetsTestProjection, offsetsTestShard, mock.Anything).Return(nil, nil)
		offsetStore.EXPECT().WriteOffsets(mock.Anything, offsetsTestProjection, mock.Anything).Return(assert.AnError)
		eventsStore := new(mockseventstore.EventsStore)
		eventsStore.EXPECT().EventsByShard(mock.Anything, offsetsTestShard, int64(0), mock.Anything).Return(
			[]*egopb.Event{event("account-a", 1, base), event("account-b", 1, base+1)}, nil)

		handler := &failingForHandler{persistenceID: "account-b"}
		runner := newProjectionRunner(offsetsTestProjection, handler, eventsStore, offsetStore, withLogger(log.DiscardLogger))
		runner.running.Store(true)

		err := runner.processShard(ctx, offsetsTestShard)
		var runnerErr *projectionRunnerError
		require.ErrorAs(t, err, &runnerErr)
		require.ErrorIs(t, err, assert.AnError)
	})
}

func TestProjectionRunnerBacktracking(t *testing.T) {
	ctx := context.Background()

	t.Run("delivers an event that became visible behind the position", func(t *testing.T) {
		fixture := newOffsetsFixture(t)
		fixture.runner.offsetSettings.backtrackingBehindCurrentTime = 0
		base := backtrackingPast()
		fixture.journal(t, "account-a", 1, base)
		fixture.journal(t, "account-b", 1, base+2)
		require.NoError(t, fixture.read(t))

		// the first read leaves the shard due for backtracking
		state := fixture.state()
		require.True(t, state.backtracking)

		// account-c's only event lands behind the position: no later event of
		// account-c reveals a gap, only backtracking finds it
		fixture.journal(t, "account-c", 1, base+1)
		require.NoError(t, fixture.read(t))

		assert.Equal(t, []delivery{{"account-a", 1}, {"account-b", 1}, {"account-c", 1}}, fixture.handler.handled())
		assert.False(t, state.backtracking)
		assert.EqualValues(t, base+2, state.latest, "backtracking never moves the position")
		assert.EqualValues(t, base+2, state.latestBacktracking)

		offset, err := fixture.offsets.ReadEntityOffset(ctx, offsetsTestProjection, offsetsTestShard, "account-c")
		require.NoError(t, err)
		require.NotNil(t, offset)
	})

	t.Run("carries on at the next pass while the backtracking reads fill the buffer", func(t *testing.T) {
		fixture := newOffsetsFixture(t)
		fixture.runner.offsetSettings.backtrackingBehindCurrentTime = 0
		fixture.runner.maxBufferSize = 2
		base := backtrackingPast()
		fixture.journal(t, "account-a", 1, base)
		fixture.journal(t, "account-b", 1, base+1)
		fixture.journal(t, "account-c", 1, base+2)
		state := newShardState(nil, base+2, defaultOffsetSettings().timeWindow)
		state.remember("account-a", 1, base)
		state.remember("account-b", 1, base+1)
		state.remember("account-c", 1, base+2)
		state.backtracking = true
		fixture.runner.shards[offsetsTestShard] = state

		require.NoError(t, fixture.read(t))

		assert.True(t, state.backtracking)
		assert.EqualValues(t, base+1, state.latestBacktracking)
		assert.True(t, fixture.runner.sawFullBatch.Load())
		assert.Empty(t, fixture.handler.handled(), "every event read was processed already")
		assert.Zero(t, state.readSinceBacktracking, "no normal read follows a backtracking read that carries on")
	})

	t.Run("stops when the range is empty", func(t *testing.T) {
		fixture := newOffsetsFixture(t)
		state := newShardState(nil, backtrackingPast(), defaultOffsetSettings().timeWindow)
		state.backtracking = true
		state.latestBacktracking = state.latest + 1
		fixture.runner.shards[offsetsTestShard] = state

		require.NoError(t, fixture.read(t))

		assert.False(t, state.backtracking)
	})

	t.Run("stops when a full page reaches past the end of the range", func(t *testing.T) {
		fixture := newOffsetsFixture(t)
		fixture.runner.maxBufferSize = 2
		base := backtrackingPast()
		now := time.Now().UnixNano()
		fixture.journal(t, "account-a", 1, base)
		fixture.journal(t, "account-b", 1, now)
		fixture.journal(t, "account-c", 1, now+1)
		state := newShardState(nil, now, defaultOffsetSettings().timeWindow)
		state.remember("account-a", 1, base)
		state.backtracking = true
		state.latestBacktracking = base
		fixture.runner.shards[offsetsTestShard] = state

		require.NoError(t, fixture.read(t))

		// the page is full, but its second event is younger than the range
		// reaches: the range is done
		assert.False(t, state.backtracking)
		assert.Greater(t, state.latestBacktracking, base)
		assert.Less(t, state.latestBacktracking, now)
	})

	t.Run("stops when every event of a full page is past the end of the range", func(t *testing.T) {
		fixture := newOffsetsFixture(t)
		fixture.runner.maxBufferSize = 2
		base := backtrackingPast()
		now := time.Now().UnixNano()
		fixture.journal(t, "account-a", 1, now-3)
		fixture.journal(t, "account-b", 1, now-2)
		fixture.journal(t, "account-c", 1, now-1)
		state := newShardState(nil, now, defaultOffsetSettings().timeWindow)
		state.backtracking = true
		state.latestBacktracking = base
		fixture.runner.shards[offsetsTestShard] = state

		require.NoError(t, fixture.read(t))

		assert.False(t, state.backtracking)
		assert.Empty(t, fixture.handler.handled(), "the events are left to a later read")
	})

	t.Run("fails a backtracking read that cannot advance", func(t *testing.T) {
		fixture := newOffsetsFixture(t)
		fixture.runner.offsetSettings.backtrackingBehindCurrentTime = 0
		fixture.runner.maxBufferSize = 2
		base := backtrackingPast()
		fixture.journal(t, "account-a", 1, base)
		fixture.journal(t, "account-b", 1, base)
		fixture.journal(t, "account-c", 1, base)
		state := newShardState(nil, base, defaultOffsetSettings().timeWindow)
		state.remember("account-a", 1, base)
		state.remember("account-b", 1, base)
		state.remember("account-c", 1, base)
		state.backtracking = true
		state.latestBacktracking = base
		fixture.runner.shards[offsetsTestShard] = state

		var runnerErr *projectionRunnerError
		require.ErrorAs(t, fixture.read(t), &runnerErr)
	})

	t.Run("fails when an event found by backtracking cannot be processed", func(t *testing.T) {
		fixture := newOffsetsFixture(t)
		fixture.runner.offsetSettings.backtrackingBehindCurrentTime = 0
		fixture.runner.handler = &failingForHandler{persistenceID: "account-c"}
		base := backtrackingPast()
		fixture.journal(t, "account-a", 1, base)
		fixture.journal(t, "account-b", 1, base+2)
		require.NoError(t, fixture.read(t))
		require.True(t, fixture.state().backtracking)

		fixture.journal(t, "account-c", 1, base+1)

		var runnerErr *projectionRunnerError
		require.ErrorAs(t, fixture.read(t), &runnerErr)
	})

	t.Run("fails when the backtracking read fails", func(t *testing.T) {
		offsetStore := new(mocksoffsetstore.OffsetStore)
		eventsStore := new(mockseventstore.EventsStore)
		eventsStore.EXPECT().EventsByShard(mock.Anything, offsetsTestShard, mock.Anything, mock.Anything).Return(nil, assert.AnError)

		runner := newProjectionRunner(offsetsTestProjection, new(deliveryLog), eventsStore, offsetStore, withLogger(log.DiscardLogger))
		runner.running.Store(true)
		state := newShardState(nil, backtrackingPast(), defaultOffsetSettings().timeWindow)
		state.backtracking = true
		runner.shards[offsetsTestShard] = state

		require.ErrorIs(t, runner.processShard(ctx, offsetsTestShard), assert.AnError)
	})
}

func TestProjectionRunnerPendingShards(t *testing.T) {
	ctx := context.Background()
	recent := time.Now().Add(-time.Second).UnixNano()

	eventsStore := new(mockseventstore.EventsStore)
	eventsStore.EXPECT().ShardOffsets(mock.Anything).Return(map[uint64]int64{
		1: recent, // never read
		2: recent, // new events past its position
		3: recent, // caught up, last read full
		4: recent, // caught up, backtracking
		5: recent, // caught up and idle
		6: recent, // caught up and idle long enough to backtrack
		7: recent, // idle long enough, but backtracking already reached its position
	}, nil)

	runner := newProjectionRunner(offsetsTestProjection, new(deliveryLog), eventsStore, new(mocksoffsetstore.OffsetStore), withLogger(log.DiscardLogger))
	caughtUp := func() *shardState {
		return &shardState{latest: recent, latestBacktracking: recent, entities: map[string]entityRecord{}}
	}
	behind := caughtUp()
	behind.latest = recent - 1
	full := caughtUp()
	full.lastReadFull = true
	backtracking := caughtUp()
	backtracking.backtracking = true
	idle := caughtUp()
	dueForBacktracking := caughtUp()
	dueForBacktracking.latestBacktracking = recent - 1
	dueForBacktracking.idlePasses = backtrackingIdlePasses - 1
	covered := caughtUp()
	covered.idlePasses = backtrackingIdlePasses - 1
	runner.shards = map[uint64]*shardState{2: behind, 3: full, 4: backtracking, 5: idle, 6: dueForBacktracking, 7: covered}

	shards, err := runner.pendingShards(ctx)
	require.NoError(t, err)

	assert.ElementsMatch(t, []uint64{1, 2, 3, 4, 6}, shards)
	assert.Equal(t, 1, idle.idlePasses)
	assert.True(t, dueForBacktracking.backtracking)
	assert.False(t, covered.backtracking, "a shard read again up to its position has nothing left to backtrack")
}

func TestProjectionRunnerDeletesOldOffsets(t *testing.T) {
	ctx := context.Background()

	t.Run("waits for the delete interval", func(t *testing.T) {
		offsetStore := new(mocksoffsetstore.OffsetStore)
		runner := newProjectionRunner(offsetsTestProjection, new(deliveryLog), new(mockseventstore.EventsStore), offsetStore, withLogger(log.DiscardLogger))
		runner.shards[offsetsTestShard] = &shardState{latest: settledPast(), dirty: true}

		runner.deleteOldOffsets(ctx)

		offsetStore.AssertNotCalled(t, "DeleteOffsetsBefore", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("deletes the old offsets of the shards written since the last deletion", func(t *testing.T) {
		latest := settledPast()
		settings := defaultOffsetSettings()
		offsetStore := new(mocksoffsetstore.OffsetStore)
		offsetStore.EXPECT().DeleteOffsetsBefore(mock.Anything, offsetsTestProjection, offsetsTestShard, latest-settings.deleteAfter.Nanoseconds(), mock.Anything).Return(nil).Once()

		runner := newProjectionRunner(offsetsTestProjection, new(deliveryLog), new(mockseventstore.EventsStore), offsetStore, withLogger(log.DiscardLogger))
		runner.lastOffsetCleanup = time.Now().Add(-settings.deleteInterval)
		written := &shardState{latest: latest, dirty: true}
		untouched := &shardState{latest: latest}
		runner.shards = map[uint64]*shardState{offsetsTestShard: written, offsetsTestShard + 1: untouched}

		runner.deleteOldOffsets(ctx)

		offsetStore.AssertExpectations(t)
		assert.False(t, written.dirty)
	})

	t.Run("runs at the end of a pass with no shard pending", func(t *testing.T) {
		eventsStore := new(mockseventstore.EventsStore)
		eventsStore.EXPECT().ShardOffsets(mock.Anything).Return(map[uint64]int64{}, nil)
		offsetStore := new(mocksoffsetstore.OffsetStore)
		offsetStore.EXPECT().DeleteOffsetsBefore(mock.Anything, offsetsTestProjection, offsetsTestShard, mock.Anything, mock.Anything).Return(nil).Once()

		runner := newProjectionRunner(offsetsTestProjection, new(deliveryLog), eventsStore, offsetStore, withLogger(log.DiscardLogger))
		runner.lastOffsetCleanup = time.Now().Add(-defaultOffsetSettings().deleteInterval)
		written := &shardState{latest: settledPast(), dirty: true}
		runner.shards[offsetsTestShard] = written

		require.True(t, runner.runPass(ctx))

		offsetStore.AssertExpectations(t)
		assert.False(t, written.dirty)
	})

	t.Run("keeps a shard due when its deletion fails", func(t *testing.T) {
		offsetStore := new(mocksoffsetstore.OffsetStore)
		offsetStore.EXPECT().DeleteOffsetsBefore(mock.Anything, offsetsTestProjection, offsetsTestShard, mock.Anything, mock.Anything).Return(assert.AnError)

		runner := newProjectionRunner(offsetsTestProjection, new(deliveryLog), new(mockseventstore.EventsStore), offsetStore, withLogger(log.DiscardLogger))
		runner.lastOffsetCleanup = time.Now().Add(-defaultOffsetSettings().deleteInterval)
		written := &shardState{latest: settledPast(), dirty: true}
		runner.shards[offsetsTestShard] = written

		runner.deleteOldOffsets(ctx)

		assert.True(t, written.dirty)
	})
}

func TestProjectionRunnerStartFloor(t *testing.T) {
	ctx := context.Background()

	t.Run("starts from the later of the recorded start and the configured one", func(t *testing.T) {
		startingOffset := time.Now().Add(-time.Hour)
		offsetStore := new(mocksoffsetstore.OffsetStore)
		offsetStore.EXPECT().StartOffset(mock.Anything, offsetsTestProjection).Return(startingOffset.Add(time.Minute).UnixNano(), nil)

		runner := newProjectionRunner(offsetsTestProjection, new(deliveryLog), new(mockseventstore.EventsStore), offsetStore,
			withLogger(log.DiscardLogger), withStartOffset(startingOffset))
		require.NoError(t, runner.preStart(ctx))
		assert.Equal(t, startingOffset.Add(time.Minute).UnixNano(), runner.startFloor)
	})

	t.Run("fails when the recorded start cannot be read", func(t *testing.T) {
		offsetStore := new(mocksoffsetstore.OffsetStore)
		offsetStore.EXPECT().StartOffset(mock.Anything, offsetsTestProjection).Return(0, assert.AnError)

		runner := newProjectionRunner(offsetsTestProjection, new(deliveryLog), new(mockseventstore.EventsStore), offsetStore, withLogger(log.DiscardLogger))
		err := runner.preStart(ctx)
		require.ErrorIs(t, err, assert.AnError)
		assert.ErrorContains(t, err, fmt.Sprintf("failed to read the start offset of projection=%s", offsetsTestProjection))
	})
}

// failingForHandler is a projection handler that fails on the events of one
// persistence ID and accepts every other event.
type failingForHandler struct {
	// persistenceID is the persistence ID whose events fail.
	persistenceID string
}

// enforces compilation error
var _ projection.Handler = (*failingForHandler)(nil)

// Handle fails for the configured persistence ID.
func (x *failingForHandler) Handle(_ context.Context, persistenceID string, _ *anypb.Any, _ uint64) error {
	if persistenceID == x.persistenceID {
		return assert.AnError
	}
	return nil
}

func TestProjectionRunnerRecordsLag(t *testing.T) {
	fixture := newOffsetsFixture(t)
	fixture.runner.metrics = newMetrics(noopmetric.NewMeterProvider().Meter("test"))
	base := settledPast()
	fixture.journal(t, "account-a", 1, base)
	fixture.journal(t, "account-a", 2, base+1)
	fixture.runner.shards[offsetsTestShard] = newShardState(nil, base, defaultOffsetSettings().timeWindow)

	require.NoError(t, fixture.read(t))

	assert.Equal(t, []delivery{{"account-a", 1}, {"account-a", 2}}, fixture.handler.handled())
}

func TestProjectionRunnerTransactionalHandler(t *testing.T) {
	ctx := context.Background()

	t.Run("does not deliver a committed event again after a restart", func(t *testing.T) {
		fixture, handler := newTransactionalFixture(t, projection.NewRecovery(), nil)
		base := settledPast()
		fixture.journal(t, "account-a", 1, base)
		fixture.journal(t, "account-a", 2, base+1)
		require.NoError(t, fixture.read(t))
		require.Equal(t, []delivery{{"account-a", 1}, {"account-a", 2}}, handler.committedEvents())
		assert.True(t, fixture.state().dirty, "the committed rows are cleaned like the runner's own")

		// the projection crashes and restarts on the same stores
		fixture.runner = newTransactionalRunner(fixture, handler, projection.NewRecovery())
		require.NoError(t, fixture.read(t))

		assert.Equal(t, []delivery{{"account-a", 1}, {"account-a", 2}}, handler.committedEvents(), "a committed event is applied once")
	})

	t.Run("applies a rolled-back event once when a retry commits it", func(t *testing.T) {
		recovery := projection.NewRecovery(projection.WithRecoveryPolicy(projection.RetryAndFail), projection.WithRetries(2), projection.WithRetryDelay(time.Millisecond))
		fixture, handler := newTransactionalFixture(t, recovery, map[string]int{"account-a": 1})
		fixture.journal(t, "account-a", 1, settledPast())

		require.NoError(t, fixture.read(t))

		assert.Equal(t, []delivery{{"account-a", 1}}, handler.committedEvents())
	})

	t.Run("records the offset of an event the policy skipped", func(t *testing.T) {
		recovery := projection.NewRecovery(projection.WithRecoveryPolicy(projection.Skip))
		fixture, handler := newTransactionalFixture(t, recovery, map[string]int{"account-a": -1})
		base := settledPast()
		fixture.journal(t, "account-a", 1, base)
		require.NoError(t, fixture.read(t))

		offset, err := fixture.offsets.ReadEntityOffset(ctx, offsetsTestProjection, offsetsTestShard, "account-a")
		require.NoError(t, err)
		require.NotNil(t, offset, "the runner records a skipped event")
		assert.EqualValues(t, 1, offset.GetSequenceNumber())

		// a restarted projection does not hand the skipped event over again
		fixture.runner = newTransactionalRunner(fixture, handler, recovery)
		require.NoError(t, fixture.read(t))
		assert.Empty(t, handler.committedEvents())
	})

	t.Run("records the offset of an event the retry policy skipped", func(t *testing.T) {
		recovery := projection.NewRecovery(projection.WithRecoveryPolicy(projection.RetryAndSkip), projection.WithRetries(2), projection.WithRetryDelay(time.Millisecond))
		fixture, handler := newTransactionalFixture(t, recovery, map[string]int{"account-a": -1})
		fixture.journal(t, "account-a", 1, settledPast())

		require.NoError(t, fixture.read(t))

		assert.Empty(t, handler.committedEvents())
		offset, err := fixture.offsets.ReadEntityOffset(ctx, offsetsTestProjection, offsetsTestShard, "account-a")
		require.NoError(t, err)
		require.NotNil(t, offset, "the runner records an event skipped after its retries")
	})

	t.Run("keeps only the committed offset after a skipped event of the same read", func(t *testing.T) {
		recovery := projection.NewRecovery(projection.WithRecoveryPolicy(projection.Skip))
		fixture, _ := newTransactionalFixture(t, recovery, map[string]int{"account-a": 1})
		base := settledPast()
		fixture.journal(t, "account-a", 1, base)
		fixture.journal(t, "account-a", 2, base+1)

		require.NoError(t, fixture.read(t))

		offsets, err := fixture.offsets.ReadOffsets(ctx, offsetsTestProjection, offsetsTestShard, 10)
		require.NoError(t, err)
		require.Len(t, offsets, 1, "the committed offset supersedes the skipped one")
		assert.EqualValues(t, 2, offsets[0].GetSequenceNumber())
	})

	t.Run("records framework events itself", func(t *testing.T) {
		fixture, handler := newTransactionalFixture(t, projection.NewRecovery(), nil)
		status, err := anypb.New(&egopb.SagaStatusChanged{Status: 1})
		require.NoError(t, err)
		require.NoError(t, fixture.events.WriteEvents(ctx, []*egopb.Event{{
			PersistenceId:  "saga-1",
			SequenceNumber: 1,
			Event:          status,
			Timestamp:      settledPast(),
			Shard:          offsetsTestShard,
		}}))

		require.NoError(t, fixture.read(t))

		assert.Empty(t, handler.committedEvents(), "a framework event never reaches the handler")
		offset, err := fixture.offsets.ReadEntityOffset(ctx, offsetsTestProjection, offsetsTestShard, "saga-1")
		require.NoError(t, err)
		require.NotNil(t, offset)
	})

	t.Run("commits the replayed events of a gap", func(t *testing.T) {
		fixture, handler := newTransactionalFixture(t, projection.NewRecovery(), nil)
		base := settledPast()
		fixture.journal(t, "account-a", 1, base)
		fixture.journal(t, "account-b", 1, base+2)
		require.NoError(t, fixture.read(t))

		fixture.journal(t, "account-a", 2, base+1)
		fixture.journal(t, "account-a", 3, base+3)
		require.NoError(t, fixture.read(t))

		assert.Equal(t, []delivery{{"account-a", 1}, {"account-b", 1}, {"account-a", 2}, {"account-a", 3}}, handler.committedEvents())
	})

	t.Run("stops the projection when the handler fails under the Fail policy", func(t *testing.T) {
		fixture, handler := newTransactionalFixture(t, projection.NewRecovery(), map[string]int{"account-a": -1})
		fixture.journal(t, "account-a", 1, settledPast())

		var runnerErr *projectionRunnerError
		require.ErrorAs(t, fixture.read(t), &runnerErr)
		assert.Empty(t, handler.committedEvents())

		offset, err := fixture.offsets.ReadEntityOffset(ctx, offsetsTestProjection, offsetsTestShard, "account-a")
		require.NoError(t, err)
		assert.Nil(t, offset, "a rolled-back event has no offset")
	})
}

func TestProjectionRunnerUnknownRecoveryPolicy(t *testing.T) {
	// a policy value outside the defined ones behaves like Fail
	fixture := newOffsetsFixture(t)
	unknown := projection.NewRecovery(projection.WithRecoveryPolicy(projection.RecoveryPolicy(99)))
	fixture.runner = newProjectionRunner(offsetsTestProjection, &failingForHandler{persistenceID: "account-a"}, fixture.events, fixture.offsets,
		withLogger(log.DiscardLogger),
		withRecoveryStrategy(unknown))
	fixture.runner.running.Store(true)
	fixture.journal(t, "account-a", 1, settledPast())

	var runnerErr *projectionRunnerError
	require.ErrorAs(t, fixture.read(t), &runnerErr)
	assert.ErrorIs(t, runnerErr, assert.AnError)
}
