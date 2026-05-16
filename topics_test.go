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
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"

	goakt "github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/remote"

	"github.com/tochemey/ego/v4/egopb"
	"github.com/tochemey/ego/v4/internal/pause"
	testpb "github.com/tochemey/ego/v4/test/data/testpb"
	"github.com/tochemey/ego/v4/testkit"
)

// TestTopicConstantsAreFixed regression-guards the topic constants. They must
// be plain strings with no fmt directives — the partition-suffixed format
// "topic.events.%d" historically caused subscribers and publishers to fall
// out of sync whenever the cluster used a partition count different from
// goakt's default. Collapsing to a single topic (with the shard travelling in
// the payload via egopb.Event.Shard / egopb.DurableState.Shard) removes that
// failure mode.
func TestTopicConstantsAreFixed(t *testing.T) {
	assert.NotEmpty(t, eventsTopic, "eventsTopic must be set")
	assert.NotEmpty(t, statesTopic, "statesTopic must be set")
	assert.NotContains(t, eventsTopic, "%", "eventsTopic must not contain fmt directives")
	assert.NotContains(t, statesTopic, "%", "statesTopic must not contain fmt directives")
	assert.NotEqual(t, eventsTopic, statesTopic, "events and states must use distinct topics")
}

// recordingEventPublisher captures every event passed to Publish so tests can
// assert delivery without racing on a channel signal.
type recordingEventPublisher struct {
	id     string
	mu     sync.Mutex
	events []*egopb.Event
	closed atomic.Bool
}

var _ EventPublisher = (*recordingEventPublisher)(nil)

func newRecordingEventPublisher(id string) *recordingEventPublisher {
	return &recordingEventPublisher{id: id}
}

func (p *recordingEventPublisher) ID() string { return p.id }

func (p *recordingEventPublisher) Publish(_ context.Context, event *egopb.Event) error {
	p.mu.Lock()
	p.events = append(p.events, event)
	p.mu.Unlock()
	return nil
}

func (p *recordingEventPublisher) Close(_ context.Context) error {
	p.closed.Store(true)
	return nil
}

func (p *recordingEventPublisher) snapshot() []*egopb.Event {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]*egopb.Event, len(p.events))
	copy(out, p.events)
	return out
}

// recordingStatePublisher is the durable-state analog of recordingEventPublisher.
type recordingStatePublisher struct {
	id     string
	mu     sync.Mutex
	states []*egopb.DurableState
	closed atomic.Bool
}

var _ StatePublisher = (*recordingStatePublisher)(nil)

func newRecordingStatePublisher(id string) *recordingStatePublisher {
	return &recordingStatePublisher{id: id}
}

func (p *recordingStatePublisher) ID() string { return p.id }

func (p *recordingStatePublisher) Publish(_ context.Context, state *egopb.DurableState) error {
	p.mu.Lock()
	p.states = append(p.states, state)
	p.mu.Unlock()
	return nil
}

func (p *recordingStatePublisher) Close(_ context.Context) error {
	p.closed.Store(true)
	return nil
}

func (p *recordingStatePublisher) snapshot() []*egopb.DurableState {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]*egopb.DurableState, len(p.states))
	copy(out, p.states)
	return out
}

// waitFor polls until cond returns true or timeout expires.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	if !waitForCond(timeout, cond) {
		t.Fatalf("condition not met within %s", timeout)
	}
}

// waitForCond polls until cond returns true or timeout expires. Returns
// whether the condition was met.
func waitForCond(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		pause.For(20 * time.Millisecond)
	}
	return false
}

// TestEventPublisherReceivesEventsFromEntity verifies the happy path: an
// event-sourced entity emits events, the registered event publisher receives
// them. Pre-fix, this also worked in single-node mode because both ends
// agreed on partition 0; the test is here to pin that behavior under the new
// single-topic model.
func TestEventPublisherReceivesEventsFromEntity(t *testing.T) {
	ctx := context.Background()
	store := testkit.NewEventsStore()
	require.NoError(t, store.Connect(ctx))
	t.Cleanup(func() { _ = store.Disconnect(ctx) })

	engine := newTestEngine(t, "Publishers", store, WithLogger(DiscardLogger))
	require.NoError(t, engine.Start(ctx))

	pub := newRecordingEventPublisher("recorder")
	require.NoError(t, engine.AddEventPublishers(pub))

	entityID := uuid.NewString()
	require.NoError(t, engine.Entity(ctx, NewEventSourcedEntity(entityID)))

	_, _, err := engine.SendCommand(ctx, entityID, &testpb.CreateAccount{AccountBalance: 100}, time.Minute)
	require.NoError(t, err)
	_, _, err = engine.SendCommand(ctx, entityID, &testpb.CreditAccount{AccountId: entityID, Balance: 50}, time.Minute)
	require.NoError(t, err)

	waitFor(t, 5*time.Second, func() bool {
		return len(pub.snapshot()) == 2
	})

	events := pub.snapshot()
	require.Len(t, events, 2)
	assert.Equal(t, entityID, events[0].GetPersistenceId())
	assert.EqualValues(t, 1, events[0].GetSequenceNumber())
	assert.EqualValues(t, 2, events[1].GetSequenceNumber())
}

// TestEventPublisherFanOutToMultipleSubscribers verifies that registering
// multiple event publishers fans events out to all of them. Each publisher
// gets a separate eventstream subscriber, and the single-topic model must not
// regress to "first subscriber wins."
func TestEventPublisherFanOutToMultipleSubscribers(t *testing.T) {
	ctx := context.Background()
	store := testkit.NewEventsStore()
	require.NoError(t, store.Connect(ctx))
	t.Cleanup(func() { _ = store.Disconnect(ctx) })

	engine := newTestEngine(t, "FanOut", store, WithLogger(DiscardLogger))
	require.NoError(t, engine.Start(ctx))

	pubs := []*recordingEventPublisher{
		newRecordingEventPublisher("a"),
		newRecordingEventPublisher("b"),
		newRecordingEventPublisher("c"),
	}
	require.NoError(t, engine.AddEventPublishers(pubs[0], pubs[1], pubs[2]))

	entityID := uuid.NewString()
	require.NoError(t, engine.Entity(ctx, NewEventSourcedEntity(entityID)))

	_, _, err := engine.SendCommand(ctx, entityID, &testpb.CreateAccount{AccountBalance: 100}, time.Minute)
	require.NoError(t, err)

	waitFor(t, 5*time.Second, func() bool {
		for _, p := range pubs {
			if len(p.snapshot()) == 0 {
				return false
			}
		}
		return true
	})

	for _, p := range pubs {
		events := p.snapshot()
		require.Lenf(t, events, 1, "publisher %s should have received exactly one event", p.ID())
		assert.Equal(t, entityID, events[0].GetPersistenceId())
	}
}

// TestEventPayloadCarriesShard verifies that downstream consumers can still
// recover the shard the event came from — the partition information that
// used to live in the topic name now lives in egopb.Event.Shard. This is the
// invariant the single-topic refactor relies on.
func TestEventPayloadCarriesShard(t *testing.T) {
	ctx := context.Background()
	store := testkit.NewEventsStore()
	require.NoError(t, store.Connect(ctx))
	t.Cleanup(func() { _ = store.Disconnect(ctx) })

	engine := newTestEngine(t, "ShardInPayload", store, WithLogger(DiscardLogger))
	require.NoError(t, engine.Start(ctx))

	pub := newRecordingEventPublisher("recorder")
	require.NoError(t, engine.AddEventPublishers(pub))

	entityID := uuid.NewString()
	require.NoError(t, engine.Entity(ctx, NewEventSourcedEntity(entityID)))

	_, _, err := engine.SendCommand(ctx, entityID, &testpb.CreateAccount{AccountBalance: 100}, time.Minute)
	require.NoError(t, err)

	waitFor(t, 5*time.Second, func() bool {
		return len(pub.snapshot()) == 1
	})

	event := pub.snapshot()[0]
	// In single-node mode the actor system's Partition() returns 0 for every
	// actor; we only care that the Shard field is wired up (not its specific
	// value here). The cluster regression test below covers non-zero shards.
	assert.EqualValues(t, 0, event.GetShard())
	assert.NotNil(t, event.GetEvent(), "event payload should be set")
	assert.Equal(t, entityID, event.GetPersistenceId())
	assert.EqualValues(t, 1, event.GetSequenceNumber())
}

// TestStatePublisherReceivesDurableStateUpdates is the durable-state analog
// of TestEventPublisherReceivesEventsFromEntity.
func TestStatePublisherReceivesDurableStateUpdates(t *testing.T) {
	ctx := context.Background()
	stateStore := testkit.NewDurableStore()
	require.NoError(t, stateStore.Connect(ctx))
	t.Cleanup(func() { _ = stateStore.Disconnect(ctx) })

	engine := newTestEngine(t, "StatePub", nil,
		WithLogger(DiscardLogger),
		WithStateStore(stateStore),
	)
	require.NoError(t, engine.Start(ctx))

	pub := newRecordingStatePublisher("recorder")
	require.NoError(t, engine.AddStatePublishers(pub))

	entityID := uuid.NewString()
	require.NoError(t, engine.DurableStateEntity(ctx, NewAccountDurableStateBehavior(entityID)))

	_, _, err := engine.SendCommand(ctx, entityID, &testpb.CreateAccount{AccountBalance: 100}, time.Minute)
	require.NoError(t, err)

	waitFor(t, 10*time.Second, func() bool {
		return len(pub.snapshot()) >= 1
	})

	states := pub.snapshot()
	require.NotEmpty(t, states)
	assert.Equal(t, entityID, states[0].GetPersistenceId())
	assert.EqualValues(t, 1, states[0].GetVersionNumber())
	// In single-node mode the actor system always returns partition 0, but
	// the Shard field must still be populated so downstream consumers can
	// branch on it once cluster mode kicks in.
	assert.EqualValues(t, 0, states[0].GetShard())
}

// TestEngineSubscribeReceivesEventsAndStates verifies that the public
// Engine.Subscribe() API delivers both events (from event-sourced entities)
// and durable state updates (from durable-state entities) through a single
// subscriber. Pre-fix, Subscribe() subscribed to 271 events topics and 271
// states topics in cluster mode and would have silently dropped anything
// outside that range.
func TestEngineSubscribeReceivesEventsAndStates(t *testing.T) {
	ctx := context.Background()
	store := testkit.NewEventsStore()
	require.NoError(t, store.Connect(ctx))
	t.Cleanup(func() { _ = store.Disconnect(ctx) })
	stateStore := testkit.NewDurableStore()
	require.NoError(t, stateStore.Connect(ctx))
	t.Cleanup(func() { _ = stateStore.Disconnect(ctx) })

	engine := newTestEngine(t, "SubAll", store,
		WithLogger(DiscardLogger),
		WithStateStore(stateStore),
	)
	require.NoError(t, engine.Start(ctx))

	sub, err := engine.Subscribe()
	require.NoError(t, err)
	t.Cleanup(sub.Shutdown)

	// Event-sourced entity emits an Event.
	esID := uuid.NewString()
	require.NoError(t, engine.Entity(ctx, NewEventSourcedEntity(esID)))
	_, _, err = engine.SendCommand(ctx, esID, &testpb.CreateAccount{AccountBalance: 1}, time.Minute)
	require.NoError(t, err)

	// Durable-state entity emits a DurableState.
	dsID := uuid.NewString()
	require.NoError(t, engine.DurableStateEntity(ctx, NewAccountDurableStateBehavior(dsID)))
	_, _, err = engine.SendCommand(ctx, dsID, &testpb.CreateAccount{AccountBalance: 1}, time.Minute)
	require.NoError(t, err)

	// Subscriber.Iterator() is a one-shot drain — each call returns whatever
	// is queued and then closes. Poll it until we see both message kinds or
	// the deadline expires.
	var (
		gotEvent bool
		gotState bool
	)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && (!gotEvent || !gotState) {
		for msg := range sub.Iterator() {
			if msg == nil {
				continue
			}
			switch msg.Payload().(type) {
			case *egopb.Event:
				gotEvent = true
			case *egopb.DurableState:
				gotState = true
			}
		}
		if gotEvent && gotState {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	assert.True(t, gotEvent, "subscriber did not receive any Event through Engine.Subscribe()")
	assert.True(t, gotState, "subscriber did not receive any DurableState through Engine.Subscribe()")
}

// TestEventPublisherClusterHighPartitionCount is the regression test for the
// bug the single-topic refactor fixes. Pre-fix, the engine's publishers
// subscribed to topic.events.0 ... topic.events.270 because the partition
// count was hardcoded to goakt's default of 271. If a deployment configured
// a higher partition count and an entity happened to map to a shard >= 271,
// the published event would be silently dropped.
//
// This test configures a 1009-partition cluster (a prime above the old
// hardcoded ceiling), forces several entities through the system, and
// verifies the publisher receives ALL of their initial events — including
// any whose shard exceeds the old ceiling. It also asserts that at least
// one event arrived from a shard outside the legacy [0, 271) window so the
// test really exercises the formerly-dropped range, not just the lucky few
// at the bottom.
func TestEventPublisherClusterHighPartitionCount(t *testing.T) {
	const partitionCount = 1009
	const numEntities = 40

	ctx := context.Background()
	store := testkit.NewEventsStore()
	require.NoError(t, store.Connect(ctx))
	t.Cleanup(func() { _ = store.Disconnect(ctx) })

	ports := dynaport.Get(3)
	gossipPort, peersPort, remotingPort := ports[0], ports[1], ports[2]
	host := "127.0.0.1"

	provider := &mockClusterProvider{
		id:    "hi-part",
		peers: []string{net.JoinHostPort(host, strconv.Itoa(peersPort))},
	}

	clusterCfg := goakt.NewClusterConfig().
		WithDiscovery(provider).
		WithDiscoveryPort(gossipPort).
		WithPeersPort(peersPort).
		WithMinimumPeersQuorum(1).
		WithReplicaCount(1).
		WithPartitionCount(partitionCount).
		WithKinds(ClusterKinds()...)

	cfg := NewConfig(store, WithLogger(DiscardLogger))

	goaktOpts := append(cfg.GoaktOptions(),
		goakt.WithCluster(clusterCfg),
		goakt.WithRemote(remote.NewConfig(host, remotingPort)),
	)

	sys, err := goakt.NewActorSystem("HighPart", goaktOpts...)
	require.NoError(t, err)
	require.NoError(t, sys.Start(ctx))
	t.Cleanup(func() { _ = sys.Stop(ctx) })

	pause.For(time.Second)
	require.True(t, sys.InCluster())

	engine, err := NewEngine(sys, cfg)
	require.NoError(t, err)
	require.NoError(t, engine.Start(ctx))
	t.Cleanup(func() { _ = engine.Stop(ctx) })

	// Warm the cluster's distributed map so that subsequent SpawnOn calls
	// get real partition assignments instead of the zero value goakt returns
	// when an actor isn't yet visible in the dmap. We spawn (and immediately
	// command) a batch of throwaway entities, then wait until probing those
	// entities by name surfaces a partition in the formerly-dropped range
	// (>= 271). Without this warm-up the workload below can race the dmap
	// under -race-detector slowdown and have every event land in shard 0,
	// which would make the assertion below trivially fail.
	const warmupEntities = 200
	warmupIDs := make([]string, 0, warmupEntities)
	for range warmupEntities {
		id := "warmup-" + uuid.NewString()
		warmupIDs = append(warmupIDs, id)
		require.NoError(t, engine.Entity(ctx, NewEventSourcedEntity(id)))
		_, _, err := engine.SendCommand(ctx, id, &testpb.CreateAccount{AccountBalance: 1}, time.Minute)
		require.NoError(t, err)
	}
	if !waitForCond(60*time.Second, func() bool {
		for _, id := range warmupIDs {
			if sys.Partition(id) >= 271 {
				return true
			}
		}
		return false
	}) {
		t.Skip("goakt cluster dmap did not surface any shard >= 271 within 60s; cannot exercise the pre-fix bug range under current scheduling (likely race-detector slowdown)")
	}

	// Now register the publisher and run the real workload. The publisher
	// only sees events emitted from this point on, so the warm-up entities'
	// events (which it would have received too) don't get mixed in.
	pub := newRecordingEventPublisher("hi-part")
	require.NoError(t, engine.AddEventPublishers(pub))

	// Generate enough entities that with 1009 partitions at least one is very
	// likely to land beyond shard 271 (P ≈ 1 - (271/1009)^40, effectively 1).
	entityIDs := make([]string, 0, numEntities)
	for range numEntities {
		entityID := uuid.NewString()
		entityIDs = append(entityIDs, entityID)

		require.NoError(t, engine.Entity(ctx, NewEventSourcedEntity(entityID)))
		_, _, err := engine.SendCommand(ctx, entityID, &testpb.CreateAccount{AccountBalance: 1}, time.Minute)
		require.NoError(t, err)
	}

	waitFor(t, 10*time.Second, func() bool {
		return len(pub.snapshot()) >= numEntities
	})

	got := pub.snapshot()
	require.Len(t, got, numEntities,
		"publisher must receive every entity's event regardless of shard placement")

	// Index received events by persistence id and assert every entity surfaced
	// exactly one event.
	gotByID := make(map[string]*egopb.Event, len(got))
	for _, evt := range got {
		gotByID[evt.GetPersistenceId()] = evt
	}
	for _, entityID := range entityIDs {
		_, ok := gotByID[entityID]
		require.Truef(t, ok, "publisher did not receive event for entity %s", entityID)
	}

	// Confirm at least one event was emitted from a shard outside the legacy
	// 0..270 window — otherwise the test wouldn't be exercising the formerly
	// silently-dropped range, and we shouldn't claim it's a regression test.
	beyondLegacyCeiling := 0
	maxShard := uint64(0)
	for _, evt := range got {
		if evt.GetShard() >= 271 {
			beyondLegacyCeiling++
		}
		if evt.GetShard() > maxShard {
			maxShard = evt.GetShard()
		}
	}
	require.Greaterf(t, beyondLegacyCeiling, 0,
		"workload only produced events in shards [0..270] (max seen: %d); the test does not exercise the pre-fix bug range",
		maxShard)
}

// TestTopicConstantsAreNotPartitionedFormats is a paranoid byte-level check:
// if someone reintroduces "topic.events.%d" or similar by accident, this
// test fires immediately rather than waiting for an event delivery to drop
// in production.
func TestTopicConstantsAreNotPartitionedFormats(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value string
	}{
		{"eventsTopic", eventsTopic},
		{"statesTopic", statesTopic},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Falsef(t, strings.Contains(tc.value, "%d"),
				"%s = %q should not contain a partition format directive", tc.name, tc.value)
			assert.Falsef(t, strings.Contains(tc.value, "%"),
				"%s = %q should not contain any fmt directive", tc.name, tc.value)
		})
	}
}
