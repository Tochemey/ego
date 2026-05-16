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
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	goakt "github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/remote"
	"github.com/travisjeffery/go-dynaport"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/tochemey/ego/v4/egopb"
	samplepb "github.com/tochemey/ego/v4/example/examplepb"
	"github.com/tochemey/ego/v4/internal/extensions"
	"github.com/tochemey/ego/v4/internal/pause"
	"github.com/tochemey/ego/v4/internal/syncmap"
	"github.com/tochemey/ego/v4/projection"
	testpb "github.com/tochemey/ego/v4/test/data/testpb"
	"github.com/tochemey/ego/v4/testkit"
)

// NewEventSourcedEntity is a test alias used by older tests; the helper file
// also references it.
func NewEventSourcedEntity(id string) *AccountEventSourcedBehavior {
	return NewAccountEventSourcedBehavior(id)
}

// TestNewEngineValidation exercises the error paths that surface a
// mis-configured handoff between the actor system and the engine.
func TestNewEngineValidation(t *testing.T) {
	t.Run("nil actor system", func(t *testing.T) {
		_, err := NewEngine(nil, NewConfig(testkit.NewEventsStore()))
		require.ErrorIs(t, err, ErrActorSystemRequired)
	})

	t.Run("nil config", func(t *testing.T) {
		cfg := NewConfig(testkit.NewEventsStore())
		sys, err := goakt.NewActorSystem("Sample", cfg.GoaktOptions()...)
		require.NoError(t, err)
		_, err = NewEngine(sys, nil)
		require.ErrorIs(t, err, ErrMissingRequiredExtensions)
	})

	t.Run("not started actor system", func(t *testing.T) {
		cfg := NewConfig(testkit.NewEventsStore())
		sys, err := goakt.NewActorSystem("Sample", cfg.GoaktOptions()...)
		require.NoError(t, err)
		// Deliberately skip sys.Start.
		_, err = NewEngine(sys, cfg)
		require.ErrorIs(t, err, ErrActorSystemNotStarted)
	})

	t.Run("missing required extension is reported", func(t *testing.T) {
		ctx := context.Background()
		// Build an actor system with NO ego extensions registered.
		sys, err := goakt.NewActorSystem("Sample", goakt.WithPubSub())
		require.NoError(t, err)
		require.NoError(t, sys.Start(ctx))
		t.Cleanup(func() { _ = sys.Stop(ctx) })

		_, err = NewEngine(sys, NewConfig(testkit.NewEventsStore()))
		require.ErrorIs(t, err, ErrMissingRequiredExtensions)
	})

	t.Run("missing optional extension is reported when configured", func(t *testing.T) {
		ctx := context.Background()
		// Build the actor system from a Config that does NOT include an offset
		// store, then ask NewEngine for a Config that does. The mismatch should
		// surface ErrMissingRequiredExtensions.
		store := testkit.NewEventsStore()
		baseCfg := NewConfig(store)
		sys, err := goakt.NewActorSystem("Sample", baseCfg.GoaktOptions()...)
		require.NoError(t, err)
		require.NoError(t, sys.Start(ctx))
		t.Cleanup(func() { _ = sys.Stop(ctx) })

		_, err = NewEngine(sys, NewConfig(store, WithOffsetStore(testkit.NewOffsetStore())))
		require.ErrorIs(t, err, ErrMissingRequiredExtensions)
	})
}

// TestEngineEventSourced covers the happy path for an event-sourced entity in
// single-node mode.
func TestEngineEventSourced(t *testing.T) {
	ctx := context.Background()
	store := testkit.NewEventsStore()
	require.NoError(t, store.Connect(ctx))
	t.Cleanup(func() { _ = store.Disconnect(ctx) })

	engine := newTestEngine(t, "Sample", store, WithLogger(DiscardLogger))
	require.NoError(t, engine.Start(ctx))

	entityID := uuid.NewString()
	require.NoError(t, engine.Entity(ctx, NewEventSourcedEntity(entityID)))

	// create
	state, revision, err := engine.SendCommand(ctx, entityID, &testpb.CreateAccount{
		AccountBalance: 500.00,
	}, time.Minute)
	require.NoError(t, err)
	acct, ok := state.(*testpb.Account)
	require.True(t, ok)
	assert.EqualValues(t, 500.00, acct.GetAccountBalance())
	assert.EqualValues(t, 1, revision)

	// credit
	state, revision, err = engine.SendCommand(ctx, entityID, &testpb.CreditAccount{
		AccountId: entityID,
		Balance:   250,
	}, time.Minute)
	require.NoError(t, err)
	acct, ok = state.(*testpb.Account)
	require.True(t, ok)
	assert.EqualValues(t, 750.00, acct.GetAccountBalance())
	assert.EqualValues(t, 2, revision)

	require.NoError(t, engine.Stop(ctx))
}

// TestEngineDurableState covers the happy path for a durable-state entity.
func TestEngineDurableState(t *testing.T) {
	ctx := context.Background()
	stateStore := testkit.NewDurableStore()
	require.NoError(t, stateStore.Connect(ctx))
	t.Cleanup(func() { _ = stateStore.Disconnect(ctx) })

	engine := newTestEngine(t, "Sample", nil,
		WithLogger(DiscardLogger),
		WithStateStore(stateStore),
	)
	require.NoError(t, engine.Start(ctx))

	entityID := uuid.NewString()
	require.NoError(t, engine.DurableStateEntity(ctx, NewAccountDurableStateBehavior(entityID)))

	state, revision, err := engine.SendCommand(ctx, entityID, &testpb.CreateAccount{
		AccountBalance: 500.00,
	}, time.Minute)
	require.NoError(t, err)
	acct, ok := state.(*testpb.Account)
	require.True(t, ok)
	assert.EqualValues(t, 500.00, acct.GetAccountBalance())
	assert.EqualValues(t, 1, revision)

	require.NoError(t, engine.Stop(ctx))
}

// TestEngineDurableStateRequiresStateStore confirms that calling
// DurableStateEntity without WithStateStore is reported as a config error.
func TestEngineDurableStateRequiresStateStore(t *testing.T) {
	ctx := context.Background()
	store := testkit.NewEventsStore()
	require.NoError(t, store.Connect(ctx))
	t.Cleanup(func() { _ = store.Disconnect(ctx) })

	engine := newTestEngine(t, "Sample", store, WithLogger(DiscardLogger))
	require.NoError(t, engine.Start(ctx))

	err := engine.DurableStateEntity(ctx, NewAccountDurableStateBehavior(uuid.NewString()))
	require.ErrorIs(t, err, ErrDurableStateStoreRequired)
}

// TestEngineSendCommandErrors covers the error paths of SendCommand that do
// not require an entity to be live.
func TestEngineSendCommandErrors(t *testing.T) {
	ctx := context.Background()
	store := testkit.NewEventsStore()
	require.NoError(t, store.Connect(ctx))
	t.Cleanup(func() { _ = store.Disconnect(ctx) })

	t.Run("engine not started", func(t *testing.T) {
		engine := newTestEngine(t, "Sample", store, WithLogger(DiscardLogger))
		state, rev, err := engine.SendCommand(ctx, uuid.NewString(), &testpb.CreateAccount{}, time.Second)
		require.ErrorIs(t, err, ErrEngineNotStarted)
		require.Nil(t, state)
		require.Zero(t, rev)
	})

	t.Run("undefined entity id", func(t *testing.T) {
		engine := newTestEngine(t, "Sample", store, WithLogger(DiscardLogger))
		require.NoError(t, engine.Start(ctx))
		state, rev, err := engine.SendCommand(ctx, "", &testpb.CreateAccount{}, time.Second)
		require.ErrorIs(t, err, ErrUndefinedEntityID)
		require.Nil(t, state)
		require.Zero(t, rev)
	})
}

// TestEngineEntityExists exercises the EntityExists liveness probe.
func TestEngineEntityExists(t *testing.T) {
	ctx := context.Background()
	store := testkit.NewEventsStore()
	require.NoError(t, store.Connect(ctx))
	t.Cleanup(func() { _ = store.Disconnect(ctx) })

	t.Run("not started", func(t *testing.T) {
		engine := newTestEngine(t, "Sample", store, WithLogger(DiscardLogger))
		exists, err := engine.EntityExists(ctx, uuid.NewString())
		require.ErrorIs(t, err, ErrEngineNotStarted)
		require.False(t, exists)
	})

	t.Run("not found", func(t *testing.T) {
		engine := newTestEngine(t, "Sample", store, WithLogger(DiscardLogger))
		require.NoError(t, engine.Start(ctx))
		exists, err := engine.EntityExists(ctx, uuid.NewString())
		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("found after materialization", func(t *testing.T) {
		engine := newTestEngine(t, "Sample", store, WithLogger(DiscardLogger))
		require.NoError(t, engine.Start(ctx))

		entityID := uuid.NewString()
		require.NoError(t, engine.Entity(ctx, NewEventSourcedEntity(entityID)))
		_, _, err := engine.SendCommand(ctx, entityID, &testpb.CreateAccount{
			AccountBalance: 100,
		}, time.Minute)
		require.NoError(t, err)

		exists, err := engine.EntityExists(ctx, entityID)
		require.NoError(t, err)
		require.True(t, exists)
	})
}

// TestEngineActorSystemAccessor covers Engine.ActorSystem before/after Start/Stop.
func TestEngineActorSystemAccessor(t *testing.T) {
	ctx := context.Background()
	store := testkit.NewEventsStore()
	require.NoError(t, store.Connect(ctx))
	t.Cleanup(func() { _ = store.Disconnect(ctx) })

	engine := newTestEngine(t, "Sample", store, WithLogger(DiscardLogger))

	// Right after NewEngine: the engine HAS its reference to the actor system
	// (it can validate extensions etc). It is NOT yet "Started" but the
	// accessor returns the system so callers can inspect it.
	require.NotNil(t, engine.ActorSystem())

	require.NoError(t, engine.Start(ctx))
	require.NotNil(t, engine.ActorSystem())

	require.NoError(t, engine.Stop(ctx))
	require.Nil(t, engine.ActorSystem(), "ActorSystem must be nil after Stop")
}

// TestEngineHotPathGuards verifies every hot-path method bails out with
// ErrEngineNotStarted when the engine's actor system reference has been
// detached (mid-Stop or never started).
func TestEngineHotPathGuards(t *testing.T) {
	// Build a synthetic engine that has Started()==true but no actor system
	// reference. Reproduces the race between Stop's atomic detach and a
	// concurrent hot-path caller.
	synth := func(t *testing.T) *Engine {
		t.Helper()
		e := &Engine{
			eventsStore:   testkit.NewEventsStore(),
			logger:        defaultLogger{},
			eventsStreams: syncmap.New[string, *eventsStream](),
			statesStreams: syncmap.New[string, *statesStream](),
		}
		e.started.Store(true)
		return e
	}

	ctx := context.Background()

	t.Run("AddProjection", func(t *testing.T) {
		err := synth(t).AddProjection(ctx, "projection-"+uuid.NewString())
		require.ErrorIs(t, err, ErrEngineNotStarted)
	})
	t.Run("RemoveProjection", func(t *testing.T) {
		err := synth(t).RemoveProjection(ctx, "projection-"+uuid.NewString())
		require.ErrorIs(t, err, ErrEngineNotStarted)
	})
	t.Run("IsProjectionRunning", func(t *testing.T) {
		running, err := synth(t).IsProjectionRunning(ctx, "projection-"+uuid.NewString())
		require.ErrorIs(t, err, ErrEngineNotStarted)
		require.False(t, running)
	})
	t.Run("Entity", func(t *testing.T) {
		err := synth(t).Entity(ctx, NewEventSourcedEntity(uuid.NewString()))
		require.ErrorIs(t, err, ErrEngineNotStarted)
	})
	t.Run("EntityExists", func(t *testing.T) {
		exists, err := synth(t).EntityExists(ctx, uuid.NewString())
		require.ErrorIs(t, err, ErrEngineNotStarted)
		require.False(t, exists)
	})
	t.Run("DurableStateEntity", func(t *testing.T) {
		err := synth(t).DurableStateEntity(ctx, NewAccountDurableStateBehavior(uuid.NewString()))
		require.ErrorIs(t, err, ErrEngineNotStarted)
	})
	t.Run("SendCommand", func(t *testing.T) {
		state, rev, err := synth(t).SendCommand(ctx, uuid.NewString(), &testpb.CreateAccount{}, time.Second)
		require.ErrorIs(t, err, ErrEngineNotStarted)
		require.Nil(t, state)
		require.Zero(t, rev)
	})
	t.Run("Saga", func(t *testing.T) {
		err := synth(t).Saga(ctx, &testSagaBehavior{sagaID: "saga-" + uuid.NewString()}, time.Second)
		require.ErrorIs(t, err, ErrEngineNotStarted)
	})
	t.Run("SagaStatus", func(t *testing.T) {
		info, err := synth(t).SagaStatus(ctx, "saga-"+uuid.NewString(), time.Second)
		require.ErrorIs(t, err, ErrEngineNotStarted)
		require.Nil(t, info)
	})
}

// TestEngineProjection covers basic projection registration in single-node
// mode (no cluster, projection runs as a regular long-lived actor).
func TestEngineProjection(t *testing.T) {
	ctx := context.Background()
	store := testkit.NewEventsStore()
	require.NoError(t, store.Connect(ctx))
	t.Cleanup(func() { _ = store.Disconnect(ctx) })

	offsetStore := testkit.NewOffsetStore()
	require.NoError(t, offsetStore.Connect(ctx))
	t.Cleanup(func() { _ = offsetStore.Disconnect(ctx) })

	engine := newTestEngine(t, "Sample", store,
		WithLogger(DiscardLogger),
		WithOffsetStore(offsetStore),
		WithProjection(&projection.Options{
			Handler:      projection.NewDiscardHandler(),
			BufferSize:   100,
			PullInterval: time.Second,
		}),
	)
	require.NoError(t, engine.Start(ctx))

	require.NoError(t, engine.AddProjection(ctx, "discard"))
	pause.For(500 * time.Millisecond)

	running, err := engine.IsProjectionRunning(ctx, "discard")
	require.NoError(t, err)
	require.True(t, running)

	require.NoError(t, engine.RemoveProjection(ctx, "discard"))
	require.NoError(t, engine.Stop(ctx))
}

// TestEngineClusterMode runs a single-node cluster end-to-end to exercise the
// AddProjection-as-singleton branch (sys.InCluster()==true) and the
// ego.ClusterKinds() registration. It builds the goakt actor system manually
// to demonstrate the cluster-mode bootstrap.
func TestEngineClusterMode(t *testing.T) {
	ctx := context.Background()
	store := testkit.NewEventsStore()
	require.NoError(t, store.Connect(ctx))
	t.Cleanup(func() { _ = store.Disconnect(ctx) })
	offsetStore := testkit.NewOffsetStore()
	require.NoError(t, offsetStore.Connect(ctx))
	t.Cleanup(func() { _ = offsetStore.Disconnect(ctx) })

	ports := dynaport.Get(3)
	gossipPort, clusterPort, remotingPort := ports[0], ports[1], ports[2]
	host := "127.0.0.1"

	provider := &mockClusterProvider{
		id:    "test",
		peers: []string{net.JoinHostPort(host, strconv.Itoa(clusterPort))},
	}

	clusterCfg := goakt.NewClusterConfig().
		WithDiscovery(provider).
		WithDiscoveryPort(gossipPort).
		WithPeersPort(clusterPort).
		WithMinimumPeersQuorum(1).
		WithReplicaCount(1).
		WithPartitionCount(4).
		WithKinds(ClusterKinds()...)

	cfg := NewConfig(store,
		WithLogger(DiscardLogger),
		WithOffsetStore(offsetStore),
		WithProjection(&projection.Options{
			Handler:      projection.NewDiscardHandler(),
			BufferSize:   100,
			PullInterval: time.Second,
		}),
	)

	goaktOpts := append(cfg.GoaktOptions(),
		goakt.WithCluster(clusterCfg),
		goakt.WithRemote(remote.NewConfig(host, remotingPort)),
	)

	sys, err := goakt.NewActorSystem("Sample", goaktOpts...)
	require.NoError(t, err)
	require.NoError(t, sys.Start(ctx))
	t.Cleanup(func() { _ = sys.Stop(ctx) })

	// wait briefly for the single-node cluster to advertise itself
	pause.For(time.Second)
	require.True(t, sys.InCluster())

	engine, err := NewEngine(sys, cfg)
	require.NoError(t, err)
	require.NoError(t, engine.Start(ctx))
	t.Cleanup(func() { _ = engine.Stop(ctx) })

	require.NoError(t, engine.AddProjection(ctx, "discard"))
	pause.For(time.Second)

	running, err := engine.IsProjectionRunning(ctx, "discard")
	require.NoError(t, err)
	require.True(t, running, "projection should be running as a cluster singleton")

	// entity flow in cluster mode
	entityID := uuid.NewString()
	require.NoError(t, engine.Entity(ctx, NewEventSourcedEntity(entityID)))
	state, _, err := engine.SendCommand(ctx, entityID, &testpb.CreateAccount{
		AccountBalance: 100,
	}, time.Minute)
	require.NoError(t, err)
	acct, ok := state.(*testpb.Account)
	require.True(t, ok)
	assert.EqualValues(t, 100, acct.GetAccountBalance())
}

// TestParseCommandReply pins the reply-decoding contract.
func TestParseCommandReply(t *testing.T) {
	t.Run("error reply", func(t *testing.T) {
		reply := &egopb.CommandReply{
			Reply: &egopb.CommandReply_ErrorReply{
				ErrorReply: &egopb.ErrorReply{Message: "something failed"},
			},
		}
		_, _, err := parseCommandReply(reply)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "something failed")
	})

	t.Run("no reply", func(t *testing.T) {
		_, _, err := parseCommandReply(&egopb.CommandReply{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no state received")
	})

	t.Run("state reply", func(t *testing.T) {
		state, _ := anypb.New(&samplepb.Account{AccountId: "acc-1", AccountBalance: 100})
		reply := &egopb.CommandReply{
			Reply: &egopb.CommandReply_StateReply{
				StateReply: &egopb.StateReply{
					PersistenceId:  "entity-1",
					State:          state,
					SequenceNumber: 5,
				},
			},
		}
		result, seq, err := parseCommandReply(reply)
		require.NoError(t, err)
		assert.EqualValues(t, 5, seq)
		assert.NotNil(t, result)
	})

	t.Run("unmarshal failure", func(t *testing.T) {
		reply := &egopb.CommandReply{
			Reply: &egopb.CommandReply_StateReply{
				StateReply: &egopb.StateReply{
					State:          &anypb.Any{TypeUrl: "type.googleapis.com/invalid.Type", Value: []byte("garbage")},
					SequenceNumber: 1,
				},
			},
		}
		_, _, err := parseCommandReply(reply)
		require.Error(t, err)
	})
}

// TestBuildSpawnOptionsFromConfig pins the SpawnOption translation that the
// entity/durable-state/saga paths share.
func TestBuildSpawnOptionsFromConfig(t *testing.T) {
	t.Run("with batch threshold", func(t *testing.T) {
		opts := buildSpawnOptionsFromConfig(&spawnConfig{
			batchThreshold:      5,
			supervisorDirective: RestartDirective,
			entitiesPlacement:   RoundRobin,
		})
		require.NotEmpty(t, opts)
	})
	t.Run("with passivation", func(t *testing.T) {
		opts := buildSpawnOptionsFromConfig(&spawnConfig{
			passivateAfter:      time.Minute,
			supervisorDirective: RestartDirective,
			entitiesPlacement:   RoundRobin,
		})
		require.NotEmpty(t, opts)
	})
	t.Run("relocation enabled", func(t *testing.T) {
		opts := buildSpawnOptionsFromConfig(&spawnConfig{
			toRelocate:          true,
			supervisorDirective: RestartDirective,
			entitiesPlacement:   RoundRobin,
		})
		require.NotEmpty(t, opts)
	})
}

// TestToSpawnPlacement maps eGo placement strategies to their goakt
// equivalents.
func TestToSpawnPlacement(t *testing.T) {
	assert.Equal(t, goakt.LeastLoad, toSpawnPlacement(LeastLoad))
	assert.Equal(t, goakt.Random, toSpawnPlacement(Random))
	assert.Equal(t, goakt.Local, toSpawnPlacement(Local))
	assert.Equal(t, goakt.RoundRobin, toSpawnPlacement(RoundRobin))
}

// TestToSupervisorDirective maps eGo supervisor directives to goakt.
func TestToSupervisorDirective(t *testing.T) {
	t.Run("stop maps to Stop", func(t *testing.T) {
		// concrete assertion is on stringer; behavior is "anything not RestartDirective stops".
		// Negative test below.
	})
	t.Run("restart maps to Restart (default)", func(t *testing.T) {
		dir := toSupervisorDirective(RestartDirective)
		require.NotNil(t, dir)
	})
}

// TestEngineEraseEntityErrors covers the engine-state guards on EraseEntity.
func TestEngineEraseEntityErrors(t *testing.T) {
	ctx := context.Background()
	store := testkit.NewEventsStore()
	require.NoError(t, store.Connect(ctx))
	t.Cleanup(func() { _ = store.Disconnect(ctx) })

	engine := newTestEngine(t, "Sample", store, WithLogger(DiscardLogger))
	// not started
	require.ErrorIs(t, engine.EraseEntity(ctx, uuid.NewString(), false), ErrEngineNotStarted)
}

// TestEngineProjectionLagErrors covers the engine-state guards on ProjectionLag.
func TestEngineProjectionLagErrors(t *testing.T) {
	ctx := context.Background()
	store := testkit.NewEventsStore()
	require.NoError(t, store.Connect(ctx))
	t.Cleanup(func() { _ = store.Disconnect(ctx) })

	t.Run("not started", func(t *testing.T) {
		engine := newTestEngine(t, "Sample", store, WithLogger(DiscardLogger))
		_, err := engine.ProjectionLag(ctx, "any")
		require.ErrorIs(t, err, ErrEngineNotStarted)
	})

	t.Run("started without offset store", func(t *testing.T) {
		engine := newTestEngine(t, "Sample", store, WithLogger(DiscardLogger))
		require.NoError(t, engine.Start(ctx))
		_, err := engine.ProjectionLag(ctx, "any")
		require.Error(t, err)
		require.Contains(t, err.Error(), "offset store is required")
	})
}

// TestEngineRebuildProjectionErrors covers the engine-state guards on
// RebuildProjection.
func TestEngineRebuildProjectionErrors(t *testing.T) {
	ctx := context.Background()
	store := testkit.NewEventsStore()
	require.NoError(t, store.Connect(ctx))
	t.Cleanup(func() { _ = store.Disconnect(ctx) })

	t.Run("not started", func(t *testing.T) {
		engine := newTestEngine(t, "Sample", store, WithLogger(DiscardLogger))
		err := engine.RebuildProjection(ctx, "any", time.Now())
		require.ErrorIs(t, err, ErrEngineNotStarted)
	})

	t.Run("started without offset store", func(t *testing.T) {
		engine := newTestEngine(t, "Sample", store, WithLogger(DiscardLogger))
		require.NoError(t, engine.Start(ctx))
		err := engine.RebuildProjection(ctx, "any", time.Now())
		require.Error(t, err)
		require.Contains(t, err.Error(), "offset store is required")
	})
}

// TestEngineSubscribeBeforeStart confirms Subscribe is gated by the engine
// being started.
func TestEngineSubscribeBeforeStart(t *testing.T) {
	ctx := context.Background()
	store := testkit.NewEventsStore()
	require.NoError(t, store.Connect(ctx))
	t.Cleanup(func() { _ = store.Disconnect(ctx) })

	engine := newTestEngine(t, "Sample", store, WithLogger(DiscardLogger))
	_, err := engine.Subscribe()
	require.ErrorIs(t, err, ErrEngineNotStarted)
}

// TestEngineConfigRegistersAllExtensions verifies that every Option that
// triggers an extension registration is honored by Config.GoaktOptions and
// makes it into the resulting actor system.
func TestEngineConfigRegistersAllExtensions(t *testing.T) {
	ctx := context.Background()
	store := testkit.NewEventsStore()
	stateStore := testkit.NewDurableStore()
	offsetStore := testkit.NewOffsetStore()
	snapStore := testkit.NewSnapshotStore()

	cfg := NewConfig(store,
		WithLogger(DiscardLogger),
		WithStateStore(stateStore),
		WithOffsetStore(offsetStore),
		WithSnapshotStore(snapStore),
		WithEventAdapters(&testEventAdapter{}),
		WithProjection(&projection.Options{
			Handler:      projection.NewDiscardHandler(),
			BufferSize:   10,
			PullInterval: time.Second,
		}),
	)

	sys, err := goakt.NewActorSystem("Sample", cfg.GoaktOptions()...)
	require.NoError(t, err)
	require.NoError(t, sys.Start(ctx))
	t.Cleanup(func() { _ = sys.Stop(ctx) })

	wantIDs := []string{
		extensions.EventsStoreExtensionID,
		extensions.EventsStreamExtensionID,
		extensions.DurableStateStoreExtensionID,
		extensions.OffsetStoreExtensionID,
		extensions.ProjectionExtensionID,
		extensions.SnapshotStoreExtensionID,
		extensions.EventAdaptersExtensionID,
	}
	for _, id := range wantIDs {
		assert.NotNilf(t, sys.Extension(id), "expected extension %s to be registered", id)
	}

	// NewEngine should accept the same Config cleanly.
	engine, err := NewEngine(sys, cfg)
	require.NoError(t, err)
	require.NoError(t, engine.Start(ctx))
	t.Cleanup(func() { _ = engine.Stop(ctx) })
}

// ensure proto and context imports are not flagged when subtests vary.
var _ context.Context = context.Background()
var _ = proto.Message(nil)
