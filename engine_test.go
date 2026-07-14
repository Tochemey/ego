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
	"net"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	goakt "github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/remote"
	"github.com/tochemey/goakt/v4/supervisor"
	"github.com/travisjeffery/go-dynaport"
	noopmetric "go.opentelemetry.io/otel/metric/noop"
	nooptrace "go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/tochemey/ego/v4/egopb"
	samplepb "github.com/tochemey/ego/v4/example/examplepb"
	"github.com/tochemey/ego/v4/internal/extensions"
	"github.com/tochemey/ego/v4/internal/pause"
	"github.com/tochemey/ego/v4/internal/syncmap"
	egomock "github.com/tochemey/ego/v4/mocks/ego"
	mockoffsetstore "github.com/tochemey/ego/v4/mocks/offsetstore"
	mockpersistence "github.com/tochemey/ego/v4/mocks/persistence"
	"github.com/tochemey/ego/v4/offsetstore"
	"github.com/tochemey/ego/v4/persistence"
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

// TestEngineMultiNodeRemoteEntitySpawn is a regression test for remote entity
// spawns failing with "dependency type is not registered".
//
// With the default RoundRobin placement, Engine.Entity routes spawns to peer
// nodes. The receiving node deserializes the spawn request's dependencies
// (the behavior and eGo's internal EntityConfig) against its own registry,
// which is populated at NewEngine time from WithEntityKinds. Only node1 ever
// calls Entity(), so every spawn landing on node2 exercises that
// pre-registration path; before the fix those spawns failed because node2's
// registry was only populated by its own (never-issued) Entity() calls.
func TestEngineMultiNodeRemoteEntitySpawn(t *testing.T) {
	ctx := context.Background()
	host := "127.0.0.1"

	ports := dynaport.Get(6)
	gossipAddrs := []string{
		net.JoinHostPort(host, strconv.Itoa(ports[0])),
		net.JoinHostPort(host, strconv.Itoa(ports[3])),
	}

	newNode := func(gossipPort, peersPort, remotingPort int) (goakt.ActorSystem, *Config) {
		store := testkit.NewEventsStore()
		require.NoError(t, store.Connect(ctx))
		t.Cleanup(func() { _ = store.Disconnect(ctx) })

		cfg := NewConfig(store,
			WithLogger(DiscardLogger),
			WithEntityKinds(new(AccountEventSourcedBehavior)),
		)

		provider := &mockClusterProvider{id: "test", peers: gossipAddrs}
		clusterCfg := goakt.NewClusterConfig().
			WithDiscovery(provider).
			WithDiscoveryPort(gossipPort).
			WithPeersPort(peersPort).
			WithMinimumPeersQuorum(1).
			WithReplicaCount(1).
			WithPartitionCount(7).
			WithKinds(ClusterKinds()...)

		goaktOpts := append(cfg.GoaktOptions(),
			goakt.WithCluster(clusterCfg),
			goakt.WithRemote(remote.NewConfig(host, remotingPort)),
		)

		sys, err := goakt.NewActorSystem("Sample", goaktOpts...)
		require.NoError(t, err)
		return sys, cfg
	}

	sys1, cfg1 := newNode(ports[0], ports[1], ports[2])
	sys2, cfg2 := newNode(ports[3], ports[4], ports[5])

	// start both nodes concurrently so they bootstrap the cluster together
	errs := make(chan error, 2)
	go func() { errs <- sys1.Start(ctx) }()
	go func() { errs <- sys2.Start(ctx) }()
	require.NoError(t, <-errs)
	require.NoError(t, <-errs)
	t.Cleanup(func() {
		_ = sys1.Stop(context.Background())
		_ = sys2.Stop(context.Background())
	})

	// wait until both nodes see each other as peers
	require.Eventually(t, func() bool {
		peers1, err1 := sys1.Peers(ctx, time.Second)
		peers2, err2 := sys2.Peers(ctx, time.Second)
		return err1 == nil && err2 == nil && len(peers1) == 1 && len(peers2) == 1
	}, 30*time.Second, 500*time.Millisecond, "the two nodes never formed a cluster")

	engine1, err := NewEngine(sys1, cfg1)
	require.NoError(t, err)
	require.NoError(t, engine1.Start(ctx))
	engine2, err := NewEngine(sys2, cfg2)
	require.NoError(t, err)
	require.NoError(t, engine2.Start(ctx))
	t.Cleanup(func() {
		_ = engine1.Stop(context.Background())
		_ = engine2.Stop(context.Background())
	})

	// Only node1 spawns. With RoundRobin placement over two members, a run of
	// spawns is guaranteed to place some entities on node2, which never called
	// Entity() itself.
	for range 8 {
		entityID := uuid.NewString()
		require.NoError(t, engine1.Entity(ctx, NewEventSourcedEntity(entityID)),
			"remote spawn must succeed on a node that never called Entity() itself")

		// SpawnOn (and therefore Entity) only returns once the actor's
		// registry record is written to the cluster store, so the entity is
		// immediately addressable from this node — no retry needed.
		state, _, err := engine1.SendCommand(ctx, entityID, &testpb.CreateAccount{
			AccountBalance: 100,
		}, time.Minute)
		require.NoError(t, err)
		account, ok := state.(*testpb.Account)
		require.True(t, ok)
		assert.EqualValues(t, 100, account.GetAccountBalance())
	}
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

// TestEngineStartWithTelemetry exercises the telemetry-enabled branch of
// Engine.Start: when WithTelemetry is configured, Start materializes the
// metrics struct and installs the OTel propagator. Both noop tracer and noop
// meter are used to keep the test side-effect free.
func TestEngineStartWithTelemetry(t *testing.T) {
	ctx := context.Background()
	store := testkit.NewEventsStore()
	require.NoError(t, store.Connect(ctx))
	t.Cleanup(func() { _ = store.Disconnect(ctx) })

	tel := &Telemetry{
		Tracer: nooptrace.NewTracerProvider().Tracer("test"),
		Meter:  noopmetric.NewMeterProvider().Meter("test"),
	}
	engine := newTestEngine(t, "Sample", store,
		WithLogger(DiscardLogger),
		WithTelemetry(tel),
	)
	require.NoError(t, engine.Start(ctx))
	require.NotNil(t, engine.metrics, "metrics should be initialized when telemetry is configured")
	require.True(t, engine.Started())
}

// TestEngineEraseEntity covers EraseEntity's happy paths: a no-op when
// `full` is false, the events-only path, and the events+snapshots path.
func TestEngineEraseEntity(t *testing.T) {
	ctx := context.Background()

	t.Run("full=false is a no-op", func(t *testing.T) {
		store := testkit.NewEventsStore()
		require.NoError(t, store.Connect(ctx))
		t.Cleanup(func() { _ = store.Disconnect(ctx) })

		engine := newTestEngine(t, "Sample", store, WithLogger(DiscardLogger))
		require.NoError(t, engine.Start(ctx))

		require.NoError(t, engine.EraseEntity(ctx, uuid.NewString(), false))
	})

	t.Run("full=true with persisted events", func(t *testing.T) {
		store := testkit.NewEventsStore()
		require.NoError(t, store.Connect(ctx))
		t.Cleanup(func() { _ = store.Disconnect(ctx) })

		snapStore := testkit.NewSnapshotStore()
		require.NoError(t, snapStore.Connect(ctx))
		t.Cleanup(func() { _ = snapStore.Disconnect(ctx) })

		engine := newTestEngine(t, "Sample", store,
			WithLogger(DiscardLogger),
			WithSnapshotStore(snapStore),
		)
		require.NoError(t, engine.Start(ctx))

		entityID := uuid.NewString()
		require.NoError(t, engine.Entity(ctx, NewEventSourcedEntity(entityID)))
		_, _, err := engine.SendCommand(ctx, entityID, &testpb.CreateAccount{
			AccountBalance: 100,
		}, time.Minute)
		require.NoError(t, err)

		require.NoError(t, engine.EraseEntity(ctx, entityID, true))

		// Subsequent erase against the same id should be a clean no-op (no
		// events left).
		require.NoError(t, engine.EraseEntity(ctx, entityID, true))
	})

	t.Run("full=true with no events is safe", func(t *testing.T) {
		store := testkit.NewEventsStore()
		require.NoError(t, store.Connect(ctx))
		t.Cleanup(func() { _ = store.Disconnect(ctx) })

		engine := newTestEngine(t, "Sample", store, WithLogger(DiscardLogger))
		require.NoError(t, engine.Start(ctx))

		// Entity that was never used: GetLatestEvent returns nil and
		// EraseEntity should short-circuit without error.
		require.NoError(t, engine.EraseEntity(ctx, uuid.NewString(), true))
	})
}

// TestEngineProjectionLagHappyPath drives ProjectionLag through its full body
// so the per-shard iteration, the empty-shard short-circuit, and the lag
// computation are all exercised.
func TestEngineProjectionLagHappyPath(t *testing.T) {
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
	)
	require.NoError(t, engine.Start(ctx))

	// Fresh stores: every known shard is empty, so the loop should run the
	// empty-shard branch and return a zero-lag map.
	lags, err := engine.ProjectionLag(ctx, "any")
	require.NoError(t, err)
	for shard, lag := range lags {
		assert.Zerof(t, lag, "expected zero lag on empty shard %d", shard)
	}
}

// TestEngineRebuildProjectionSuccess exercises the success branch of
// RebuildProjection: it stops the running projection, resets its offset, and
// restarts it.
func TestEngineRebuildProjectionSuccess(t *testing.T) {
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

	const name = "rebuild-target"
	require.NoError(t, engine.AddProjection(ctx, name))
	pause.For(300 * time.Millisecond)

	require.NoError(t, engine.RebuildProjection(ctx, name, ZeroTime))
	pause.For(300 * time.Millisecond)

	running, err := engine.IsProjectionRunning(ctx, name)
	require.NoError(t, err)
	require.True(t, running, "projection should be running again after rebuild")
}

// TestEngineSagaHappyPath registers a saga via Engine.Saga and then queries
// its status via Engine.SagaStatus, covering the success branches of both.
func TestEngineSagaHappyPath(t *testing.T) {
	ctx := context.Background()
	store := testkit.NewEventsStore()
	require.NoError(t, store.Connect(ctx))
	t.Cleanup(func() { _ = store.Disconnect(ctx) })

	engine := newTestEngine(t, "Sample", store, WithLogger(DiscardLogger))
	require.NoError(t, engine.Start(ctx))

	sagaID := "saga-" + uuid.NewString()
	require.NoError(t, engine.Saga(ctx, &testSagaBehavior{sagaID: sagaID}, 0))
	pause.For(300 * time.Millisecond)

	info, err := engine.SagaStatus(ctx, sagaID, time.Minute)
	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, sagaID, info.ID)
}

// ensure proto and context imports are not flagged when subtests vary.
var _ context.Context = context.Background()
var _ = proto.Message(nil)

// TestEngineAddEventPublishersGuards covers the engine-state guards on
// AddEventPublishers.
func TestEngineAddEventPublishersGuards(t *testing.T) {
	ctx := context.Background()
	store := testkit.NewEventsStore()
	require.NoError(t, store.Connect(ctx))
	t.Cleanup(func() { _ = store.Disconnect(ctx) })

	engine := newTestEngine(t, "Sample", store, WithLogger(DiscardLogger))
	// not started
	pub := new(egomock.EventPublisher)
	err := engine.AddEventPublishers(pub)
	require.ErrorIs(t, err, ErrEngineNotStarted)
}

// TestEngineAddStatePublishersGuards covers the engine-state guards on
// AddStatePublishers.
func TestEngineAddStatePublishersGuards(t *testing.T) {
	ctx := context.Background()
	store := testkit.NewEventsStore()
	require.NoError(t, store.Connect(ctx))
	t.Cleanup(func() { _ = store.Disconnect(ctx) })

	engine := newTestEngine(t, "Sample", store, WithLogger(DiscardLogger))
	// not started
	pub := new(egomock.StatePublisher)
	err := engine.AddStatePublishers(pub)
	require.ErrorIs(t, err, ErrEngineNotStarted)
}

// TestEngineAddEventPublishers exercises the happy path of
// AddEventPublishers: the publisher must observe events generated by an
// event-sourced entity through the in-process stream.
func TestEngineAddEventPublishers(t *testing.T) {
	ctx := context.Background()
	store := testkit.NewEventsStore()
	require.NoError(t, store.Connect(ctx))
	t.Cleanup(func() { _ = store.Disconnect(ctx) })

	published := make(chan struct{}, 4)
	pub := new(egomock.EventPublisher)
	pub.On("ID").Return("eGo.test.EventPublisher")
	pub.On("Close", mock.Anything).Return(nil)
	pub.On("Publish", mock.Anything, mock.AnythingOfType("*egopb.Event")).
		Run(func(_ mock.Arguments) {
			select {
			case published <- struct{}{}:
			default:
			}
		}).
		Return(nil)

	engine := newTestEngine(t, "Sample", store, WithLogger(DiscardLogger))
	require.NoError(t, engine.Start(ctx))
	require.NoError(t, engine.AddEventPublishers(pub))

	entityID := uuid.NewString()
	require.NoError(t, engine.Entity(ctx, NewEventSourcedEntity(entityID)))
	_, _, err := engine.SendCommand(ctx, entityID, &testpb.CreateAccount{
		AccountBalance: 100,
	}, time.Minute)
	require.NoError(t, err)

	select {
	case <-published:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for event publish")
	}
}

// TestEnginePublisherIdleCPU is the regression test for
// https://github.com/Tochemey/ego/issues/291: the publisher consumption
// loops used to poll Subscriber.Iterator() — which returns a closed snapshot
// channel — in a tight select, pinning one full CPU core per registered
// publisher whenever the stream was idle. Post-fix the loops block on the
// subscriber's Ready signal, so an idle engine with publishers must consume
// close to zero CPU.
func TestEnginePublisherIdleCPU(t *testing.T) {
	ctx := context.Background()
	store := testkit.NewEventsStore()
	require.NoError(t, store.Connect(ctx))
	t.Cleanup(func() { _ = store.Disconnect(ctx) })

	eventPub := new(egomock.EventPublisher)
	eventPub.On("ID").Return("eGo.test.EventPublisher")
	eventPub.On("Close", mock.Anything).Return(nil)

	statePub := new(egomock.StatePublisher)
	statePub.On("ID").Return("eGo.test.StatePublisher")
	statePub.On("Close", mock.Anything).Return(nil)

	engine := newTestEngine(t, "Sample", store, WithLogger(DiscardLogger))
	require.NoError(t, engine.Start(ctx))
	require.NoError(t, engine.AddEventPublishers(eventPub))
	require.NoError(t, engine.AddStatePublishers(statePub))

	// let startup work settle before sampling
	pause.For(500 * time.Millisecond)

	cpuStart := processCPUTime(t)
	const idle = 2 * time.Second
	pause.For(idle)
	cpuBurned := processCPUTime(t) - cpuStart

	// Pre-fix, each of the two idle consumption loops burned a full core
	// (~2s of CPU each over the 2s window). The threshold leaves generous
	// headroom for runtime and actor-system background work while still
	// catching any loop that spins instead of blocking.
	require.Less(t, cpuBurned, idle/2,
		"idle publisher loops burned %v of CPU over %v of wall time: busy-spin regression", cpuBurned, idle)
}

// processCPUTime returns the cumulative user+system CPU time of the test
// process.
func processCPUTime(t *testing.T) time.Duration {
	t.Helper()
	var usage syscall.Rusage
	require.NoError(t, syscall.Getrusage(syscall.RUSAGE_SELF, &usage))
	return time.Duration(usage.Utime.Nano() + usage.Stime.Nano())
}

// TestEngineAddStatePublishers exercises the happy path of
// AddStatePublishers: the publisher must observe durable-state updates
// generated by a durable-state entity.
func TestEngineAddStatePublishers(t *testing.T) {
	ctx := context.Background()
	stateStore := testkit.NewDurableStore()
	require.NoError(t, stateStore.Connect(ctx))
	t.Cleanup(func() { _ = stateStore.Disconnect(ctx) })

	published := make(chan struct{}, 4)
	pub := new(egomock.StatePublisher)
	pub.On("ID").Return("eGo.test.StatePublisher")
	pub.On("Close", mock.Anything).Return(nil)
	pub.On("Publish", mock.Anything, mock.AnythingOfType("*egopb.DurableState")).
		Run(func(_ mock.Arguments) {
			select {
			case published <- struct{}{}:
			default:
			}
		}).
		Return(nil)

	engine := newTestEngine(t, "Sample", nil,
		WithLogger(DiscardLogger),
		WithStateStore(stateStore),
	)
	require.NoError(t, engine.Start(ctx))
	require.NoError(t, engine.AddStatePublishers(pub))

	entityID := uuid.NewString()
	require.NoError(t, engine.DurableStateEntity(ctx, NewAccountDurableStateBehavior(entityID)))
	_, _, err := engine.SendCommand(ctx, entityID, &testpb.CreateAccount{
		AccountBalance: 100,
	}, time.Minute)
	require.NoError(t, err)

	select {
	case <-published:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for state publish")
	}
}

// TestEngineStopReturnsEventPublisherCloseError verifies that a failure in an
// EventPublisher's Close surfaces from Engine.Stop.
func TestEngineStopReturnsEventPublisherCloseError(t *testing.T) {
	ctx := context.Background()
	store := testkit.NewEventsStore()
	require.NoError(t, store.Connect(ctx))
	t.Cleanup(func() { _ = store.Disconnect(ctx) })

	closeErr := errors.New("close error")
	pub := new(egomock.EventPublisher)
	pub.On("ID").Return("eGo.test.FailingEventPublisher")
	pub.On("Close", mock.Anything).Return(closeErr)

	cfg := NewConfig(store, WithLogger(DiscardLogger))
	sys, err := goakt.NewActorSystem("Sample", cfg.GoaktOptions()...)
	require.NoError(t, err)
	require.NoError(t, sys.Start(ctx))
	t.Cleanup(func() { _ = sys.Stop(ctx) })

	engine, err := NewEngine(sys, cfg)
	require.NoError(t, err)
	require.NoError(t, engine.Start(ctx))
	require.NoError(t, engine.AddEventPublishers(pub))

	require.ErrorIs(t, engine.Stop(ctx), closeErr)
}

// TestEngineStopReturnsStatePublisherCloseError verifies that a failure in a
// StatePublisher's Close surfaces from Engine.Stop.
func TestEngineStopReturnsStatePublisherCloseError(t *testing.T) {
	ctx := context.Background()
	stateStore := testkit.NewDurableStore()
	require.NoError(t, stateStore.Connect(ctx))
	t.Cleanup(func() { _ = stateStore.Disconnect(ctx) })

	closeErr := errors.New("close error")
	pub := new(egomock.StatePublisher)
	pub.On("ID").Return("eGo.test.FailingStatePublisher")
	pub.On("Close", mock.Anything).Return(closeErr)

	cfg := NewConfig(nil, WithLogger(DiscardLogger), WithStateStore(stateStore))
	sys, err := goakt.NewActorSystem("Sample", cfg.GoaktOptions()...)
	require.NoError(t, err)
	require.NoError(t, sys.Start(ctx))
	t.Cleanup(func() { _ = sys.Stop(ctx) })

	engine, err := NewEngine(sys, cfg)
	require.NoError(t, err)
	require.NoError(t, engine.Start(ctx))
	require.NoError(t, engine.AddStatePublishers(pub))

	require.ErrorIs(t, engine.Stop(ctx), closeErr)
}

// TestEngineEventPublisherKeepsGoingOnPublishError ensures that a failing
// Publish does not stall or kill the sendEvent goroutine: a subsequent event
// must still be delivered.
func TestEngineEventPublisherKeepsGoingOnPublishError(t *testing.T) {
	ctx := context.Background()
	store := testkit.NewEventsStore()
	require.NoError(t, store.Connect(ctx))
	t.Cleanup(func() { _ = store.Disconnect(ctx) })

	published := make(chan struct{}, 4)
	pub := new(egomock.EventPublisher)
	pub.On("ID").Return("eGo.test.FailingPublisher")
	pub.On("Close", mock.Anything).Return(nil)
	pub.On("Publish", mock.Anything, mock.AnythingOfType("*egopb.Event")).
		Run(func(_ mock.Arguments) {
			select {
			case published <- struct{}{}:
			default:
			}
		}).
		Return(assert.AnError)

	engine := newTestEngine(t, "Sample", store, WithLogger(DiscardLogger))
	require.NoError(t, engine.Start(ctx))
	require.NoError(t, engine.AddEventPublishers(pub))

	entityID := uuid.NewString()
	require.NoError(t, engine.Entity(ctx, NewEventSourcedEntity(entityID)))
	_, _, err := engine.SendCommand(ctx, entityID, &testpb.CreateAccount{
		AccountBalance: 100,
	}, time.Minute)
	require.NoError(t, err)

	select {
	case <-published:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for first publish attempt")
	}

	_, _, err = engine.SendCommand(ctx, entityID, &testpb.CreditAccount{
		AccountId: entityID, Balance: 25,
	}, time.Minute)
	require.NoError(t, err)

	select {
	case <-published:
	case <-time.After(5 * time.Second):
		t.Fatal("sendEvent goroutine appears to have stopped after a publish error")
	}
}

// TestEngineStatePublisherKeepsGoingOnPublishError ensures that a failing
// state Publish does not kill the sendState goroutine.
func TestEngineStatePublisherKeepsGoingOnPublishError(t *testing.T) {
	ctx := context.Background()
	stateStore := testkit.NewDurableStore()
	require.NoError(t, stateStore.Connect(ctx))
	t.Cleanup(func() { _ = stateStore.Disconnect(ctx) })

	published := make(chan struct{}, 4)
	pub := new(egomock.StatePublisher)
	pub.On("ID").Return("eGo.test.FailingStatePublisher")
	pub.On("Close", mock.Anything).Return(nil)
	pub.On("Publish", mock.Anything, mock.AnythingOfType("*egopb.DurableState")).
		Run(func(_ mock.Arguments) {
			select {
			case published <- struct{}{}:
			default:
			}
		}).
		Return(assert.AnError)

	engine := newTestEngine(t, "Sample", nil,
		WithLogger(DiscardLogger),
		WithStateStore(stateStore),
	)
	require.NoError(t, engine.Start(ctx))
	require.NoError(t, engine.AddStatePublishers(pub))

	entityID := uuid.NewString()
	require.NoError(t, engine.DurableStateEntity(ctx, NewAccountDurableStateBehavior(entityID)))
	_, _, err := engine.SendCommand(ctx, entityID, &testpb.CreateAccount{
		AccountBalance: 100,
	}, time.Minute)
	require.NoError(t, err)

	select {
	case <-published:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for first state publish attempt")
	}

	_, _, err = engine.SendCommand(ctx, entityID, &testpb.CreditAccount{
		AccountId: entityID, Balance: 25,
	}, time.Minute)
	require.NoError(t, err)

	select {
	case <-published:
	case <-time.After(5 * time.Second):
		t.Fatal("sendState goroutine appears to have stopped after a publish error")
	}
}

// TestEngineSendCommandWithTelemetry exercises the telemetry-instrumented
// branch of SendCommand for both the success and error paths.
func TestEngineSendCommandWithTelemetry(t *testing.T) {
	ctx := context.Background()
	store := testkit.NewEventsStore()
	require.NoError(t, store.Connect(ctx))
	t.Cleanup(func() { _ = store.Disconnect(ctx) })

	tel := &Telemetry{
		Tracer: nooptrace.NewTracerProvider().Tracer("test"),
		Meter:  noopmetric.NewMeterProvider().Meter("test"),
	}
	engine := newTestEngine(t, "Sample", store,
		WithLogger(DiscardLogger),
		WithTelemetry(tel),
	)
	require.NoError(t, engine.Start(ctx))

	entityID := uuid.NewString()
	require.NoError(t, engine.Entity(ctx, NewEventSourcedEntity(entityID)))

	state, _, err := engine.SendCommand(ctx, entityID, &testpb.CreateAccount{
		AccountBalance: 42,
	}, time.Minute)
	require.NoError(t, err)
	require.NotNil(t, state)

	// Error path: an unknown entity hits a send timeout, exercising the
	// span.RecordError branch.
	_, _, err = engine.SendCommand(ctx, "missing-"+uuid.NewString(),
		&testpb.CreateAccount{AccountBalance: 1}, 10*time.Millisecond)
	require.Error(t, err)
}

// TestToSupervisorDirectiveStop covers the StopDirective branch.
func TestToSupervisorDirectiveStop(t *testing.T) {
	assert.Equal(t, supervisor.StopDirective, toSupervisorDirective(StopDirective))
	assert.Equal(t, supervisor.RestartDirective, toSupervisorDirective(RestartDirective))
}

// TestEngineStartWithoutActorSystem covers the guard in Start that returns
// ErrActorSystemRequired when the engine's atomic actor-system reference has
// been detached (e.g. mid-shutdown or in a manually-constructed instance).
func TestEngineStartWithoutActorSystem(t *testing.T) {
	e := &Engine{
		eventsStore:   testkit.NewEventsStore(),
		logger:        defaultLogger{},
		eventsStreams: syncmap.New[string, *eventsStream](),
		statesStreams: syncmap.New[string, *statesStream](),
	}
	require.ErrorIs(t, e.Start(context.Background()), ErrActorSystemRequired)
}

// TestEngineProjectionLagClampsNegative seeds the offset store with a value
// far in the future so that latestTimestamp - currOffset is negative,
// exercising the lag clamp in ProjectionLag.
func TestEngineProjectionLagClampsNegative(t *testing.T) {
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
	)
	require.NoError(t, engine.Start(ctx))

	// Produce an event so the shard is non-empty and we follow the
	// latestTimestamp/currOffset arithmetic branch.
	entityID := uuid.NewString()
	require.NoError(t, engine.Entity(ctx, NewEventSourcedEntity(entityID)))
	_, _, err := engine.SendCommand(ctx, entityID, &testpb.CreateAccount{
		AccountBalance: 100,
	}, time.Minute)
	require.NoError(t, err)

	// Discover the populated shards and stamp a future offset on each so
	// currOffset > latestTimestamp and the clamp branch fires.
	shards, err := store.ShardNumbers(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, shards)

	const projectionName = "future-projection"
	future := time.Now().Add(24 * time.Hour).UnixNano()
	for _, shard := range shards {
		require.NoError(t, offsetStore.WriteOffset(ctx, &egopb.Offset{
			ProjectionName: projectionName,
			ShardNumber:    shard,
			Value:          future,
			Timestamp:      time.Now().UnixMilli(),
		}))
	}

	lags, err := engine.ProjectionLag(ctx, projectionName)
	require.NoError(t, err)
	require.NotEmpty(t, lags)
	for shard, lag := range lags {
		assert.Equalf(t, time.Duration(0), lag,
			"expected clamped zero lag on shard %d when offset is in the future", shard)
	}
}

// TestEngineNotStartedGuardsDirect drives the top-of-function
// `!engine.Started()` short-circuit on every API that has one. The synth
// engine used by TestEngineHotPathGuards has started=true and exercises the
// "ref==nil" branch; this test complements it by exercising the "started==
// false" branch with a real engine that simply has not been started.
func TestEngineNotStartedGuardsDirect(t *testing.T) {
	ctx := context.Background()
	store := testkit.NewEventsStore()
	require.NoError(t, store.Connect(ctx))
	t.Cleanup(func() { _ = store.Disconnect(ctx) })

	newEngine := func() *Engine {
		return newTestEngine(t, "Sample", store,
			WithLogger(DiscardLogger),
			WithStateStore(testkit.NewDurableStore()),
		)
	}

	t.Run("AddProjection", func(t *testing.T) {
		require.ErrorIs(t, newEngine().AddProjection(ctx, "p"), ErrEngineNotStarted)
	})
	t.Run("RemoveProjection", func(t *testing.T) {
		require.ErrorIs(t, newEngine().RemoveProjection(ctx, "p"), ErrEngineNotStarted)
	})
	t.Run("IsProjectionRunning", func(t *testing.T) {
		running, err := newEngine().IsProjectionRunning(ctx, "p")
		require.ErrorIs(t, err, ErrEngineNotStarted)
		require.False(t, running)
	})
	t.Run("Entity", func(t *testing.T) {
		require.ErrorIs(t,
			newEngine().Entity(ctx, NewEventSourcedEntity(uuid.NewString())),
			ErrEngineNotStarted)
	})
	t.Run("DurableStateEntity", func(t *testing.T) {
		require.ErrorIs(t,
			newEngine().DurableStateEntity(ctx, NewAccountDurableStateBehavior(uuid.NewString())),
			ErrEngineNotStarted)
	})
	t.Run("Saga", func(t *testing.T) {
		require.ErrorIs(t,
			newEngine().Saga(ctx, &testSagaBehavior{sagaID: "s"}, 0),
			ErrEngineNotStarted)
	})
	t.Run("SagaStatus", func(t *testing.T) {
		info, err := newEngine().SagaStatus(ctx, "s", time.Second)
		require.ErrorIs(t, err, ErrEngineNotStarted)
		require.Nil(t, info)
	})
}

// TestEngineIsProjectionRunningActorOfError covers the ActorOf-failure
// branch of IsProjectionRunning. Asking for an actor name that does not
// exist makes the underlying actor system surface an error, which the
// engine wraps.
func TestEngineIsProjectionRunningActorOfError(t *testing.T) {
	ctx := context.Background()
	store := testkit.NewEventsStore()
	require.NoError(t, store.Connect(ctx))
	t.Cleanup(func() { _ = store.Disconnect(ctx) })

	engine := newTestEngine(t, "Sample", store, WithLogger(DiscardLogger))
	require.NoError(t, engine.Start(ctx))

	running, err := engine.IsProjectionRunning(ctx, "missing-projection-"+uuid.NewString())
	require.Error(t, err)
	require.False(t, running)
}

// TestEngineAddProjectionStandaloneSpawnError covers the spawn-failure wrap
// in AddProjection's standalone branch (lines 378-379). The actor system is
// stopped out from under the engine so that the next Spawn call returns
// ErrActorSystemNotStarted, which the engine wraps.
func TestEngineAddProjectionStandaloneSpawnError(t *testing.T) {
	ctx := context.Background()
	store := testkit.NewEventsStore()
	require.NoError(t, store.Connect(ctx))
	t.Cleanup(func() { _ = store.Disconnect(ctx) })

	offsetStore := testkit.NewOffsetStore()
	require.NoError(t, offsetStore.Connect(ctx))
	t.Cleanup(func() { _ = offsetStore.Disconnect(ctx) })

	cfg := NewConfig(store,
		WithLogger(DiscardLogger),
		WithOffsetStore(offsetStore),
		WithProjection(&projection.Options{
			Handler:      projection.NewDiscardHandler(),
			BufferSize:   100,
			PullInterval: time.Second,
		}),
	)
	sys, err := goakt.NewActorSystem("Sample", cfg.GoaktOptions()...)
	require.NoError(t, err)
	require.NoError(t, sys.Start(ctx))

	engine, err := NewEngine(sys, cfg)
	require.NoError(t, err)
	require.NoError(t, engine.Start(ctx))

	// Stop the actor system; the engine still believes it's running, so
	// the AddProjection call falls through to the Spawn-in-standalone path
	// and gets ErrActorSystemNotStarted from goakt.
	require.NoError(t, sys.Stop(ctx))

	err = engine.AddProjection(ctx, "boom-projection")
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to register the projection")
}

// TestEngineEntityWithRetentionPolicy exercises the retention-policy block
// in Entity (lines 552-556) by passing a non-nil RetentionPolicy.
func TestEngineEntityWithRetentionPolicy(t *testing.T) {
	ctx := context.Background()
	store := testkit.NewEventsStore()
	require.NoError(t, store.Connect(ctx))
	t.Cleanup(func() { _ = store.Disconnect(ctx) })

	snapStore := testkit.NewSnapshotStore()
	require.NoError(t, snapStore.Connect(ctx))
	t.Cleanup(func() { _ = snapStore.Disconnect(ctx) })

	engine := newTestEngine(t, "Sample", store,
		WithLogger(DiscardLogger),
		WithSnapshotStore(snapStore),
	)
	require.NoError(t, engine.Start(ctx))

	entityID := uuid.NewString()
	require.NoError(t, engine.Entity(ctx, NewEventSourcedEntity(entityID),
		WithRetentionPolicy(RetentionPolicy{
			DeleteEventsOnSnapshot:    true,
			DeleteSnapshotsOnSnapshot: true,
			EventsRetentionCount:      3,
		}),
	))
}

// TestEngineSendCommandUnexpectedReply pins the ErrCommandReplyUnmarshalling
// branch: when the reply is not a *egopb.CommandReply, SendCommand surfaces
// that sentinel error.
func TestEngineSendCommandUnexpectedReply(t *testing.T) {
	ctx := context.Background()
	store := testkit.NewEventsStore()
	require.NoError(t, store.Connect(ctx))
	t.Cleanup(func() { _ = store.Disconnect(ctx) })

	engine := newTestEngine(t, "Sample", store, WithLogger(DiscardLogger))
	require.NoError(t, engine.Start(ctx))

	// Spawn a plain goakt actor under a known name that replies with a
	// non-CommandReply proto; SendCommand routes there by entityID.
	sys := engine.ActorSystem()
	require.NotNil(t, sys)
	entityID := "weird-" + uuid.NewString()
	_, err := sys.Spawn(ctx, entityID,
		&simpleReplyActor{reply: &samplepb.Account{AccountId: entityID}},
		goakt.WithLongLived())
	require.NoError(t, err)

	state, rev, err := engine.SendCommand(ctx, entityID, &testpb.CreateAccount{
		AccountBalance: 1,
	}, time.Minute)
	require.ErrorIs(t, err, ErrCommandReplyUnmarshalling)
	require.Nil(t, state)
	require.Zero(t, rev)
}

// TestEngineSagaStatusErrorPaths covers the three error branches of
// SagaStatus that follow the not-started guard: SendSync failure, an
// unexpected reply type, and parseCommandReply returning an error.
func TestEngineSagaStatusErrorPaths(t *testing.T) {
	ctx := context.Background()
	store := testkit.NewEventsStore()
	require.NoError(t, store.Connect(ctx))
	t.Cleanup(func() { _ = store.Disconnect(ctx) })

	engine := newTestEngine(t, "Sample", store, WithLogger(DiscardLogger))
	require.NoError(t, engine.Start(ctx))

	t.Run("empty saga id", func(t *testing.T) {
		info, err := engine.SagaStatus(ctx, "", time.Second)
		require.ErrorIs(t, err, ErrUndefinedEntityID)
		require.Nil(t, info)
	})

	t.Run("SendSync failure", func(t *testing.T) {
		info, err := engine.SagaStatus(ctx, "missing-saga-"+uuid.NewString(), 10*time.Millisecond)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to get saga status")
		require.Nil(t, info)
	})

	sys := engine.ActorSystem()
	require.NotNil(t, sys)

	t.Run("unexpected reply type", func(t *testing.T) {
		sagaID := "saga-bad-reply-" + uuid.NewString()
		_, err := sys.Spawn(ctx, sagaID,
			&simpleReplyActor{reply: &samplepb.Account{}},
			goakt.WithLongLived())
		require.NoError(t, err)
		info, err := engine.SagaStatus(ctx, sagaID, time.Minute)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unexpected reply type from saga")
		require.Nil(t, info)
	})

	t.Run("parseCommandReply error reply", func(t *testing.T) {
		sagaID := "saga-error-reply-" + uuid.NewString()
		errReply := &egopb.CommandReply{
			Reply: &egopb.CommandReply_ErrorReply{
				ErrorReply: &egopb.ErrorReply{Message: "saga is sick"},
			},
		}
		_, err := sys.Spawn(ctx, sagaID,
			&simpleReplyActor{reply: errReply},
			goakt.WithLongLived())
		require.NoError(t, err)
		info, err := engine.SagaStatus(ctx, sagaID, time.Minute)
		require.Error(t, err)
		require.Contains(t, err.Error(), "saga is sick")
		require.Nil(t, info)
	})
}

// synthEngineWithStores builds a minimal Engine whose state-machine looks
// "started" to the API but whose stores are injected directly. The actor
// system is NOT populated: any code path that touches it via
// engine.actorSystem.Load() will fail, so this synth is appropriate only for
// methods that read stores after their started-guard (EraseEntity,
// ProjectionLag, ...).
func synthEngineWithStores(eventsStore persistence.EventsStore, snapStore persistence.SnapshotStore, offsetStore offsetstore.OffsetStore) *Engine {
	e := &Engine{
		eventsStore:   eventsStore,
		snapshotStore: snapStore,
		offsetStore:   offsetStore,
		logger:        defaultLogger{},
		eventsStreams: syncmap.New[string, *eventsStream](),
		statesStreams: syncmap.New[string, *statesStream](),
	}
	e.started.Store(true)
	return e
}

// TestEngineEraseEntityStoreErrors covers the three error wraps in
// EraseEntity's full-erase block (lines 906-918): GetLatestEvent failure,
// DeleteEvents failure, and DeleteSnapshots failure.
func TestEngineEraseEntityStoreErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("GetLatestEvent error", func(t *testing.T) {
		eventsStore := new(mockpersistence.EventsStore)
		eventsStore.On("GetLatestEvent", mock.Anything, mock.AnythingOfType("string")).
			Return(nil, errors.New("boom"))

		engine := synthEngineWithStores(eventsStore, nil, nil)
		err := engine.EraseEntity(ctx, "pid-1", true)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to get latest event for erasure")
	})

	t.Run("DeleteEvents error", func(t *testing.T) {
		eventsStore := new(mockpersistence.EventsStore)
		eventsStore.On("GetLatestEvent", mock.Anything, mock.AnythingOfType("string")).
			Return(&egopb.Event{SequenceNumber: 5}, nil)
		eventsStore.On("DeleteEvents", mock.Anything, mock.AnythingOfType("string"), uint64(5)).
			Return(errors.New("delete fail"))

		engine := synthEngineWithStores(eventsStore, nil, nil)
		err := engine.EraseEntity(ctx, "pid-2", true)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to delete events for erasure")
	})

	t.Run("DeleteSnapshots error", func(t *testing.T) {
		eventsStore := new(mockpersistence.EventsStore)
		eventsStore.On("GetLatestEvent", mock.Anything, mock.AnythingOfType("string")).
			Return(&egopb.Event{SequenceNumber: 7}, nil)
		eventsStore.On("DeleteEvents", mock.Anything, mock.AnythingOfType("string"), uint64(7)).
			Return(nil)

		snapStore := new(mockpersistence.SnapshotStore)
		snapStore.On("DeleteSnapshots", mock.Anything, mock.AnythingOfType("string"), uint64(7)).
			Return(errors.New("snap fail"))

		engine := synthEngineWithStores(eventsStore, snapStore, nil)
		err := engine.EraseEntity(ctx, "pid-3", true)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to delete snapshots for erasure")
	})
}

// TestEngineProjectionLagStoreErrors covers ProjectionLag's per-store error
// wraps: ShardNumbers failure, GetCurrentOffset failure, and both
// GetShardEvents failure points.
func TestEngineProjectionLagStoreErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("ShardNumbers failure", func(t *testing.T) {
		eventsStore := new(mockpersistence.EventsStore)
		eventsStore.On("ShardNumbers", mock.Anything).Return([]uint64{}, errors.New("shards down"))
		offsetStore := new(mockoffsetstore.OffsetStore)

		engine := synthEngineWithStores(eventsStore, nil, offsetStore)
		lags, err := engine.ProjectionLag(ctx, "any")
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to fetch shard numbers")
		require.Nil(t, lags)
	})

	t.Run("GetCurrentOffset failure", func(t *testing.T) {
		eventsStore := new(mockpersistence.EventsStore)
		eventsStore.On("ShardNumbers", mock.Anything).Return([]uint64{1}, nil)

		offsetStore := new(mockoffsetstore.OffsetStore)
		offsetStore.On("GetCurrentOffset", mock.Anything, mock.AnythingOfType("*egopb.ProjectionId")).
			Return(nil, errors.New("offset down"))

		engine := synthEngineWithStores(eventsStore, nil, offsetStore)
		lags, err := engine.ProjectionLag(ctx, "any")
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to get offset for shard")
		require.Nil(t, lags)
	})

	t.Run("first GetShardEvents failure", func(t *testing.T) {
		eventsStore := new(mockpersistence.EventsStore)
		eventsStore.On("ShardNumbers", mock.Anything).Return([]uint64{1}, nil)
		eventsStore.On("GetShardEvents", mock.Anything, uint64(1), int64(0), uint64(1)).
			Return([]*egopb.Event(nil), int64(0), errors.New("probe down"))

		offsetStore := new(mockoffsetstore.OffsetStore)
		offsetStore.On("GetCurrentOffset", mock.Anything, mock.AnythingOfType("*egopb.ProjectionId")).
			Return(&egopb.Offset{Value: 0}, nil)

		engine := synthEngineWithStores(eventsStore, nil, offsetStore)
		lags, err := engine.ProjectionLag(ctx, "any")
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to get latest event for shard")
		require.Nil(t, lags)
	})

	t.Run("second GetShardEvents failure", func(t *testing.T) {
		eventsStore := new(mockpersistence.EventsStore)
		eventsStore.On("ShardNumbers", mock.Anything).Return([]uint64{1}, nil)
		// Probe succeeds, returning one event so the loop falls through to
		// the larger scan.
		eventsStore.On("GetShardEvents", mock.Anything, uint64(1), int64(0), uint64(1)).
			Return([]*egopb.Event{{Timestamp: 100}}, int64(0), nil)
		// Larger scan fails.
		eventsStore.On("GetShardEvents", mock.Anything, uint64(1), int64(0), uint64(10000)).
			Return([]*egopb.Event(nil), int64(0), errors.New("scan down"))

		offsetStore := new(mockoffsetstore.OffsetStore)
		offsetStore.On("GetCurrentOffset", mock.Anything, mock.AnythingOfType("*egopb.ProjectionId")).
			Return(&egopb.Offset{Value: 0}, nil)

		engine := synthEngineWithStores(eventsStore, nil, offsetStore)
		lags, err := engine.ProjectionLag(ctx, "any")
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to get events for shard")
		require.Nil(t, lags)
	})
}

// TestEngineProjectionLagShardContinueBranches drives the two
// "empty-events -> zero-lag, continue" branches inside ProjectionLag.
//
// Branch 1 (probe empty): GetShardEvents at limit=1 returns no events;
// the loop records 0 and continues without doing the larger scan.
//
// Branch 2 (probe sees events but the larger scan returns none): the
// defensive `if len(allEvents) == 0` short-circuit also records 0 and
// continues. This second branch is unreachable from the real testkit
// store but is exercised here with a mock to lock the behavior in.
func TestEngineProjectionLagShardContinueBranches(t *testing.T) {
	ctx := context.Background()

	t.Run("probe returns no events", func(t *testing.T) {
		eventsStore := new(mockpersistence.EventsStore)
		eventsStore.On("ShardNumbers", mock.Anything).Return([]uint64{1}, nil)
		eventsStore.On("GetShardEvents", mock.Anything, uint64(1), int64(0), uint64(1)).
			Return([]*egopb.Event{}, int64(0), nil)

		offsetStore := new(mockoffsetstore.OffsetStore)
		offsetStore.On("GetCurrentOffset", mock.Anything, mock.AnythingOfType("*egopb.ProjectionId")).
			Return(&egopb.Offset{Value: 0}, nil)

		engine := synthEngineWithStores(eventsStore, nil, offsetStore)
		lags, err := engine.ProjectionLag(ctx, "any")
		require.NoError(t, err)
		require.Equal(t, time.Duration(0), lags[1])
	})

	t.Run("larger scan returns no events", func(t *testing.T) {
		eventsStore := new(mockpersistence.EventsStore)
		eventsStore.On("ShardNumbers", mock.Anything).Return([]uint64{2}, nil)
		eventsStore.On("GetShardEvents", mock.Anything, uint64(2), int64(0), uint64(1)).
			Return([]*egopb.Event{{Timestamp: 100}}, int64(0), nil)
		eventsStore.On("GetShardEvents", mock.Anything, uint64(2), int64(0), uint64(10000)).
			Return([]*egopb.Event{}, int64(0), nil)

		offsetStore := new(mockoffsetstore.OffsetStore)
		offsetStore.On("GetCurrentOffset", mock.Anything, mock.AnythingOfType("*egopb.ProjectionId")).
			Return(&egopb.Offset{Value: 0}, nil)

		engine := synthEngineWithStores(eventsStore, nil, offsetStore)
		lags, err := engine.ProjectionLag(ctx, "any")
		require.NoError(t, err)
		require.Equal(t, time.Duration(0), lags[2])
	})
}

// TestEngineSagaSpawnError covers the Spawn-failure wrap in Engine.Saga
// (line 838). Same trick as the projection variant: stop the actor system
// while the engine still believes it owns one.
func TestEngineSagaSpawnError(t *testing.T) {
	ctx := context.Background()
	store := testkit.NewEventsStore()
	require.NoError(t, store.Connect(ctx))
	t.Cleanup(func() { _ = store.Disconnect(ctx) })

	cfg := NewConfig(store, WithLogger(DiscardLogger))
	sys, err := goakt.NewActorSystem("Sample", cfg.GoaktOptions()...)
	require.NoError(t, err)
	require.NoError(t, sys.Start(ctx))

	engine, err := NewEngine(sys, cfg)
	require.NoError(t, err)
	require.NoError(t, engine.Start(ctx))

	require.NoError(t, sys.Stop(ctx))

	err = engine.Saga(ctx, &testSagaBehavior{sagaID: "doomed-saga"}, time.Second)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to start saga")
}

// TestEngineRebuildProjectionRemoveError covers RebuildProjection's
// "failed to stop projection" branch (lines 470-472). Rebuilding a name
// that was never registered makes the internal RemoveProjection call fail
// because Kill cannot find the actor.
func TestEngineRebuildProjectionRemoveError(t *testing.T) {
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
	)
	require.NoError(t, engine.Start(ctx))

	err := engine.RebuildProjection(ctx, "never-registered-"+uuid.NewString(), ZeroTime)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to stop projection")
}

// TestEngineRebuildProjectionResetOffsetError covers the
// "failed to reset offset" branch (lines 475-477). A real projection is
// added so RemoveProjection succeeds, then the engine's offset store is
// swapped for a mock that fails on ResetOffset.
func TestEngineRebuildProjectionResetOffsetError(t *testing.T) {
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

	const name = "rebuild-reset-error"
	require.NoError(t, engine.AddProjection(ctx, name))
	pause.For(200 * time.Millisecond)

	// Swap the offset store for one that fails on ResetOffset so the
	// rebuild path takes the ResetOffset-error branch.
	bad := new(mockoffsetstore.OffsetStore)
	bad.On("ResetOffset", mock.Anything, name, mock.AnythingOfType("int64")).
		Return(errors.New("reset boom"))
	engine.mutex.Lock()
	engine.offsetStore = bad
	engine.mutex.Unlock()

	err := engine.RebuildProjection(ctx, name, ZeroTime)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to reset offset")
}

// TestEngineRebuildProjectionRestartError covers the
// "failed to restart projection" branch (lines 480-482). A successful
// RemoveProjection + ResetOffset sequence is followed by an AddProjection
// that fails because the actor system is stopped from inside the offset
// store mock just before the restart runs.
func TestEngineRebuildProjectionRestartError(t *testing.T) {
	ctx := context.Background()
	store := testkit.NewEventsStore()
	require.NoError(t, store.Connect(ctx))
	t.Cleanup(func() { _ = store.Disconnect(ctx) })

	offsetStore := testkit.NewOffsetStore()
	require.NoError(t, offsetStore.Connect(ctx))
	t.Cleanup(func() { _ = offsetStore.Disconnect(ctx) })

	cfg := NewConfig(store,
		WithLogger(DiscardLogger),
		WithOffsetStore(offsetStore),
		WithProjection(&projection.Options{
			Handler:      projection.NewDiscardHandler(),
			BufferSize:   100,
			PullInterval: time.Second,
		}),
	)
	sys, err := goakt.NewActorSystem("Sample", cfg.GoaktOptions()...)
	require.NoError(t, err)
	require.NoError(t, sys.Start(ctx))

	engine, err := NewEngine(sys, cfg)
	require.NoError(t, err)
	require.NoError(t, engine.Start(ctx))

	const name = "rebuild-restart-error"
	require.NoError(t, engine.AddProjection(ctx, name))
	pause.For(200 * time.Millisecond)

	// Inject a mock offset store whose ResetOffset stops the actor system
	// in-place. The subsequent AddProjection call inside RebuildProjection
	// will then see a not-running actor system and fail.
	bad := new(mockoffsetstore.OffsetStore)
	bad.On("ResetOffset", mock.Anything, name, mock.AnythingOfType("int64")).
		Run(func(_ mock.Arguments) {
			_ = sys.Stop(ctx)
		}).
		Return(nil)

	engine.mutex.Lock()
	engine.offsetStore = bad
	engine.mutex.Unlock()

	err = engine.RebuildProjection(ctx, name, ZeroTime)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to restart projection")
}

// TestEngineClusterModeAddProjectionAlreadyExists exercises the cluster
// singleton branch in AddProjection where the second registration of the
// same projection name surfaces gerrors.ErrSingletonAlreadyExists, which
// the engine swallows (lines 363-369).
func TestEngineClusterModeAddProjectionAlreadyExists(t *testing.T) {
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

	pause.For(time.Second)
	require.True(t, sys.InCluster())

	engine, err := NewEngine(sys, cfg)
	require.NoError(t, err)
	require.NoError(t, engine.Start(ctx))
	t.Cleanup(func() { _ = engine.Stop(ctx) })

	const name = "discard-once"
	require.NoError(t, engine.AddProjection(ctx, name))
	pause.For(time.Second)

	// Re-registering must take the ErrSingletonAlreadyExists branch and
	// silently return nil rather than erroring.
	require.NoError(t, engine.AddProjection(ctx, name),
		"second AddProjection on the same name must be a clean no-op via ErrSingletonAlreadyExists")
}

// TestEngineProjectionLagWithEvents drives ProjectionLag through the path
// where a shard actually has events, exercising the latestTimestamp/offset
// arithmetic and the per-shard accumulation.
func TestEngineProjectionLagWithEvents(t *testing.T) {
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
	)
	require.NoError(t, engine.Start(ctx))

	// Produce at least one event so a shard becomes non-empty and the loop
	// falls through to the latestTimestamp/offset computation.
	entityID := uuid.NewString()
	require.NoError(t, engine.Entity(ctx, NewEventSourcedEntity(entityID)))
	_, _, err := engine.SendCommand(ctx, entityID, &testpb.CreateAccount{
		AccountBalance: 100,
	}, time.Minute)
	require.NoError(t, err)

	lags, err := engine.ProjectionLag(ctx, "any")
	require.NoError(t, err)
	require.NotEmpty(t, lags, "expected at least one shard to be reported")
	for shard, lag := range lags {
		assert.GreaterOrEqualf(t, int64(lag), int64(0), "lag for shard %d must not be negative", shard)
	}
}
