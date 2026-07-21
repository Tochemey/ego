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
	"strings"
	"sync"
	"time"

	goakt "github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/extension"
	"github.com/tochemey/goakt/v4/passivation"
	"github.com/tochemey/goakt/v4/supervisor"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/atomic"

	"github.com/tochemey/ego/v4/egopb"
	"github.com/tochemey/ego/v4/encryption"
	"github.com/tochemey/ego/v4/eventadapter"
	"github.com/tochemey/ego/v4/eventstream"
	"github.com/tochemey/ego/v4/internal/extensions"
	"github.com/tochemey/ego/v4/internal/syncmap"
	"github.com/tochemey/ego/v4/offsetstore"
	"github.com/tochemey/ego/v4/persistence"
)

var (
	// ErrEngineNotStarted is returned when the eGo engine has not started
	ErrEngineNotStarted = errors.New("eGo engine has not started")
	// ErrUndefinedEntityID is returned when sending a command to an undefined entity
	ErrUndefinedEntityID = errors.New("eGo entity id is not defined")
	// ErrCommandReplyUnmarshalling is returned when the unmarshalling command reply failed
	ErrCommandReplyUnmarshalling = errors.New("failed to parse command reply")
	// ErrDurableStateStoreRequired is returned when the eGo engine durable store is not set
	ErrDurableStateStoreRequired = errors.New("durable state store is required")
	// ErrProjectionNotRegistered is returned by StartProjection when the given
	// name was never registered on the engine's Config via WithProjection.
	ErrProjectionNotRegistered = errors.New("projection is not registered; register it with ego.WithProjection")
	// ErrActorSystemRequired is returned when NewEngine is called with a nil
	// actor system. The caller must construct and start the actor system
	// themselves before plugging eGo in.
	ErrActorSystemRequired = errors.New("actor system is required")
	// ErrActorSystemNotStarted is returned when NewEngine is given an actor
	// system whose Start has not yet been called or has not yet succeeded.
	ErrActorSystemNotStarted = errors.New("actor system must be started before NewEngine")
	// ErrMissingRequiredExtensions is returned when NewEngine validates the
	// actor system and finds that one or more extensions eGo needs are
	// absent. The error message lists the missing extension IDs. Callers
	// typically hit this when the actor system was built from a different
	// Config than the one passed to NewEngine, or when cfg.GoaktOptions()
	// was not applied at construction time.
	ErrMissingRequiredExtensions = errors.New("actor system is missing required ego extensions")
	// ZeroTime is the zero time
	ZeroTime = time.Time{}
)

// Done is a signal that an operation has completed
type Done struct{}

// actorSystemRef bundles the underlying goakt.ActorSystem with its NoSender PID
// so they can be published as a single value via atomic.Pointer. Bundling
// guarantees readers always see a consistent (sys, noSender) pair — they cannot
// observe a half-initialized engine where one is set and the other is not.
type actorSystemRef struct {
	sys      goakt.ActorSystem
	noSender *goakt.PID
}

type eventsStream struct {
	publisher  EventPublisher
	subscriber eventstream.Subscriber
	done       chan Done
}

type statesStream struct {
	publisher  StatePublisher
	subscriber eventstream.Subscriber
	done       chan Done
}

// Engine represents the engine that empowers the various entities
type Engine struct {
	name          string
	eventsStore   persistence.EventsStore
	stateStore    persistence.StateStore
	offsetStore   offsetstore.OffsetStore
	snapshotStore persistence.SnapshotStore
	actorSystem   atomic.Pointer[actorSystemRef]
	logger        Logger
	started       atomic.Bool
	eventStream   eventstream.Stream
	mutex         sync.RWMutex

	eventsStreams *syncmap.Map[string, *eventsStream]
	statesStreams *syncmap.Map[string, *statesStream]
	eventAdapters []eventadapter.EventAdapter
	telemetry     *Telemetry
	metrics       *metrics
	encryptor     encryption.Encryptor
}

// NewEngine plugs eGo into an already-constructed and started goakt.ActorSystem.
//
// The caller builds a single Config with NewConfig, passes cfg.GoaktOptions()
// to goakt.NewActorSystem, starts the actor system, and then hands the same
// Config to NewEngine. This guarantees the actor system's extensions and the
// engine's view of stores, projections, telemetry, and the in-process event
// stream stay in lockstep.
//
// NewEngine validates the actor system on entry:
//
//   - sys must be non-nil (otherwise returns ErrActorSystemRequired);
//   - sys.Running() must be true (otherwise returns ErrActorSystemNotStarted);
//   - every extension eGo needs based on cfg must be registered on sys
//     (otherwise returns ErrMissingRequiredExtensions with the missing IDs).
//
// NewEngine also registers eGo's internal spawn-configuration dependency
// types and any behavior kinds supplied via WithEntityKinds on the actor
// system, so that entity spawn requests routed to this node from cluster
// peers can be deserialized. Every node in a cluster must therefore build
// its engine with the same WithEntityKinds list.
//
// The engine does NOT take ownership of the actor system. Engine.Stop will
// not call sys.Stop; the caller stops the actor system on their own
// schedule (typically after Engine.Stop).
//
// Parameters:
//   - actorSys: A running goakt.ActorSystem with eGo's required extensions registered.
//   - config: The Config used to build the actor system's eGo extensions.
//
// Returns:
//   - A pointer to the newly created Engine instance, or an error.
func NewEngine(actorSys goakt.ActorSystem, config *Config) (*Engine, error) {
	if actorSys == nil {
		return nil, ErrActorSystemRequired
	}

	if config == nil {
		return nil, fmt.Errorf("%w: nil Config", ErrMissingRequiredExtensions)
	}

	if !actorSys.Running() {
		return nil, ErrActorSystemNotStarted
	}

	if err := validateActorSystemExtensions(actorSys, config); err != nil {
		return nil, err
	}

	// Register dependency types on this node so spawn requests placed here by
	// peers (SpawnOn placement, relocation) can be deserialized even before
	// this node has spawned such an entity itself. The internal spawn-config
	// types live in internal/extensions and cannot be registered by
	// application code; user behavior kinds come from WithEntityKinds.
	dependencies := append(
		[]extension.Dependency{new(extensions.EntityConfig), new(extensions.SagaConfig)},
		config.entityKinds...,
	)
	if err := actorSys.Inject(dependencies...); err != nil {
		return nil, err
	}

	e := &Engine{
		name:          actorSys.Name(),
		eventsStore:   config.eventsStore,
		stateStore:    config.stateStore,
		offsetStore:   config.offsetStore,
		snapshotStore: config.snapshotStore,
		logger:        config.logger,
		eventStream:   config.eventStream,
		eventAdapters: config.eventAdapters,
		telemetry:     config.telemetry,
		encryptor:     config.encryptor,
		eventsStreams: syncmap.New[string, *eventsStream](),
		statesStreams: syncmap.New[string, *statesStream](),
	}
	e.actorSystem.Store(&actorSystemRef{
		sys:      actorSys,
		noSender: actorSys.NoSender(),
	})

	return e, nil
}

// validateActorSystemExtensions asserts that every extension eGo needs given
// the engine's configuration is registered on the actor system. Missing
// extensions are collected and reported together so the caller learns about
// the full set of problems at once.
func validateActorSystemExtensions(sys goakt.ActorSystem, cfg *Config) error {
	type check struct {
		id       string
		required bool
	}
	checks := []check{
		{extensions.EventsStoreExtensionID, true},
		{extensions.EventsStreamExtensionID, true},
		{extensions.DurableStateStoreExtensionID, cfg.stateStore != nil},
		{extensions.OffsetStoreExtensionID, cfg.offsetStore != nil},
		{extensions.ProjectionExtensionID, len(cfg.projections) > 0},
		{extensions.SnapshotStoreExtensionID, cfg.snapshotStore != nil},
		{extensions.EventAdaptersExtensionID, len(cfg.eventAdapters) > 0},
		{extensions.TelemetryExtensionID, cfg.telemetry != nil},
		{extensions.EncryptorExtensionID, cfg.encryptor != nil},
	}

	var missing []string
	for _, c := range checks {
		if c.required && sys.Extension(c.id) == nil {
			missing = append(missing, c.id)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("%w: %s (hint: build the actor system with cfg.GoaktOptions() and pass the same cfg to NewEngine)",
			ErrMissingRequiredExtensions, strings.Join(missing, ", "))
	}
	return nil
}

// Start initializes the eGo engine on top of an actor system that is already
// running.
//
// In the meta-framework design, Start does no actor-system construction —
// the caller has already built and started goakt.NewActorSystem. Start only
// wires the OpenTelemetry propagator (when WithTelemetry is configured) and
// flips the engine into a "ready to receive entity work" state.
//
// Parameters:
//   - ctx: Execution context. Currently unused but retained for API symmetry
//     with Stop and to leave room for future async initialization.
//
// Returns:
//   - An error if the engine is in an inconsistent state; otherwise, nil.
func (engine *Engine) Start(_ context.Context) error {
	if engine.actorSystem.Load() == nil {
		return ErrActorSystemRequired
	}

	if engine.telemetry != nil {
		engine.metrics = newMetrics(engine.telemetry.Meter)
		// Set the global OTel text map propagator so that trace context is
		// propagated across process boundaries (HTTP headers, gRPC metadata,
		// goakt remote calls). This is required for end-to-end distributed
		// tracing — without it, each service/node starts a new root trace.
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		))
	}
	engine.started.Store(true)
	return nil
}

// Stop gracefully shuts down the eGo engine.
//
// Stop terminates all running publishers, closes the local event stream
// adapter, and detaches the engine's reference to the actor system so
// concurrent callers fail fast with ErrEngineNotStarted. The actor system
// itself is NOT stopped — its lifecycle belongs to the caller, who built it
// and is expected to call sys.Stop(ctx) on their own schedule (typically
// after Engine.Stop returns).
//
// Parameters:
//   - ctx: Execution context for managing cancellation and timeouts during
//     publisher shutdown.
//
// Returns:
//   - An error if a publisher fails to close; otherwise, nil.
func (engine *Engine) Stop(ctx context.Context) error {
	if !engine.Started() {
		return nil
	}

	engine.started.Store(false)

	// Shutdown all event publishers
	for _, stream := range engine.eventsStreams.Values() {
		stream.done <- Done{}
		stream.subscriber.Shutdown()
		if err := stream.publisher.Close(ctx); err != nil {
			return err
		}
	}

	for _, stream := range engine.statesStreams.Values() {
		stream.done <- Done{}
		stream.subscriber.Shutdown()
		if err := stream.publisher.Close(ctx); err != nil {
			return err
		}
	}

	if engine.eventStream != nil {
		engine.eventStream.Close()
	}
	engine.eventsStreams.Reset()
	engine.statesStreams.Reset()

	// Detach our reference atomically so callers racing with shutdown fail
	// fast. We deliberately do NOT call sys.Stop — the actor system belongs
	// to the caller.
	engine.actorSystem.Store(nil)
	return nil
}

// Started returns true when the eGo engine has started
func (engine *Engine) Started() bool {
	return engine.started.Load()
}

// ActorSystem returns the underlying goakt.ActorSystem used by the engine.
//
// Returns nil if the engine has not been started or has already been stopped.
// The returned reference is a snapshot; callers should not retain it across
// engine restarts.
func (engine *Engine) ActorSystem() goakt.ActorSystem {
	ref := engine.actorSystem.Load()
	if ref == nil {
		return nil
	}
	return ref.sys
}

// StartProjection starts the named projection previously registered on the
// engine's Config via WithProjection.
//
// The projection processes events from the events store applying its own
// registered handler to manage state updates based on incoming events. The
// provided offset store ensures the projection maintains its processing
// position across restarts.
//
// Key behavior:
//   - Projections once created, will persist for the entire lifespan of the running eGo system.
//   - In cluster mode, the projection runs as a singleton on the oldest node. If that node
//     leaves the cluster, the singleton is automatically restarted on the new oldest node.
//   - In standalone (non-cluster) mode, the projection runs as a regular long-lived actor.
//   - A failed store round trip never stops the projection: the runner retries the pull with
//     exponential backoff until the store recovers and then resumes from committed offsets.
//     An event that cannot be processed (a handler error under the Fail or RetryAndFail
//     recovery policy, a failed decryption or event adaptation) stops the projection through
//     supervision, so the failure is visible through IsProjectionRunning.
//
// Parameters:
//   - ctx: Execution context used for cancellation and deadlines.
//   - name: The unique identifier the projection was registered under via WithProjection.
//
// Returns ErrProjectionNotRegistered when the name is unknown, or an error if
// the projection fails to start due to misconfiguration or underlying system issues.
func (engine *Engine) StartProjection(ctx context.Context, name string) error {
	if !engine.Started() {
		return ErrEngineNotStarted
	}

	actor := NewProjectionActor()

	ref := engine.actorSystem.Load()
	if ref == nil {
		return ErrEngineNotStarted
	}
	actorSystem := ref.sys

	// Fail fast on unknown names instead of letting the actor's PreStart
	// (and its retries) discover the missing registration. When the registry
	// extension cannot be resolved (e.g. the actor system is already stopped),
	// fall through and let the spawn surface the actual failure.
	if registry, ok := actorSystem.Extension(extensions.ProjectionExtensionID).(*extensions.ProjectionExtension); ok && registry.Get(name) == nil {
		return fmt.Errorf("%w: %s", ErrProjectionNotRegistered, name)
	}

	if actorSystem.InCluster() {
		// In cluster mode, run the projection as a singleton to avoid
		// duplicate event processing across nodes. The singleton is keyed by
		// the projection name and placed on the oldest node. SpawnSingleton
		// is idempotent when the name is already bound to this singleton, so
		// concurrent StartProjection calls across nodes all succeed; any
		// error it returns is a genuine failure worth surfacing. The
		// projection supervisor travels with the singleton wherever it is
		// placed or relocated.
		if _, err := actorSystem.SpawnSingleton(ctx, name, actor,
			goakt.WithSingletonSupervisor(newProjectionSupervisor())); err != nil {
			return fmt.Errorf("failed to start the projection=(%s): %w", name, err)
		}
		return nil
	}

	// In standalone mode, run as a regular long-lived actor.
	if _, err := actorSystem.Spawn(ctx, name,
		actor,
		goakt.WithLongLived(),
		goakt.WithRelocationDisabled(),
		goakt.WithSupervisor(newProjectionSupervisor())); err != nil {
		return fmt.Errorf("failed to start the projection=(%s): %w", name, err)
	}

	return nil
}

// StopProjection stops and removes the specified projection from the engine.
//
// This function gracefully shuts down the projection identified by `name` and removes it from the system.
// Any in-progress processing will be stopped, and the projection will no longer receive events.
//
// Parameters:
//   - ctx: Execution context for managing cancellation and timeouts.
//   - name: The unique identifier of the projection to be removed.
//
// Returns:
//   - An error if the projection fails to stop or does not exist; otherwise, nil.
func (engine *Engine) StopProjection(ctx context.Context, name string) error {
	if !engine.Started() {
		return ErrEngineNotStarted
	}

	ref := engine.actorSystem.Load()
	if ref == nil {
		return ErrEngineNotStarted
	}
	return ref.sys.Kill(ctx, name)
}

// IsProjectionRunning checks whether the specified projection is currently active and running.
//
// This function returns `true` if the projection identified by `name` is running. However, callers should
// always check the returned error to ensure the result is valid, as an error may indicate an inability to
// determine the projection's status.
//
// Parameters:
//   - ctx: Execution context for managing timeouts and cancellations.
//   - name: The unique identifier of the projection.
//
// Returns:
//   - A boolean indicating whether the projection is running (`true` if active, `false` otherwise).
//   - An error if the status check fails, which may result in a false negative.
func (engine *Engine) IsProjectionRunning(ctx context.Context, name string) (bool, error) {
	if !engine.Started() {
		return false, ErrEngineNotStarted
	}

	ref := engine.actorSystem.Load()
	if ref == nil {
		return false, ErrEngineNotStarted
	}

	pid, err := ref.sys.ActorOf(ctx, name)
	if err != nil {
		return false, fmt.Errorf("failed to get projection %s: %w", name, err)
	}

	if pid != nil {
		return pid.IsRunning(), nil
	}

	return false, nil
}

// RebuildProjection stops the named projection, resets its offset to the
// given timestamp, and restarts it. This causes the projection to
// reprocess all events from that point forward.
//
// Passing ego.ZeroTime replays from the beginning of time.
//
// Parameters:
//   - ctx: Execution context for managing cancellation and timeouts.
//   - name: The unique identifier of the projection to rebuild.
//   - from: The timestamp from which to start reprocessing events.
//
// Returns:
//   - An error if the rebuild process encounters issues; otherwise, nil.
func (engine *Engine) RebuildProjection(ctx context.Context, name string, from time.Time) error {
	if !engine.Started() {
		return ErrEngineNotStarted
	}

	engine.mutex.RLock()
	offsetStore := engine.offsetStore
	engine.mutex.RUnlock()

	if offsetStore == nil {
		return fmt.Errorf("offset store is required to rebuild projection")
	}

	// stop the running projection
	if err := engine.StopProjection(ctx, name); err != nil {
		return fmt.Errorf("failed to stop projection %s for rebuild: %w", name, err)
	}

	// reset the offset
	if err := offsetStore.ResetOffset(ctx, name, from.UnixMilli()); err != nil {
		return fmt.Errorf("failed to reset offset for projection %s: %w", name, err)
	}

	// restart the projection
	if err := engine.StartProjection(ctx, name); err != nil {
		return fmt.Errorf("failed to restart projection %s after rebuild: %w", name, err)
	}

	return nil
}

// Subscribe creates an events' subscriber.
//
// This function initializes a new subscriber for the event stream managed by the eGo engine. The subscriber
// will receive events from the topics specified by the engine's configuration.
//
// Returns:
//   - An eventstream.Subscriber instance that can be used to receive events.
//   - An error if the engine has not started or if there is an issue creating the subscriber.
func (engine *Engine) Subscribe() (eventstream.Subscriber, error) {
	if !engine.Started() {
		return nil, ErrEngineNotStarted
	}

	engine.mutex.RLock()
	eventStream := engine.eventStream
	engine.mutex.RUnlock()

	subscriber := eventStream.AddSubscriber()
	eventStream.Subscribe(subscriber, eventsTopic)
	eventStream.Subscribe(subscriber, statesTopic)

	return subscriber, nil
}

// Entity creates an event-sourced entity that persists its state by storing a history of events.
//
// This entity follows the event sourcing pattern, where changes to its state are driven by events rather than direct mutations.
// It processes incoming commands, validates them, generates corresponding events upon successful validation, and persists those
// events before applying them to update its state.
//
// Key Behavior:
//   - Commands: The entity receives commands (non-persistent messages) that are validated before being processed.
//   - Validation: This can range from simple field checks to interactions with external services.
//   - Events: If validation succeeds, events are derived from the command, persisted in the event store, and then used to update the entity’s state.
//   - Recovery: During recovery, only persisted events are replayed to rebuild the entity’s state, ensuring deterministic behavior.
//   - Command vs. Event: Commands may be rejected if they are invalid, while events—once persisted—cannot fail during replay.
//
// If no new events are generated, the entity simply returns its current state.
// To interact with the entity, use SendCommand to send commands to a durable event-sourced entity.
//
// Parameters:
//   - ctx: Execution context for controlling the lifecycle of the entity.
//   - behavior: Defines the entity’s event-sourced behavior, including command handling and state transitions.
//   - opts: Additional spawning options to configure entity behavior.
//
// Returns an error if the entity fails to initialize or encounters an issue during execution.
func (engine *Engine) Entity(ctx context.Context, behavior EventSourcedBehavior, opts ...SpawnOption) error {
	if !engine.Started() {
		return ErrEngineNotStarted
	}

	ref := engine.actorSystem.Load()
	if ref == nil {
		return ErrEngineNotStarted
	}
	actorSystem := ref.sys

	// Register the behavior type on the local node as a fallback for kinds
	// missing from WithEntityKinds. This only covers spawns placed locally:
	// in cluster mode the spawn may land on a peer, which can only
	// deserialize the behavior if it was registered there via WithEntityKinds.
	// Inject only errors when the actor system is not started, which the
	// Started check above already rules out.
	_ = actorSystem.Inject(behavior)

	config := newSpawnConfig(opts...)
	sOptions := buildSpawnOptionsFromConfig(config)

	entityConfig := extensions.NewEntityConfig(config.snapshotInterval)
	if config.retentionPolicy != nil {
		entityConfig.HasRetentionPolicy = true
		entityConfig.DeleteEventsOnSnapshot = config.retentionPolicy.DeleteEventsOnSnapshot
		entityConfig.DeleteSnapshotsOnSnapshot = config.retentionPolicy.DeleteSnapshotsOnSnapshot
		entityConfig.EventsRetentionCount = config.retentionPolicy.EventsRetentionCount
	}

	entityConfig.BatchThreshold = config.batchThreshold
	entityConfig.BatchFlushWindow = config.batchFlushWindow
	_ = actorSystem.Inject(entityConfig)
	sOptions = append(sOptions, goakt.WithDependencies(behavior, entityConfig))

	_, err := actorSystem.SpawnOn(ctx, behavior.ID(), newEventSourcedActor(), sOptions...)
	return err
}

// EntityExists reports whether an entity with the given ID is currently alive in the cluster.
//
// The lookup works for both event-sourced and durable state entities, and is purely
// a liveness probe: it does not spawn the entity, recover its state, or replay its
// journal. As a result, an entity that has been persisted but is not currently
// hydrated (for example, one that has been passivated or has not yet received its
// first command after process restart) will report as non-existent.
//
// The method returns:
//   - (true, nil)  if the entity is registered and its underlying actor is running.
//   - (false, nil) if no actor for the given ID is found, or if the actor exists
//     but is no longer running (e.g. stopping or stopped).
//   - (false, ErrEngineNotStarted) if the engine has not been started.
//   - (false, error) if the underlying actor system lookup fails for any other reason.
//
// EntityExists is safe to call concurrently and is intended for callers that need
// to branch on entity presence without incurring the cost or side effects of
// materializing the entity.
func (engine *Engine) EntityExists(ctx context.Context, entityID string) (bool, error) {
	if !engine.Started() {
		return false, ErrEngineNotStarted
	}

	ref := engine.actorSystem.Load()
	if ref == nil {
		return false, ErrEngineNotStarted
	}
	exists, err := ref.sys.ActorExists(ctx, entityID)
	if err != nil {
		return false, fmt.Errorf("failed to check existence of entity %s: %w", entityID, err)
	}
	return exists, nil
}

// DurableStateEntity creates an entity that persists its full state in a durable store without maintaining historical event records.
//
// Unlike an event-sourced entity, a durable state entity does not track past state changes as a sequence of events. Instead, it
// directly stores and updates its current state in a durable store.
//
// Key Behavior:
//   - Commands: The entity receives non-persistent commands that are validated before being processed.
//   - Validation: This can range from simple field checks to complex interactions with external services.
//   - State Updates: If validation succeeds, a new state is derived from the command, persisted, and then applied to update the entity’s state.
//   - Persistence: The latest state is always stored, ensuring that only the most recent version is retained.
//   - Recovery: Upon restart, the entity reloads its last persisted state, rather than replaying a sequence of past events.
//   - Shutdown Handling: During a normal shutdown, the entity ensures that its current state is persisted before termination.
//
// To interact with the entity, use SendCommand to send commands and update the durable state.
//
// Parameters:
//   - ctx: Execution context for controlling the entity’s lifecycle.
//   - behavior: Defines the entity’s behavior, including command handling and state transitions.
//   - opts: Additional spawning options to configure entity behavior.
//
// Returns an error if the entity fails to initialize or encounters an issue during execution.
func (engine *Engine) DurableStateEntity(ctx context.Context, behavior DurableStateBehavior, opts ...SpawnOption) error {
	if !engine.Started() {
		return ErrEngineNotStarted
	}

	ref := engine.actorSystem.Load()
	if ref == nil {
		return ErrEngineNotStarted
	}
	actorSystem := ref.sys

	engine.mutex.RLock()
	durableStateStore := engine.stateStore
	engine.mutex.RUnlock()

	if durableStateStore == nil {
		return ErrDurableStateStoreRequired
	}

	// Register the behavior type on the local node as a fallback for kinds
	// missing from WithEntityKinds. This only covers spawns placed locally:
	// in cluster mode the spawn may land on a peer, which can only
	// deserialize the behavior if it was registered there via WithEntityKinds.
	// Inject only errors when the actor system is not started, which the
	// Started check above already rules out.
	_ = actorSystem.Inject(behavior)

	sOptions := buildSpawnOptions(opts...)
	sOptions = append(sOptions, goakt.WithDependencies(behavior))

	_, err := actorSystem.SpawnOn(ctx, behavior.ID(), newDurableStateActor(), sOptions...)
	return err
}

// SendCommand sends a command to the specified entity and processes its response.
//
// This function dispatches a command to an entity identified by `entityID`. The entity validates the command, applies
// any necessary state changes, and persists the resulting state if applicable. The function returns the updated state,
// a revision number, or an error if the operation fails.
//
// Behavior:
//   - If the command is successfully processed and results in a state update, the new state and its revision number are returned.
//   - If no state update occurs (i.e., no event is persisted or the command does not trigger a change), `nil` is returned.
//   - If an error occurs during processing, the function returns a non-nil error.
//
// Parameters:
//   - ctx: Execution context for handling timeouts and cancellations.
//   - entityID: The unique identifier of the target entity.
//   - cmd: The command to be processed by the entity.
//   - timeout: The duration within which the command must be processed before timing out.
//
// Returns:
//   - resultingState: The updated state of the entity after handling the command, or `nil` if no state change occurred.
//   - revision: A monotonically increasing revision number representing the persisted state version.
//   - err: An error if the command processing fails.
//
// nolint
func (engine *Engine) SendCommand(ctx context.Context, entityID string, cmd Command, timeout time.Duration) (resultingState State, revision uint64, err error) {
	if !engine.Started() {
		return nil, 0, ErrEngineNotStarted
	}

	// entityID is not defined
	if entityID == "" {
		return nil, 0, ErrUndefinedEntityID
	}

	// Create a trace span that connects the caller's context (e.g. an HTTP
	// request span) to the command dispatch, providing end-to-end visibility.
	if engine.telemetry != nil && engine.telemetry.Tracer != nil {
		var span trace.Span
		ctx, span = engine.telemetry.Tracer.Start(ctx, "ego.send_command",
			trace.WithAttributes(
				attribute.String("ego.entity_id", entityID),
				attribute.String("ego.command_type", string(cmd.ProtoReflect().Descriptor().FullName())),
			))
		defer func() {
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
			}
			span.End()
		}()
	}

	ref := engine.actorSystem.Load()
	if ref == nil {
		return nil, 0, ErrEngineNotStarted
	}

	reply, err := ref.noSender.SendSync(ctx, entityID, cmd, timeout)
	if err != nil {
		return nil, 0, err
	}

	// cast the reply as it supposes
	commandReply, ok := reply.(*egopb.CommandReply)
	if ok {
		return parseCommandReply(commandReply)
	}
	return nil, 0, ErrCommandReplyUnmarshalling
}

// AddEventPublishers registers one or more event publishers with the eGo engine.
// This function subscribes the publishers to the event stream, allowing them to receive events.
//
// Note: Event publishers are responsible for publishing events to external systems. They need to be added to the engine before processing any events.
//
// Parameters:
//   - publishers: A list of event publishers to be added to the engine.
//
// Returns an error if the engine has not started.
func (engine *Engine) AddEventPublishers(publishers ...EventPublisher) error {
	if !engine.Started() {
		return ErrEngineNotStarted
	}

	engine.mutex.Lock()
	defer engine.mutex.Unlock()

	for _, publisher := range publishers {
		subscriber := engine.eventStream.AddSubscriber()
		engine.logger.Debug(fmt.Sprintf("%s subscribing to topic: %s", publisher.ID(), eventsTopic))
		engine.eventStream.Subscribe(subscriber, eventsTopic)

		// create an instance of the event subscriber
		eventSubscriber := &eventsStream{
			publisher:  publisher,
			subscriber: subscriber,
			done:       make(chan Done, 1),
		}

		// add the event publisher to the engine
		engine.eventsStreams.Set(publisher.ID(), eventSubscriber)

		// start the event publisher
		engine.logger.Info(fmt.Sprintf("starting %s events publisher....", publisher.ID()))
		go engine.sendEvent(eventSubscriber)
	}

	return nil
}

// AddStatePublishers registers one or more state publishers with the eGo engine.
// This function subscribes the publishers to the event stream, allowing them to receive state changes.
//
// Note: State publishers are responsible for publishing durable state changes to external systems.
// They need to be added to the engine before processing any durable state.
//
// Parameters:
//   - publishers: A list of state publishers to be added to the engine.
//
// Returns an error if the engine has not started.
func (engine *Engine) AddStatePublishers(publishers ...StatePublisher) error {
	if !engine.Started() {
		return ErrEngineNotStarted
	}

	engine.mutex.Lock()
	defer engine.mutex.Unlock()

	for _, publisher := range publishers {
		subscriber := engine.eventStream.AddSubscriber()
		engine.logger.Debug(fmt.Sprintf("%s subscribing to topic: %s", publisher.ID(), statesTopic))
		engine.eventStream.Subscribe(subscriber, statesTopic)

		// create an instance of the state subscriber
		stateSubscriber := &statesStream{
			publisher:  publisher,
			subscriber: subscriber,
			done:       make(chan Done, 1),
		}

		// add the state publisher to the engine
		engine.statesStreams.Set(publisher.ID(), stateSubscriber)

		// start the state publisher
		engine.logger.Info(fmt.Sprintf("starting %s durable state publisher....", publisher.ID()))
		go engine.sendState(stateSubscriber)
	}

	return nil
}

// Saga creates a saga/process manager that coordinates multiple entities.
//
// A saga is a long-running business process that reacts to events from the event
// stream, sends commands to entities, persists its own events for recovery, and
// supports compensation logic for rollback on failures.
//
// Parameters:
//   - ctx: Execution context for controlling the saga lifecycle.
//   - behavior: Defines the saga's logic including event handling, command dispatch, and compensation.
//   - timeout: Maximum duration for the saga. Zero means no timeout.
//
// Returns an error if the saga fails to initialize.
func (engine *Engine) Saga(ctx context.Context, behavior SagaBehavior, timeout time.Duration) error {
	if !engine.Started() {
		return ErrEngineNotStarted
	}

	ref := engine.actorSystem.Load()
	if ref == nil {
		return ErrEngineNotStarted
	}
	actorSystem := ref.sys

	// Register the behavior type on the local node as a fallback for kinds
	// missing from WithEntityKinds; relocation to a peer requires the type to
	// be registered there via WithEntityKinds. Inject only errors when the
	// actor system is not started, which the Started check above rules out.
	_ = actorSystem.Inject(behavior)

	sagaCfg := extensions.NewSagaConfig(timeout)
	_ = actorSystem.Inject(sagaCfg)
	actor := newSagaActor()

	_, err := actorSystem.Spawn(ctx, behavior.ID(),
		actor,
		goakt.WithLongLived(),
		goakt.WithDependencies(behavior, sagaCfg),
		goakt.WithSupervisor(newSupervisor(RestartDirective)))
	if err != nil {
		return fmt.Errorf("failed to start saga %s: %w", behavior.ID(), err)
	}

	return nil
}

// SagaStatus returns the current status and state of the named saga.
//
// Parameters:
//   - ctx: Execution context for managing timeouts and cancellations.
//   - sagaID: The unique identifier of the saga.
//   - timeout: The duration within which the query must be processed.
//
// Returns:
//   - SagaInfo containing the saga's current status and state.
//   - An error if the saga is not found or the query fails.
func (engine *Engine) SagaStatus(ctx context.Context, sagaID string, timeout time.Duration) (*SagaInfo, error) {
	if !engine.Started() {
		return nil, ErrEngineNotStarted
	}

	if sagaID == "" {
		return nil, ErrUndefinedEntityID
	}

	ref := engine.actorSystem.Load()
	if ref == nil {
		return nil, ErrEngineNotStarted
	}

	reply, err := ref.noSender.SendSync(ctx, sagaID, new(egopb.GetStateCommand), timeout)
	if err != nil {
		return nil, fmt.Errorf("failed to get saga status for %s: %w", sagaID, err)
	}

	commandReply, ok := reply.(*egopb.CommandReply)
	if !ok {
		return nil, fmt.Errorf("unexpected reply type from saga %s", sagaID)
	}

	state, _, err := parseCommandReply(commandReply)
	if err != nil {
		return nil, err
	}

	return &SagaInfo{
		ID:    sagaID,
		State: state,
	}, nil
}

// EraseEntity performs GDPR erasure for the given persistence ID.
// When an encryptor backed by a KeyStore is configured, this deletes the encryption
// key (crypto-shredding), making all encrypted events and snapshots irrecoverable.
// Optionally, it also physically deletes events and snapshots from the stores.
func (engine *Engine) EraseEntity(ctx context.Context, persistenceID string, full bool) error {
	if !engine.Started() {
		return ErrEngineNotStarted
	}

	engine.mutex.RLock()
	eventsStore := engine.eventsStore
	snapshotStore := engine.snapshotStore
	engine.mutex.RUnlock()

	if full {
		// Get the latest event to find the max sequence number
		latestEvent, err := eventsStore.GetLatestEvent(ctx, persistenceID)
		if err != nil {
			return fmt.Errorf("failed to get latest event for erasure: %w", err)
		}
		if latestEvent != nil {
			if err := eventsStore.DeleteEvents(ctx, persistenceID, latestEvent.GetSequenceNumber()); err != nil {
				return fmt.Errorf("failed to delete events for erasure: %w", err)
			}
		}
		if snapshotStore != nil && latestEvent != nil {
			if err := snapshotStore.DeleteSnapshots(ctx, persistenceID, latestEvent.GetSequenceNumber()); err != nil {
				return fmt.Errorf("failed to delete snapshots for erasure: %w", err)
			}
		}
	}

	return nil
}

// ProjectionLag reports, per shard, how far behind the named projection is
// relative to the newest event persisted in that shard.
//
// The returned map is keyed by shard number. For each shard the value is the
// time delta between the timestamp of the most recent event in the journal and
// the offset the projection has committed to the offset store. A value of 0
// means the projection is fully caught up for that shard (either because the
// shard is empty or because the committed offset already covers the latest
// event).
//
// Implementation overview:
//  1. Enumerate every shard known to the events store.
//  2. For each shard, read the projection's committed offset from the offset
//     store. In ego, this offset is stored as the timestamp (in milliseconds)
//     of the last event processed by the projection for that shard.
//  3. Determine the timestamp of the newest event currently persisted in the
//     shard.
//  4. lag = latestEventTimestamp - committedOffset (clamped at 0 so that a
//     projection running ahead of the probe window is not reported as negative).
//
// The engine must be started and an offset store must be configured; otherwise
// an error is returned.
func (engine *Engine) ProjectionLag(ctx context.Context, projectionName string) (map[uint64]time.Duration, error) {
	if !engine.Started() {
		return nil, ErrEngineNotStarted
	}

	// Snapshot the store references under the mutex so we do not race with a
	// concurrent engine shutdown or reconfiguration while iterating shards.
	engine.mutex.RLock()
	offsetStore := engine.offsetStore
	eventsStore := engine.eventsStore
	engine.mutex.RUnlock()

	// Lag is defined relative to the projection's committed offset, so an
	// offset store is mandatory. Without it there is no meaningful answer.
	if offsetStore == nil {
		return nil, fmt.Errorf("offset store is required to compute projection lag")
	}

	// Projections are sharded: each shard is advanced independently by its own
	// runner, so lag is always reported per shard rather than as a single
	// aggregate number. ShardOffsets answers "which shards exist" and "what is
	// the newest event timestamp per shard" in one round trip, so the only
	// per-shard work left is reading the projection's committed offset.
	shardOffsets, err := eventsStore.ShardOffsets(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch shard offsets: %w", err)
	}

	lags := make(map[uint64]time.Duration, len(shardOffsets))
	for shard, latestTimestamp := range shardOffsets {
		projectionID := &egopb.ProjectionId{
			ProjectionName: projectionName,
			ShardNumber:    shard,
		}

		// The committed offset is the "watermark" for this projection on this
		// shard: the timestamp of the last event the projection handler
		// successfully processed. A brand-new projection returns a zero-value
		// offset, which will naturally yield a lag equal to the age of the
		// oldest-to-newest span of events.
		offset, err := offsetStore.GetCurrentOffset(ctx, projectionID)
		if err != nil {
			return nil, fmt.Errorf("failed to get offset for shard %d: %w", shard, err)
		}

		// Both values below are unix **nanoseconds**:
		//   - the ShardOffsets values are event timestamps written by the
		//     entity actors using time.Now().UnixNano() (see
		//     event_sourced_actor.go / durable_state_actor.go /
		//     saga_actor.go). Nanosecond resolution gives projection offsets
		//     a tiebreaker fine enough to make co-timestamp collisions across
		//     two polls astronomically improbable.
		//   - Offset.Value is a copy of a prior event's timestamp, propagated
		//     from EventsStore.GetShardEvents' nextOffset return value through
		//     projectionRunner.commitOffset.
		// Their subtraction therefore yields the lag in nanoseconds, which is
		// the native representation of time.Duration.
		// (Note: Offset.Timestamp is a separate wall-clock audit field written
		// in milliseconds; it is deliberately not used here.)
		//
		// Clamp negative values: the projection runner may have advanced its
		// offset between the two store reads above, which would otherwise
		// surface as a spurious negative lag.
		lagNanos := latestTimestamp - offset.GetValue()
		if lagNanos < 0 {
			lagNanos = 0
		}
		lags[shard] = time.Duration(lagNanos)
	}

	return lags, nil
}

// parseCommandReply parses the command reply
func parseCommandReply(reply *egopb.CommandReply) (State, uint64, error) {
	var (
		state State
		err   error
	)

	switch r := reply.GetReply().(type) {
	case *egopb.CommandReply_StateReply:
		msg, err := r.StateReply.GetState().UnmarshalNew()
		if err != nil {
			return state, 0, err
		}

		switch v := msg.(type) {
		case State:
			return v, r.StateReply.GetSequenceNumber(), nil
		default:
			return state, 0, fmt.Errorf("got %s", r.StateReply.GetState().GetTypeUrl())
		}
	case *egopb.CommandReply_ErrorReply:
		err = errors.New(r.ErrorReply.GetMessage())
		return state, 0, err
	}
	return state, 0, errors.New("no state received")
}

func buildSpawnOptions(opts ...SpawnOption) []goakt.SpawnOption {
	return buildSpawnOptionsFromConfig(newSpawnConfig(opts...))
}

func buildSpawnOptionsFromConfig(config *spawnConfig) []goakt.SpawnOption {
	sOptions := []goakt.SpawnOption{
		goakt.WithLongLived(),
	}

	if config.passivateAfter > 0 {
		sOptions = append(sOptions, goakt.WithPassivationStrategy(passivation.NewTimeBasedStrategy(config.passivateAfter)))
	}

	if !config.toRelocate {
		sOptions = append(sOptions, goakt.WithRelocationDisabled())
	}

	sOptions = append(sOptions,
		goakt.WithPlacement(toSpawnPlacement(config.entitiesPlacement)),
		goakt.WithSupervisor(newSupervisor(config.supervisorDirective)),
	)

	if config.batchThreshold > 0 {
		sOptions = append(sOptions, goakt.WithStashing())
	}

	return sOptions
}

func newSupervisor(directive SupervisorDirective) *supervisor.Supervisor {
	return supervisor.NewSupervisor(supervisor.WithAnyErrorDirective(toSupervisorDirective(directive)))
}

func toSpawnPlacement(placement EntitiesPlacement) goakt.SpawnPlacement {
	switch placement {
	case LeastLoad:
		return goakt.LeastLoad
	case Random:
		return goakt.Random
	case Local:
		return goakt.Local
	default:
		return goakt.RoundRobin
	}
}

func toSupervisorDirective(directive SupervisorDirective) supervisor.Directive {
	switch directive {
	case StopDirective:
		return supervisor.StopDirective
	default:
		return supervisor.RestartDirective
	}
}

// sendEvent sends events to the event publisher.
// It blocks on the subscriber's Ready signal when idle, then drains the
// snapshot returned by Iterator. Selecting on Iterator directly would
// busy-spin a CPU core: it returns a closed snapshot channel that yields
// nil immediately whenever the queue is empty.
func (engine *Engine) sendEvent(stream *eventsStream) {
	for {
		select {
		case <-stream.done:
			return
		case <-stream.subscriber.Ready():
		}

		for message := range stream.subscriber.Iterator() {
			select {
			case <-stream.done:
				return
			default:
			}

			if message == nil {
				continue
			}

			event, ok := message.Payload().(*egopb.Event)
			if !ok {
				continue
			}

			if err := stream.publisher.Publish(context.Background(), event); err != nil {
				engine.logger.Error(fmt.Sprintf("(%s) failed to publish event=[persistenceID=%s, sequenceNumber=%d]: %s",
					stream.publisher.ID(),
					event.GetPersistenceId(),
					event.GetSequenceNumber(),
					err.Error()))
				continue
			}

			engine.logger.Info(fmt.Sprintf("(%s) successfully published event=[persistenceID=%s, sequenceNumber=%d]",
				stream.publisher.ID(),
				event.GetPersistenceId(),
				event.GetSequenceNumber()))
		}
	}
}

// sendState sends state changes to the state publisher.
// It blocks on the subscriber's Ready signal when idle, then drains the
// snapshot returned by Iterator. Selecting on Iterator directly would
// busy-spin a CPU core: it returns a closed snapshot channel that yields
// nil immediately whenever the queue is empty.
func (engine *Engine) sendState(stream *statesStream) {
	for {
		select {
		case <-stream.done:
			return
		case <-stream.subscriber.Ready():
		}

		for message := range stream.subscriber.Iterator() {
			select {
			case <-stream.done:
				return
			default:
			}

			if message == nil {
				continue
			}

			msg, ok := message.Payload().(*egopb.DurableState)
			if !ok {
				continue
			}

			publisher := stream.publisher
			if err := publisher.Publish(context.Background(), msg); err != nil {
				engine.logger.Error(fmt.Sprintf("(%s) failed to publish state=[persistenceID=%s, version=%d]: %s",
					publisher.ID(),
					msg.GetPersistenceId(),
					msg.GetVersionNumber(),
					err.Error()))
				continue
			}

			engine.logger.Info(fmt.Sprintf("(%s) successfully published state=[persistenceID=%s, version=%d]",
				publisher.ID(),
				msg.GetPersistenceId(),
				msg.GetVersionNumber()))
		}
	}
}
