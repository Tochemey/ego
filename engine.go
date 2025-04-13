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

package ego

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	goakt "github.com/tochemey/goakt/v3/actor"
	"github.com/tochemey/goakt/v3/address"
	"github.com/tochemey/goakt/v3/discovery"
	"github.com/tochemey/goakt/v3/log"
	"github.com/tochemey/goakt/v3/remote"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"

	"github.com/tochemey/ego/v3/egopb"
	"github.com/tochemey/ego/v3/eventstream"
	"github.com/tochemey/ego/v3/internal/syncmap"
	"github.com/tochemey/ego/v3/offsetstore"
	"github.com/tochemey/ego/v3/persistence"
	"github.com/tochemey/ego/v3/projection"
)

var (
	// ErrEngineNotStarted is returned when the eGo engine has not started
	ErrEngineNotStarted = errors.New("eGo engine has not started")
	// ErrUndefinedEntityID is returned when sending a command to an undefined entity
	ErrUndefinedEntityID = errors.New("eGo entity id is not defined")
	// ErrCommandReplyUnmarshalling is returned when unmarshalling command reply failed
	ErrCommandReplyUnmarshalling = errors.New("failed to parse command reply")
	// ErrDurableStateStoreRequired is returned when the eGo engine durable store is not set
	ErrDurableStateStoreRequired = errors.New("durable state store is required")
)

// Done is a signal that an operation has completed
type Done struct{}

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
	name               string                  // name is the application name
	eventsStore        persistence.EventsStore // eventsStore is the events store
	stateStore         persistence.StateStore  // stateStore is the durable state store
	enableCluster      *atomic.Bool            // enableCluster enable/disable cluster mode
	actorSystem        goakt.ActorSystem       // actorSystem is the underlying actor system
	logger             log.Logger              // logger is the logging engine to use
	discoveryProvider  discovery.Provider      // discoveryProvider is the discovery provider for clustering
	partitionsCount    uint64                  // partitionsCount specifies the number of partitions
	started            atomic.Bool
	bindAddr           string
	peersPort          int
	discoveryPort      int
	remotingPort       int
	minimumPeersQuorum uint16
	eventStream        eventstream.Stream
	mutex              *sync.Mutex
	remoting           *goakt.Remoting
	tls                *TLS

	eventsStreams *syncmap.Map[string, *eventsStream]
	statesStreams *syncmap.Map[string, *statesStream]
}

// NewEngine creates and initializes a new instance of the eGo engine.
//
// This function constructs an engine with the specified name and event store, applying any additional
// configuration options. The engine serves as the core for managing event-sourced entities, durable
// state entities, and projections.
//
// Parameters:
//   - name: A unique identifier for the engine instance.
//   - eventsStore: The event store responsible for persisting and retrieving events.
//   - opts: Optional configurations to customize engine behavior.
//
// Returns:
//   - A pointer to the newly created Engine instance.
func NewEngine(name string, eventsStore persistence.EventsStore, opts ...Option) *Engine {
	e := &Engine{
		name:          name,
		eventsStore:   eventsStore,
		enableCluster: atomic.NewBool(false),
		logger:        log.New(log.ErrorLevel, os.Stderr),
		eventStream:   eventstream.New(),
		mutex:         &sync.Mutex{},
		bindAddr:      "0.0.0.0",
		remoting:      goakt.NewRemoting(),
		eventsStreams: syncmap.New[string, *eventsStream](),
		statesStreams: syncmap.New[string, *statesStream](),
	}

	for _, opt := range opts {
		opt.Apply(e)
	}

	if e.tls != nil {
		e.remoting = goakt.NewRemoting(goakt.WithRemotingTLS(e.tls.ClientTLS))
	}

	e.started.Store(false)
	return e
}

// Start initializes and starts eGo engine.
//
// This function launches the engine, enabling it to manage event-sourced entities, durable state entities,
// and projections. It ensures that all necessary components are initialized and ready to process commands
// and events.
//
// Parameters:
//   - ctx: Execution context for managing startup behavior and handling cancellations.
//
// Returns:
//   - An error if the engine fails to start due to misconfiguration or system issues; otherwise, nil.
func (engine *Engine) Start(ctx context.Context) error {
	opts := []goakt.Option{
		goakt.WithLogger(engine.logger),
		goakt.WithPassivationDisabled(),
		goakt.WithActorInitMaxRetries(1),
	}

	if engine.tls != nil {
		opts = append(opts, goakt.WithTLS(&goakt.TLSInfo{
			ClientTLS: engine.tls.ClientTLS,
			ServerTLS: engine.tls.ServerTLS,
		}))
	}

	if engine.enableCluster.Load() {
		if engine.bindAddr == "" {
			engine.bindAddr, _ = os.Hostname()
		}

		replicaCount := 1
		if engine.minimumPeersQuorum > 1 {
			replicaCount = 2
		}

		clusterConfig := goakt.
			NewClusterConfig().
			WithDiscovery(engine.discoveryProvider).
			WithDiscoveryPort(engine.discoveryPort).
			WithPeersPort(engine.peersPort).
			WithMinimumPeersQuorum(uint32(engine.minimumPeersQuorum)).
			WithReplicaCount(uint32(replicaCount)).
			WithPartitionCount(engine.partitionsCount).
			WithKinds(
				new(eventSourcedActor),
				new(durableStateActor),
			)

		opts = append(opts,
			goakt.WithCluster(clusterConfig),
			goakt.WithRemote(remote.NewConfig(engine.bindAddr, engine.remotingPort)))
	}

	var err error
	engine.actorSystem, err = goakt.NewActorSystem(engine.name, opts...)
	if err != nil {
		return fmt.Errorf("failed to create the ego actor system: %w", err)
	}

	if err := engine.actorSystem.Start(ctx); err != nil {
		return err
	}

	engine.started.Store(true)

	return nil
}

// AddProjection registers a new projection with the eGo engine and starts its execution.
//
// The projection processes events from the events store applying the specified handler to manage state updates
// based on incoming events. The provided offset store ensures the projection maintains its processing position
// across restarts.
//
// Key behavior:
//   - Projections once created, will persist for the entire lifespan of the running eGo system.
//
// Parameters:
//   - ctx: Execution context used for cancellation and deadlines.
//   - name: A unique identifier for the projection.
//   - handler: The event handler responsible for processing events and updating the projection state.
//   - offsetStore: The storage mechanism for tracking the last processed event, ensuring resumability.
//   - opts: Optional configuration settings that modify projection behavior.
//
// Returns an error if the projection fails to start due to misconfiguration or underlying system issues.
func (engine *Engine) AddProjection(ctx context.Context, name string, handler projection.Handler, offsetStore offsetstore.OffsetStore, opts ...projection.Option) error {
	if !engine.Started() {
		return ErrEngineNotStarted
	}

	actor := projection.New(name, handler, engine.eventsStore, offsetStore, opts...)

	engine.mutex.Lock()
	actorSystem := engine.actorSystem
	engine.mutex.Unlock()

	// projections are long-lived actors
	// TODO: resuming the projection may not be a good option. Need to figure out the correlation between the handler
	// TODO: retry policy and the backing actor behavior
	supervisor := goakt.NewSupervisor(goakt.WithAnyErrorDirective(goakt.ResumeDirective))
	if _, err := actorSystem.Spawn(ctx, name,
		actor,
		goakt.WithLongLived(),
		goakt.WithRelocationDisabled(),
		goakt.WithSupervisor(supervisor)); err != nil {
		return fmt.Errorf("failed to register the projection=(%s): %w", name, err)
	}

	return nil
}

// RemoveProjection stops and removes the specified projection from the engine.
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
func (engine *Engine) RemoveProjection(ctx context.Context, name string) error {
	if !engine.Started() {
		return ErrEngineNotStarted
	}

	engine.mutex.Lock()
	actorSystem := engine.actorSystem
	engine.mutex.Unlock()

	return actorSystem.Kill(ctx, name)
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
	engine.mutex.Lock()
	actorSystem := engine.actorSystem
	engine.mutex.Unlock()

	addr, pid, err := actorSystem.ActorOf(ctx, name)
	if err != nil {
		return false, fmt.Errorf("failed to get projection %s: %w", name, err)
	}

	if pid != nil {
		return pid.IsRunning(), nil
	}

	return addr.Equals(address.NoSender()), nil
}

// Stop gracefully shuts down the eGo engine.
//
// This function ensures that all running projections, entities, and processes managed by the engine
// are properly stopped before termination. It waits for ongoing operations to complete or time out
// based on the provided context.
//
// Parameters:
//   - ctx: Execution context for managing cancellation and timeouts.
//
// Returns:
//   - An error if the shutdown process encounters issues; otherwise, nil.
func (engine *Engine) Stop(ctx context.Context) error {
	if !engine.Started() {
		return nil
	}

	engine.started.Store(false)

	// shutdown all event eventsStreams
	eventsStreams := engine.eventsStreams.Values()
	for _, stream := range eventsStreams {
		// signal the publisher to stop
		stream.done <- Done{}
		stream.subscriber.Shutdown()
		if err := stream.publisher.Close(ctx); err != nil {
			return err
		}
	}

	statesStreams := engine.statesStreams.Values()
	for _, stream := range statesStreams {
		stream.done <- Done{}
		stream.subscriber.Shutdown()
		if err := stream.publisher.Close(ctx); err != nil {
			return err
		}
	}

	engine.eventStream.Close()
	engine.eventsStreams.Reset()
	engine.statesStreams.Reset()
	return engine.actorSystem.Stop(ctx)
}

// Started returns true when the eGo engine has started
func (engine *Engine) Started() bool {
	return engine.started.Load()
}

// Subscribe creates an events subscriber.
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

	engine.mutex.Lock()
	eventStream := engine.eventStream
	engine.mutex.Unlock()

	subscriber := eventStream.AddSubscriber()
	eventTopics := generateTopics(eventsTopic, engine.partitionsCount)
	stateTopics := generateTopics(statesTopic, engine.partitionsCount)
	for _, topic := range append(eventTopics, stateTopics...) {
		eventStream.Subscribe(subscriber, topic)
	}

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

	engine.mutex.Lock()
	actorSystem := engine.actorSystem
	eventsStore := engine.eventsStore
	eventStream := engine.eventStream
	engine.mutex.Unlock()

	config := newSpawnConfig(opts...)
	var sOptions []goakt.SpawnOption

	switch {
	case config.passivateAfter > 0:
		sOptions = append(sOptions,
			goakt.WithRelocationDisabled(),
			goakt.WithPassivateAfter(config.passivateAfter))
	default:
		sOptions = append(sOptions,
			goakt.WithRelocationDisabled(),
			goakt.WithLongLived())
	}

	_, err := actorSystem.Spawn(ctx,
		behavior.ID(),
		newEventSourcedActor(behavior, eventsStore, eventStream),
		sOptions...)
	return err
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

	engine.mutex.Lock()
	actorSystem := engine.actorSystem
	durableStateStore := engine.stateStore
	eventStream := engine.eventStream
	engine.mutex.Unlock()

	if durableStateStore == nil {
		return ErrDurableStateStoreRequired
	}

	config := newSpawnConfig(opts...)
	var sOptions []goakt.SpawnOption

	switch {
	case config.passivateAfter > 0:
		sOptions = append(sOptions,
			goakt.WithRelocationDisabled(),
			goakt.WithPassivateAfter(config.passivateAfter))
	default:
		sOptions = append(sOptions,
			goakt.WithRelocationDisabled(),
			goakt.WithLongLived())
	}

	_, err := actorSystem.Spawn(ctx,
		behavior.ID(),
		newDurableStateActor(behavior, durableStateStore, eventStream),
		sOptions...)
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
func (engine *Engine) SendCommand(ctx context.Context, entityID string, cmd Command, timeout time.Duration) (resultingState State, revision uint64, err error) {
	if !engine.Started() {
		return nil, 0, ErrEngineNotStarted
	}

	// entityID is not defined
	if entityID == "" {
		return nil, 0, ErrUndefinedEntityID
	}

	engine.mutex.Lock()
	actorSystem := engine.actorSystem
	engine.mutex.Unlock()

	// locate the given actor
	addr, pid, err := actorSystem.ActorOf(ctx, entityID)
	if err != nil {
		return nil, 0, err
	}

	var reply proto.Message
	switch {
	case pid != nil:
		reply, err = goakt.Ask(ctx, pid, cmd, timeout)
	case addr != nil:
		res, err := engine.remoting.RemoteAsk(ctx, address.NoSender(), addr, cmd, timeout)
		if err == nil {
			// let us unmarshal the response
			reply, err = res.UnmarshalNew()
		}
	}

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
		topics := generateTopics(eventsTopic, engine.partitionsCount)
		for _, topic := range topics {
			engine.logger.Debugf("%s subscribing to topic: %s", publisher.ID(), topic)
			engine.eventStream.Subscribe(subscriber, topic)
		}

		// create an instance of the event subscriber
		eventSubscriber := &eventsStream{
			publisher:  publisher,
			subscriber: subscriber,
			done:       make(chan Done, 1),
		}

		// add the event publisher to the engine
		engine.eventsStreams.Set(publisher.ID(), eventSubscriber)

		// start the event publisher
		engine.logger.Infof("starting %s events publisher....", publisher.ID())
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
		topics := generateTopics(statesTopic, engine.partitionsCount)

		for _, topic := range topics {
			engine.logger.Debugf("%s subscribing to topic: %s", publisher.ID(), topic)
			engine.eventStream.Subscribe(subscriber, topic)
		}

		// create an instance of the state subscriber
		stateSubscriber := &statesStream{
			publisher:  publisher,
			subscriber: subscriber,
			done:       make(chan Done, 1),
		}

		// add the state publisher to the engine
		engine.statesStreams.Set(publisher.ID(), stateSubscriber)

		// start the state publisher
		engine.logger.Infof("starting %s durable state publisher....", publisher.ID())
		go engine.sendState(stateSubscriber)
	}

	return nil
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

// sendEvent sends events to the event publisher
func (engine *Engine) sendEvent(stream *eventsStream) {
	for {
		select {
		case <-stream.done:
			return
		case message := <-stream.subscriber.Iterator():
			if message == nil {
				continue
			}

			event, ok := message.Payload().(*egopb.Event)
			if !ok {
				continue
			}

			publisher := stream.publisher
			if err := publisher.Publish(context.Background(), event); err != nil {
				engine.logger.Errorf("(%s) failed to publish event=[persistenceID=%s, sequenceNumber=%d]: %s",
					publisher.ID(),
					event.GetPersistenceId(),
					event.GetSequenceNumber(),
					err.Error())
				continue
			}

			engine.logger.Infof("(%s) successfully published event=[persistenceID=%s, sequenceNumber=%d]: %s",
				publisher.ID(),
				event.GetPersistenceId(),
				event.GetSequenceNumber())
		}
	}
}

// sendState sends state changes to the state publisher
func (engine *Engine) sendState(stream *statesStream) {
	for {
		select {
		case <-stream.done:
			return
		case message := <-stream.subscriber.Iterator():
			if message == nil {
				continue
			}

			msg, ok := message.Payload().(*egopb.DurableState)
			if !ok {
				continue
			}

			publisher := stream.publisher
			if err := publisher.Publish(context.Background(), msg); err != nil {
				engine.logger.Errorf("(%s) failed to publish state=[persistenceID=%s, version=%d]: %s",
					publisher.ID(),
					msg.GetPersistenceId(),
					msg.GetVersionNumber(),
					err.Error())
				continue
			}

			engine.logger.Infof("(%s) successfully published state=[persistenceID=%s, version=%d]: %s",
				publisher.ID(),
				msg.GetPersistenceId(),
				msg.GetVersionNumber())
		}
	}
}

// generateTopics generates a list of topics based on the base topic name and partitions count.
func generateTopics(baseTopic string, partitionsCount uint64) []string {
	var topics []string
	switch {
	case partitionsCount == 0:
		topics = append(topics, fmt.Sprintf(baseTopic, 0))
	default:
		for i := range int(partitionsCount) {
			topics = append(topics, fmt.Sprintf(baseTopic, i))
		}
	}
	return topics
}
