/*
 * MIT License
 *
 * Copyright (c) 2022-2024 Tochemey
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

	"github.com/tochemey/goakt/v2/address"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"

	"github.com/tochemey/goakt/v2/actors"
	"github.com/tochemey/goakt/v2/discovery"
	"github.com/tochemey/goakt/v2/log"

	"github.com/tochemey/ego/v3/egopb"
	"github.com/tochemey/ego/v3/eventstore"
	"github.com/tochemey/ego/v3/eventstream"
	"github.com/tochemey/ego/v3/offsetstore"
	"github.com/tochemey/ego/v3/projection"
)

var (
	// ErrEngineNotStarted is returned when the eGo engine has not started
	ErrEngineNotStarted = errors.New("eGo engine has not started")
	// ErrUndefinedEntityID is returned when sending a command to an undefined entity
	ErrUndefinedEntityID = errors.New("eGo entity id is not defined")
	// ErrCommandReplyUnmarshalling is returned when unmarshalling command reply failed
	ErrCommandReplyUnmarshalling = errors.New("failed to parse command reply")
)

// Engine represents the engine that empowers the various entities
type Engine struct {
	name               string                 // name is the application name
	eventsStore        eventstore.EventsStore // eventsStore is the events store
	enableCluster      *atomic.Bool           // enableCluster enable/disable cluster mode
	actorSystem        actors.ActorSystem     // actorSystem is the underlying actor system
	logger             log.Logger             // logger is the logging engine to use
	discoveryProvider  discovery.Provider     // discoveryProvider is the discovery provider for clustering
	partitionsCount    uint64                 // partitionsCount specifies the number of partitions
	started            atomic.Bool
	hostName           string
	peersPort          int
	gossipPort         int
	remotingPort       int
	minimumPeersQuorum uint16
	eventStream        eventstream.Stream
	mutex              *sync.Mutex
	remoting           *actors.Remoting
}

// NewEngine creates an instance of Engine
func NewEngine(name string, eventsStore eventstore.EventsStore, opts ...Option) *Engine {
	e := &Engine{
		name:          name,
		eventsStore:   eventsStore,
		enableCluster: atomic.NewBool(false),
		logger:        log.New(log.ErrorLevel, os.Stderr),
		eventStream:   eventstream.New(),
		mutex:         &sync.Mutex{},
		remoting:      actors.NewRemoting(),
	}

	for _, opt := range opts {
		opt.Apply(e)
	}

	e.started.Store(false)
	return e
}

// Start starts the ego engine
func (engine *Engine) Start(ctx context.Context) error {
	opts := []actors.Option{
		actors.WithLogger(engine.logger),
		actors.WithPassivationDisabled(),
		actors.WithActorInitMaxRetries(1),
	}

	if engine.enableCluster.Load() {
		if engine.hostName == "" {
			engine.hostName, _ = os.Hostname()
		}

		replicaCount := 1
		if engine.minimumPeersQuorum > 1 {
			replicaCount = 2
		}

		clusterConfig := actors.
			NewClusterConfig().
			WithDiscovery(engine.discoveryProvider).
			WithDiscoveryPort(engine.gossipPort).
			WithPeersPort(engine.peersPort).
			WithMinimumPeersQuorum(uint32(engine.minimumPeersQuorum)).
			WithReplicaCount(uint32(replicaCount)).
			WithPartitionCount(engine.partitionsCount).
			WithKinds(new(actor))

		opts = append(opts,
			actors.WithCluster(clusterConfig),
			actors.WithRemoting(engine.hostName, int32(engine.remotingPort)))
	}

	var err error
	engine.actorSystem, err = actors.NewActorSystem(engine.name, opts...)
	if err != nil {
		return fmt.Errorf("failed to create the ego actor system: %w", err)
	}

	if err := engine.actorSystem.Start(ctx); err != nil {
		return err
	}

	engine.started.Store(true)

	return nil
}

// AddProjection add a projection to the running eGo engine and starts it
func (engine *Engine) AddProjection(ctx context.Context, name string, handler projection.Handler, offsetStore offsetstore.OffsetStore, opts ...projection.Option) error {
	if !engine.Started() {
		return ErrEngineNotStarted
	}

	actor := projection.New(name, handler, engine.eventsStore, offsetStore, opts...)

	engine.mutex.Lock()
	actorSystem := engine.actorSystem
	engine.mutex.Unlock()

	if _, err := actorSystem.Spawn(ctx, name, actor); err != nil {
		return fmt.Errorf("failed to register the projection=(%s): %w", name, err)
	}

	return nil
}

// RemoveProjection stops and removes a given projection from the engine
func (engine *Engine) RemoveProjection(ctx context.Context, name string) error {
	if !engine.Started() {
		return ErrEngineNotStarted
	}

	engine.mutex.Lock()
	actorSystem := engine.actorSystem
	engine.mutex.Unlock()

	return actorSystem.Kill(ctx, name)
}

// IsProjectionRunning returns true when the projection is active and running
// One needs to check the error to see whether this function does not return a false negative
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

// Stop stops the ego engine
func (engine *Engine) Stop(ctx context.Context) error {
	engine.started.Store(false)
	engine.eventStream.Close()
	return engine.actorSystem.Stop(ctx)
}

// Started returns true when the eGo engine has started
func (engine *Engine) Started() bool {
	return engine.started.Load()
}

// Subscribe creates an events subscriber
func (engine *Engine) Subscribe() (eventstream.Subscriber, error) {
	if !engine.Started() {
		return nil, ErrEngineNotStarted
	}

	engine.mutex.Lock()
	eventStream := engine.eventStream
	engine.mutex.Unlock()

	subscriber := eventStream.AddSubscriber()
	for i := 0; i < int(engine.partitionsCount); i++ {
		topic := fmt.Sprintf(eventsTopic, i)
		engine.eventStream.Subscribe(subscriber, topic)
	}

	return subscriber, nil
}

// Entity creates an entity. This will return the entity path
// that can be used to send command to the entity
func (engine *Engine) Entity(ctx context.Context, behavior EntityBehavior) error {
	if !engine.Started() {
		return ErrEngineNotStarted
	}

	engine.mutex.Lock()
	actorSystem := engine.actorSystem
	eventsStore := engine.eventsStore
	eventStream := engine.eventStream
	engine.mutex.Unlock()

	_, err := actorSystem.Spawn(ctx,
		behavior.ID(),
		newActor(behavior, eventsStore, eventStream))
	if err != nil {
		return err
	}

	return nil
}

// SendCommand sends command to a given entity ref.
// This will return:
// 1. the resulting state after the command has been handled and the emitted event persisted
// 2. nil when there is no resulting state or no event persisted
// 3. an error in case of error
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
		reply, err = actors.Ask(ctx, pid, cmd, timeout)
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
