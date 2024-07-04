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
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"

	"github.com/tochemey/goakt/v2/actors"
	"github.com/tochemey/goakt/v2/discovery"
	"github.com/tochemey/goakt/v2/log"
	"github.com/tochemey/goakt/v2/telemetry"

	"github.com/tochemey/ego/v3/egopb"
	"github.com/tochemey/ego/v3/eventstore"
	"github.com/tochemey/ego/v3/eventstream"
	egotel "github.com/tochemey/ego/v3/internal/telemetry"
	"github.com/tochemey/ego/v3/offsetstore"
	"github.com/tochemey/ego/v3/projection"
)

var (
	// ErrEngineRequired is returned when the eGo engine is not set
	ErrEngineRequired = errors.New("eGo engine is not defined")
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
	telemetry          *telemetry.Telemetry   // telemetry is the observability engine
	partitionsCount    uint64                 // partitionsCount specifies the number of partitions
	started            atomic.Bool
	hostName           string
	peersPort          int
	gossipPort         int
	remotingPort       int
	minimumPeersQuorum uint16
	eventStream        eventstream.Stream
	locker             *sync.Mutex
}

// NewEngine creates an instance of Engine
func NewEngine(name string, eventsStore eventstore.EventsStore, opts ...Option) *Engine {
	e := &Engine{
		name:          name,
		eventsStore:   eventsStore,
		enableCluster: atomic.NewBool(false),
		logger:        log.DefaultLogger,
		telemetry:     telemetry.New(),
		eventStream:   eventstream.New(),
		locker:        &sync.Mutex{},
	}

	for _, opt := range opts {
		opt.Apply(e)
	}

	e.started.Store(false)
	return e
}

// Start starts the ego engine
func (x *Engine) Start(ctx context.Context) error {
	opts := []actors.Option{
		actors.WithLogger(x.logger),
		actors.WithPassivationDisabled(),
		actors.WithActorInitMaxRetries(1),
		actors.WithReplyTimeout(5 * time.Second),
		actors.WithTelemetry(x.telemetry),
		actors.WithSupervisorStrategy(actors.StopDirective),
	}

	if x.enableCluster.Load() {
		if x.hostName == "" {
			x.hostName, _ = os.Hostname()
		}

		replicaCount := 1
		if x.minimumPeersQuorum > 1 {
			replicaCount = 2
		}

		clusterConfig := actors.
			NewClusterConfig().
			WithDiscovery(x.discoveryProvider).
			WithGossipPort(x.gossipPort).
			WithPeersPort(x.peersPort).
			WithMinimumPeersQuorum(uint32(x.minimumPeersQuorum)).
			WithReplicaCount(uint32(replicaCount)).
			WithPartitionCount(x.partitionsCount).
			WithKinds(new(actor))

		opts = append(opts,
			actors.WithCluster(clusterConfig),
			actors.WithRemoting(x.hostName, int32(x.remotingPort)))
	}

	var err error
	x.actorSystem, err = actors.NewActorSystem(x.name, opts...)
	if err != nil {
		x.logger.Error(errors.Wrap(err, "failed to create the ego actor system"))
		return err
	}

	if err := x.actorSystem.Start(ctx); err != nil {
		return err
	}

	x.started.Store(true)

	return nil
}

// AddProjection add a projection to the running eGo engine and start it
func (x *Engine) AddProjection(ctx context.Context, name string, handler projection.Handler, offsetStore offsetstore.OffsetStore, opts ...projection.Option) error {
	spanCtx, span := egotel.SpanContext(ctx, "AddProjection")
	defer span.End()

	x.locker.Lock()
	started := x.started.Load()
	x.locker.Unlock()
	if !started {
		return ErrEngineNotStarted
	}

	actor := projection.New(name, handler, x.eventsStore, offsetStore, opts...)

	var (
		pid actors.PID
		err error
	)

	x.locker.Lock()
	actorSystem := x.actorSystem
	x.locker.Unlock()

	if pid, err = actorSystem.Spawn(spanCtx, name, actor); err != nil {
		x.logger.Error(errors.Wrapf(err, "failed to register the projection=(%s)", name))
		return err
	}

	if err := actors.Tell(spanCtx, pid, projection.Start); err != nil {
		x.logger.Error(errors.Wrapf(err, "failed to start the projection=(%s)", name))
		return err
	}

	return nil
}

// Stop stops the ego engine
func (x *Engine) Stop(ctx context.Context) error {
	x.started.Store(false)
	x.eventStream.Close()
	return x.actorSystem.Stop(ctx)
}

// Subscribe creates an events subscriber
func (x *Engine) Subscribe(ctx context.Context) (eventstream.Subscriber, error) {
	_, span := egotel.SpanContext(ctx, "Subscribe")
	defer span.End()

	x.locker.Lock()
	started := x.started.Load()
	x.locker.Unlock()
	if !started {
		return nil, ErrEngineNotStarted
	}

	x.locker.Lock()
	eventStream := x.eventStream
	x.locker.Unlock()

	subscriber := eventStream.AddSubscriber()
	for i := 0; i < int(x.partitionsCount); i++ {
		topic := fmt.Sprintf(eventsTopic, i)
		x.eventStream.Subscribe(subscriber, topic)
	}

	return subscriber, nil
}

// Entity creates an entity. This will return the entity path
// that can be used to send command to the entity
func (x *Engine) Entity(ctx context.Context, behavior EntityBehavior) error {
	x.locker.Lock()
	started := x.started.Load()
	x.locker.Unlock()
	if !started {
		return ErrEngineNotStarted
	}

	x.locker.Lock()
	actorSystem := x.actorSystem
	eventsStore := x.eventsStore
	eventStream := x.eventStream
	x.locker.Unlock()

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
func (x *Engine) SendCommand(ctx context.Context, entityID string, cmd Command, timeout time.Duration) (resultingState State, revision uint64, err error) {
	ctx, span := egotel.SpanContext(ctx, "SendCommand")
	defer span.End()

	x.locker.Lock()
	started := x.started.Load()
	x.locker.Unlock()
	if !started {
		return nil, 0, ErrEngineNotStarted
	}

	// entityID is not defined
	if entityID == "" {
		return nil, 0, ErrUndefinedEntityID
	}

	x.locker.Lock()
	actorSystem := x.actorSystem
	x.locker.Unlock()

	// locate the given actor
	addr, pid, err := actorSystem.ActorOf(ctx, entityID)
	if err != nil {
		return nil, 0, err
	}

	var reply proto.Message
	if pid != nil {
		// put the given command to the underlying actor of the entity
		reply, err = actors.Ask(ctx, pid, cmd, timeout)
	} else if addr != nil {
		// send the command to the given address
		res, err := actors.RemoteAsk(ctx, addr, cmd, timeout)
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
