package ego

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/tochemey/ego/egopb"
	"github.com/tochemey/ego/entity"
	"github.com/tochemey/ego/eventstore"
	"github.com/tochemey/goakt/actors"
	"github.com/tochemey/goakt/discovery"
	"github.com/tochemey/goakt/log"
	"github.com/tochemey/goakt/pkg/telemetry"
	"go.uber.org/atomic"
)

// Ego represents the engine that empowers the various entities
type Ego struct {
	name              string                 // name is the application name
	eventsStore       eventstore.EventsStore // eventsStore is the events store
	enableCluster     *atomic.Bool           // enableCluster enable/disable cluster mode
	actorSystem       actors.ActorSystem     // actorSystem is the underlying actor system
	logger            log.Logger             // logger is the logging engine to use
	discoveryProvider discovery.Provider     // discoveryProvider is the discovery provider for clustering
	discoveryConfig   discovery.Config       // discoveryConfig is the discovery provider config for clustering
	telemetry         *telemetry.Telemetry   // telemetry is the observability engine
	partitionsCount   uint64                 // partitionsCount specifies the number of partitions
}

// New creates an instance of Ego
func New(name string, eventsStore eventstore.EventsStore, opts ...Option) *Ego {
	// create an instance of ego
	e := &Ego{
		name:          name,
		eventsStore:   eventsStore,
		enableCluster: atomic.NewBool(false),
		logger:        log.DefaultLogger,
		telemetry:     telemetry.New(),
	}
	// apply the various options
	for _, opt := range opts {
		opt.Apply(e)
	}
	return e
}

// Start starts the ego engine
func (x *Ego) Start(ctx context.Context) error {
	// create a variable to hold the options
	opts := []actors.Option{
		actors.WithLogger(x.logger),
		actors.WithPassivationDisabled(),
		actors.WithActorInitMaxRetries(1),
		actors.WithReplyTimeout(5 * time.Second),
		actors.WithTelemetry(x.telemetry),
		actors.WithSupervisorStrategy(actors.StopDirective),
	}
	// set the remaining options
	if x.enableCluster.Load() {
		opts = append(opts, actors.WithClustering(
			discovery.NewServiceDiscovery(x.discoveryProvider, x.discoveryConfig),
			x.partitionsCount))
	}

	var err error
	// create the actor system that will empower the entities
	x.actorSystem, err = actors.NewActorSystem(x.name, opts...)
	// handle the error
	if err != nil {
		// log the error
		x.logger.Error(errors.Wrap(err, "failed to create the ego actor system"))
		return err
	}
	// start the actor system
	return x.actorSystem.Start(ctx)
}

// Stop stops the ego engine
func (x *Ego) Stop(ctx context.Context) error {
	return x.actorSystem.Stop(ctx)
}

// SendCommand sends command to a given entity ref
func (x *Ego) SendCommand(ctx context.Context, command entity.Command, entityID string) (*egopb.CommandReply, error) {
	// first check whether we are running in cluster mode or not
	if x.enableCluster.Load() {
		// grab the remote address of the actor
		addr, err := x.actorSystem.RemoteActor(ctx, entityID)
		// handle the error
		if err != nil {
			x.logger.Error(errors.Wrap(err, "failed to remotely locate actor address"))
			return nil, err
		}
		// send a remote message to the actor
		reply, err := actors.RemoteAsk(ctx, addr, command)
		// handle the error
		if err != nil {
			// create a custom error
			e := errors.Wrapf(err, "failed to send command to entity=(%s)", entityID)
			// log the error
			x.logger.Error(e)
			return nil, e
		}
		// let us unmarshall the reply
		commandReply := new(egopb.CommandReply)
		err = reply.UnmarshalTo(commandReply)
		return commandReply, err
	}
	// locate the given actor
	pid, err := x.actorSystem.LocalActor(ctx, entityID)
	// handle the error
	if err != nil {
		// create a custom error
		e := errors.Wrap(err, "failed to locally locate actor address")
		// log the error
		x.logger.Error(e)
		return nil, e
	}
	// send the command to the actor
	reply, err := actors.Ask(ctx, pid, command, time.Second)
	// handle the error
	if err != nil {
		x.logger.Error(err)
		return nil, err
	}

	// TODO use ok, comma idiom to cast
	return reply.(*egopb.CommandReply), nil
}

// NewEntity creates an entity and return the entity reference
func NewEntity[T entity.State](ctx context.Context, e *Ego, entityBehavior entity.Behavior[T]) actors.PID {
	// create the entity
	return e.actorSystem.Spawn(ctx, entityBehavior.ID(), entity.New(entityBehavior, e.eventsStore))
}
