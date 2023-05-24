package ego

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/tochemey/ego/aggregate"
	"github.com/tochemey/ego/egopb"
	"github.com/tochemey/ego/storage"
	"github.com/tochemey/goakt/actors"
	"github.com/tochemey/goakt/discovery"
	"github.com/tochemey/goakt/log"
	"github.com/tochemey/goakt/pkg/telemetry"
)

// Ego represents the engine that empowers the various entities
type Ego struct {
	name          string               // name is the application name
	remotingHost  string               // remotingHost is the host machine ip address
	remotingPort  int32                // remotingPort is the host machine port number. This port number is the remoting port number
	eventsStore   storage.EventsStore  // eventsStore is the events store
	enableCluster bool                 // enableCluster enable/disable cluster mode
	actorSystem   actors.ActorSystem   // actorSystem is the underlying actor system
	logger        log.Logger           // logger is the logging engine to use
	discoveryMode discovery.Discovery  // discoveryMode is the discovery provider for clustering
	telemetry     *telemetry.Telemetry // telemetry is the observability engine
}

// New creates an instance of Ego
func New(name string, eventsStore storage.EventsStore, opts ...Option) *Ego {
	// create an instance of ego
	e := &Ego{
		name:          name,
		eventsStore:   eventsStore,
		enableCluster: false,
		remotingPort:  0,
		remotingHost:  "",
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
func (e *Ego) Start(ctx context.Context) error {
	// create a variable to hold the options
	opts := []actors.Option{
		actors.WithLogger(e.logger),
		actors.WithPassivationDisabled(),
		actors.WithActorInitMaxRetries(1),
		actors.WithReplyTimeout(5 * time.Second),
		actors.WithTelemetry(e.telemetry),
		actors.WithSupervisorStrategy(actors.StopDirective),
	}
	// set the remaining options
	if e.enableCluster {
		opts = append(opts, actors.WithClustering(e.discoveryMode, e.remotingPort))
	}

	// create the actor system configuration
	cfg, err := actors.NewConfig(e.name, opts...)
	// handle the error
	if err != nil {
		// log the error
		e.logger.Error(errors.Wrap(err, "failed to create the ego actor system configuration"))
		return err
	}

	// create the actor system that will empower the entities
	e.actorSystem, err = actors.NewActorSystem(cfg)
	// handle the error
	if err != nil {
		// log the error
		e.logger.Error(errors.Wrap(err, "failed to create the ego actor system"))
		return err
	}
	// start the actor system
	return e.actorSystem.Start(ctx)
}

// Stop stops the ego engine
func (e *Ego) Stop(ctx context.Context) error {
	return e.actorSystem.Stop(ctx)
}

// SendCommand sends command to a given entity ref
func (e *Ego) SendCommand(ctx context.Context, command aggregate.Command, entityID string) (*egopb.CommandReply, error) {
	// first check whether we are running in cluster mode or not
	if e.enableCluster {
		// grab the remote address of the actor
		addr, err := e.actorSystem.GetRemoteActor(ctx, entityID)
		// handle the error
		if err != nil {
			e.logger.Error(errors.Wrap(err, "failed to remotely locate actor address"))
			return nil, err
		}
		// send a remote message to the actor
		reply, err := actors.RemoteAsk(ctx, addr, command)
		// handle the error
		if err != nil {
			e.logger.Error(err)
			return nil, err
		}
		// let us unmarshall the reply
		commandReply := new(egopb.CommandReply)
		err = reply.UnmarshalTo(commandReply)
		return commandReply, err
	}
	// locate the given actor
	pid, err := e.actorSystem.GetLocalActor(ctx, entityID)
	// handle the error
	if err != nil {
		e.logger.Error(errors.Wrap(err, "failed to locally locate actor address"))
		return nil, err
	}
	// send the command to the actor
	reply, err := actors.Ask(ctx, pid, command, time.Second)
	// handle the error
	if err != nil {
		e.logger.Error(err)
		return nil, err
	}

	// TODO use ok, comma idiom to cast
	return reply.(*egopb.CommandReply), nil
}

// CreateAggregate creates an entity and return the entity reference
func CreateAggregate[T aggregate.State](ctx context.Context, e *Ego, behavior aggregate.Behavior[T]) {
	// create the entity
	e.actorSystem.StartActor(ctx, behavior.ID(), aggregate.New(behavior, e.eventsStore))
}
