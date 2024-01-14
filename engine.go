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
	"time"

	"github.com/pkg/errors"
	"github.com/tochemey/ego/eventstore"
	"github.com/tochemey/ego/eventstream"
	egotel "github.com/tochemey/ego/internal/telemetry"
	"github.com/tochemey/ego/offsetstore"
	"github.com/tochemey/ego/projection"
	"github.com/tochemey/goakt/actors"
	"github.com/tochemey/goakt/discovery"
	"github.com/tochemey/goakt/log"
	"github.com/tochemey/goakt/telemetry"
	"go.uber.org/atomic"
)

// Engine represents the engine that empowers the various entities
type Engine struct {
	name              string                 // name is the application name
	eventsStore       eventstore.EventsStore // eventsStore is the events store
	enableCluster     *atomic.Bool           // enableCluster enable/disable cluster mode
	actorSystem       actors.ActorSystem     // actorSystem is the underlying actor system
	logger            log.Logger             // logger is the logging engine to use
	discoveryProvider discovery.Provider     // discoveryProvider is the discovery provider for clustering
	discoveryConfig   discovery.Config       // discoveryConfig is the discovery provider config for clustering
	telemetry         *telemetry.Telemetry   // telemetry is the observability engine
	partitionsCount   uint64                 // partitionsCount specifies the number of partitions
	started           atomic.Bool

	eventStream eventstream.Stream
}

// NewEngine creates an instance of Engine
func NewEngine(name string, eventsStore eventstore.EventsStore, opts ...Option) *Engine {
	// create an instance of ego
	e := &Engine{
		name:          name,
		eventsStore:   eventsStore,
		enableCluster: atomic.NewBool(false),
		logger:        log.DefaultLogger,
		telemetry:     telemetry.New(),
		eventStream:   eventstream.New(),
	}
	// apply the various options
	for _, opt := range opts {
		opt.Apply(e)
	}

	e.started.Store(false)
	return e
}

// Start starts the ego engine
func (x *Engine) Start(ctx context.Context) error {
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
	if err := x.actorSystem.Start(ctx); err != nil {
		return err
	}
	// set the started to true
	x.started.Store(true)

	return nil
}

// AddProjection add a projection to the running eGo engine and start it
func (x *Engine) AddProjection(ctx context.Context, name string, handler projection.Handler, offsetStore offsetstore.OffsetStore, opts ...projection.Option) error {
	// add a span context
	spanCtx, span := egotel.SpanContext(ctx, "AddProjection")
	defer span.End()

	// first check whether the ego engine has started or not
	if !x.started.Load() {
		return errors.New("eGo engine has not started")
	}
	// create the projection actor
	actor := projection.New(name, handler, x.eventsStore, offsetStore, opts...)
	// define variables to hold projection actor ref and error
	var pid actors.PID
	var err error
	// spawn the actor
	if pid, err = x.actorSystem.Spawn(spanCtx, name, actor); err != nil {
		// add some error logging
		x.logger.Error(errors.Wrapf(err, "failed to register the projection=(%s)", name))
		return err
	}
	// start the projection
	if err := actors.Tell(spanCtx, pid, projection.Start); err != nil {
		// add some error logging
		x.logger.Error(errors.Wrapf(err, "failed to start the projection=(%s)", name))
		return err
	}

	return nil
}

// Stop stops the ego engine
func (x *Engine) Stop(ctx context.Context) error {
	// set the started to false
	x.started.Store(false)
	// close the event stream
	x.eventStream.Close()
	// stop the actor system and return the possible error
	return x.actorSystem.Stop(ctx)
}

// Subscribe creates an events subscriber
func (x *Engine) Subscribe(ctx context.Context) (eventstream.Subscriber, error) {
	// add a span context
	_, span := egotel.SpanContext(ctx, "Subscribe")
	defer span.End()

	// first check whether the ego engine has started or not
	if !x.started.Load() {
		return nil, errors.New("eGo engine has not started")
	}
	// create the subscriber
	subscriber := x.eventStream.AddSubscriber()
	// subscribe to all the topics
	for i := 0; i < int(x.partitionsCount); i++ {
		// create the topic
		topic := fmt.Sprintf(eventsTopic, i)
		// subscribe to the topic
		x.eventStream.Subscribe(subscriber, topic)
	}
	// return the subscriber
	return subscriber, nil
}
