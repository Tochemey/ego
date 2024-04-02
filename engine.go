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
	"go.uber.org/atomic"

	"github.com/tochemey/ego/eventstore"
	"github.com/tochemey/ego/eventstream"
	egotel "github.com/tochemey/ego/internal/telemetry"
	"github.com/tochemey/ego/offsetstore"
	"github.com/tochemey/ego/projection"
	"github.com/tochemey/goakt/actors"
	"github.com/tochemey/goakt/discovery"
	"github.com/tochemey/goakt/log"
	"github.com/tochemey/goakt/telemetry"
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
	e := &Engine{
		name:          name,
		eventsStore:   eventsStore,
		enableCluster: atomic.NewBool(false),
		logger:        log.DefaultLogger,
		telemetry:     telemetry.New(),
		eventStream:   eventstream.New(),
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
		opts = append(opts, actors.WithClustering(
			discovery.NewServiceDiscovery(x.discoveryProvider, x.discoveryConfig),
			x.partitionsCount))
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

	if !x.started.Load() {
		return errors.New("eGo engine has not started")
	}

	actor := projection.New(name, handler, x.eventsStore, offsetStore, opts...)

	var pid actors.PID
	var err error

	if pid, err = x.actorSystem.Spawn(spanCtx, name, actor); err != nil {
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

	if !x.started.Load() {
		return nil, errors.New("eGo engine has not started")
	}

	subscriber := x.eventStream.AddSubscriber()
	for i := 0; i < int(x.partitionsCount); i++ {
		topic := fmt.Sprintf(eventsTopic, i)
		x.eventStream.Subscribe(subscriber, topic)
	}

	return subscriber, nil
}
