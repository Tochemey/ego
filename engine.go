/*
 * Copyright (c) 2022-2023 Tochemey
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
	"time"

	"github.com/pkg/errors"
	"github.com/tochemey/ego/eventstore"
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

	// define the list of projections
	projections       []*Projection
	projectionRunners []*projection.Runner
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
		projections:   make([]*Projection, 0),
	}
	// apply the various options
	for _, opt := range opts {
		opt.Apply(e)
	}

	// set the projection runners
	for i, p := range e.projections {
		// create the projection instance and add it to the list of runners
		e.projectionRunners[i] = projection.NewRunner(
			p.Name,
			p.Handler,
			e.eventsStore,
			p.OffsetStore,
			projection.WithMaxBufferSize(p.MaxBufferSize),
			projection.WithRefreshInterval(p.RefreshInterval),
			projection.WithRecoveryStrategy(p.Recovery),
			projection.WithStartOffset(p.StartOffset),
			projection.WithResetOffset(p.ResetOffset),
			projection.WithLogger(e.logger))
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

	// start the projections if is set
	if len(x.projectionRunners) > 0 {
		// simply iterate the list of projections and start them
		// fail start when a single projection fail to start
		for _, runner := range x.projectionRunners {
			// start the runner
			if err := runner.Start(ctx); err != nil {
				x.logger.Error(errors.Wrapf(err, "failed to start projection=(%s)", runner.Name()))
				return err
			}
		}
	}

	return nil
}

// Stop stops the ego engine
func (x *Engine) Stop(ctx context.Context) error {
	// set the started to false
	x.started.Store(false)
	// stop the projections
	if len(x.projectionRunners) > 0 {
		// simply iterate the list of projections and start them
		// fail stop when a single projection fail to stop
		for _, runner := range x.projectionRunners {
			// stop the runner
			if err := runner.Stop(ctx); err != nil {
				x.logger.Error(errors.Wrapf(err, "failed to stop projection=(%s)", runner.Name()))
				return err
			}
		}
	}
	// stop the actor system and return the possible error
	return x.actorSystem.Stop(ctx)
}
