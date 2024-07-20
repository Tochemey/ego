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

package projection

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/flowchartsman/retry"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/tochemey/goakt/v2/log"

	"github.com/tochemey/ego/v3/egopb"
	"github.com/tochemey/ego/v3/eventstore"
	"github.com/tochemey/ego/v3/internal/telemetry"
	"github.com/tochemey/ego/v3/offsetstore"
)

// runner defines the projection runner
type runner struct {
	// Name specifies the runner Name
	name string
	// Logger specifies the logger
	logger log.Logger
	// Handler specifies the projection handler
	handler Handler
	// JournalStore specifies the journal store for reading events
	eventsStore eventstore.EventsStore
	// OffsetStore specifies the offset store to commit offsets
	offsetsStore offsetstore.OffsetStore
	// Specifies the recovery setting
	recovery *Recovery
	// stop signal
	stopSignal chan struct{}
	// started status
	started *atomic.Bool
	// refresh interval. Events are fetched with this interval
	// the default value is 1s
	refreshInterval time.Duration
	// defines how many events are fetched
	// the default value is 500
	maxBufferSize int
	// defines the timestamp where to start consuming events
	startingOffset time.Time
	// reset the projection offset to a given timestamp
	resetOffsetTo time.Time
}

// newRunner create an instance of runner given the name of the projection, the handler and the offsets store
// The name of the projection should be unique
func newRunner(name string,
	handler Handler,
	eventsStore eventstore.EventsStore,
	offsetStore offsetstore.OffsetStore,
	opts ...Option) *runner {
	runner := &runner{
		name:            name,
		logger:          log.DefaultLogger,
		handler:         handler,
		eventsStore:     eventsStore,
		offsetsStore:    offsetStore,
		recovery:        NewRecovery(),
		stopSignal:      make(chan struct{}, 1),
		started:         atomic.NewBool(false),
		refreshInterval: time.Second,
		maxBufferSize:   500,
		startingOffset:  time.Time{},
		resetOffsetTo:   time.Time{},
	}

	for _, opt := range opts {
		opt.Apply(runner)
	}

	return runner
}

// Start starts the projection runner
func (x *runner) Start(ctx context.Context) error {
	spanCtx, span := telemetry.SpanContext(ctx, "PreStart")
	defer span.End()

	if x.started.Load() {
		return nil
	}

	if x.offsetsStore == nil {
		return errors.New("offsets store is not defined")
	}

	if err := x.offsetsStore.Ping(spanCtx); err != nil {
		return fmt.Errorf("failed to connect to the offsets store: %ws", err)
	}

	if x.eventsStore == nil {
		return errors.New("events store is not defined")
	}

	if err := x.eventsStore.Ping(spanCtx); err != nil {
		return fmt.Errorf("failed to connect to the events store: %w", err)
	}

	// we will ping the stores 5 times to see whether there have started successfully or not.
	// The operation will be done in an exponential backoff mechanism with an initial delay of a second and a maximum delay of a second.
	// Once the retries have completed and still not connected we fail the start process of the projection.
	const (
		maxRetries   = 5
		initialDelay = time.Second
		maxDelay     = time.Second
	)
	// create a new instance of retrier that will try a maximum of five times, with
	// an initial delay of 100 ms and a maximum delay of 1 second
	retrier := retry.NewRetrier(maxRetries, initialDelay, maxDelay)
	err := retrier.RunContext(spanCtx, func(ctx context.Context) error {
		g, ctx := errgroup.WithContext(ctx)
		g.Go(func() error {
			return x.eventsStore.Ping(ctx)
		})
		g.Go(func() error {
			return x.offsetsStore.Ping(ctx)
		})
		return g.Wait()
	})

	if err != nil {
		return fmt.Errorf("failed to start the projection: %w", err)
	}

	if err := x.preStart(spanCtx); err != nil {
		return err
	}

	x.started.Store(true)

	return nil
}

// Stop stops the projection runner
func (x *runner) Stop(ctx context.Context) error {
	_, span := telemetry.SpanContext(ctx, "PostStop")
	defer span.End()

	if !x.started.Load() {
		return nil
	}

	x.stopSignal <- struct{}{}

	x.started.Store(false)
	return nil
}

// Name returns the projection runner Name
func (x *runner) Name() string {
	return x.name
}

// Run start the runner
func (x *runner) Run(ctx context.Context) {
	// start processing
	go x.processingLoop(ctx)
}

// processingLoop is a loop that continuously runs to process events persisted onto the journal store until the projection is stopped
func (x *runner) processingLoop(ctx context.Context) {
	ticker := time.NewTicker(x.refreshInterval)
	tickerStopSig := make(chan struct{}, 1)
	go func() {
		for {
			select {
			case <-x.stopSignal:
				tickerStopSig <- struct{}{}
				return
			case <-ticker.C:
				g, ctx := errgroup.WithContext(ctx)
				shardsChan := make(chan uint64, 1)

				// let us fetch the shards
				g.Go(func() error {
					defer close(shardsChan)
					shards, err := x.eventsStore.ShardNumbers(ctx)
					if err != nil {
						x.logger.Error(fmt.Errorf("failed to fetch the list of shards:%w", err))
						return err
					}

					for _, shard := range shards {
						select {
						case shardsChan <- shard:
						case <-ctx.Done():
							return ctx.Err()
						}
					}
					return nil
				})

				// Start a fixed number of goroutines process the shards.
				for i := 0; i < 5; i++ {
					g.Go(func() error {
						for shard := range shardsChan {
							select {
							case <-ctx.Done():
								return ctx.Err()
							default:
								return x.doProcess(ctx, shard)
							}
						}
						return nil
					})
				}

				// wait for all the processing to be done
				if err := g.Wait(); err != nil {
					x.logger.Error(err)
					if err := x.Stop(ctx); err != nil {
						x.logger.Error(err)
						return
					}
				}
			}
		}
	}()

	<-tickerStopSig
	ticker.Stop()
}

// doProcess processes all events of a given persistent entity and hand them over to the handler
func (x *runner) doProcess(ctx context.Context, shard uint64) error {
	spanCtx, span := telemetry.SpanContext(ctx, "HandleShard")
	defer span.End()

	if !x.started.Load() {
		return nil
	}

	projectionID := &egopb.ProjectionId{
		ProjectionName: x.name,
		ShardNumber:    shard,
	}

	offset, err := x.offsetsStore.GetCurrentOffset(spanCtx, projectionID)
	if err != nil {
		return err
	}

	currOffset := offset.GetValue()
	if !x.startingOffset.IsZero() {
		currOffset = x.startingOffset.UnixMilli()
	}

	events, nextOffset, err := x.eventsStore.GetShardEvents(spanCtx, shard, currOffset, uint64(x.maxBufferSize))
	if err != nil {
		return err
	}

	length := len(events)

	if length == 0 {
		return nil
	}

	// define a variable that hold the number of events successfully processed
	// iterate the events
	for i := 0; i < length; i++ {
		envelope := events[i]

		state := envelope.GetResultingState()
		event := envelope.GetEvent()
		seqNr := envelope.GetSequenceNumber()
		persistenceID := envelope.GetPersistenceId()

		// send the request to the handler based upon the recovery strategy in place
		switch x.recovery.RecoveryPolicy() {
		case Fail:
			// send the data to the handler. In case of error we log the error and fail the projection
			if err := x.handler.Handle(spanCtx, persistenceID, event, state, seqNr); err != nil {
				x.logger.Error(fmt.Errorf("failed to process event for persistence id=%s, revision=%d: %w", persistenceID, seqNr, err))
				return err
			}

		case RetryAndFail:
			retries := x.recovery.Retries()
			delay := x.recovery.RetryDelay()
			// create a new exponential backoff that will try a maximum of retries times, with
			// an initial delay of 100 ms and a maximum delay
			backoff := retry.NewRetrier(int(retries), 100*time.Millisecond, delay)
			// pass the data to the projection handler
			if err := backoff.Run(func() error {
				if err := x.handler.Handle(spanCtx, persistenceID, event, state, seqNr); err != nil {
					x.logger.Error(fmt.Errorf("failed to process event for persistence id=%s, revision=%d: %w", persistenceID, seqNr, err))
					return err
				}
				return nil
			}); err != nil {
				// because we fail we return the error without committing the offset
				return err
			}

		case RetryAndSkip:
			retries := x.recovery.Retries()
			delay := x.recovery.RetryDelay()
			// create a new exponential backoff that will try a maximum of retries times, with
			// an initial delay of 100 ms and a maximum delay
			backoff := retry.NewRetrier(int(retries), 100*time.Millisecond, delay)
			// pass the data to the projection handler
			if err := backoff.Run(func() error {
				return x.handler.Handle(spanCtx, persistenceID, event, state, seqNr)
			}); err != nil {
				// here we just log the error, but we skip the event and commit the offset
				x.logger.Error(fmt.Errorf("failed to process event for persistence id=%s, revision=%d: %w", persistenceID, seqNr, err))
			}

		case Skip:
			// send the data to the handler. In case of error we just log the error and skip the event by committing the offset
			if err := x.handler.Handle(spanCtx, persistenceID, event, state, seqNr); err != nil {
				x.logger.Error(fmt.Errorf("failed to process event for persistence id=%s, revision=%d: %w", persistenceID, seqNr, err))
			}
		}

		// the envelope has been successfully processed
		// here we commit the offset to the offset store and continue the next event
		offset = &egopb.Offset{
			ShardNumber:    shard,
			ProjectionName: x.name,
			Value:          nextOffset,
			Timestamp:      timestamppb.Now().AsTime().UnixMilli(),
		}

		if err := x.offsetsStore.WriteOffset(spanCtx, offset); err != nil {
			return fmt.Errorf("failed to persist offset for persistence id=%s: %w", persistenceID, err)
		}
	}

	return nil
}

// preStart is used to perform some tasks before the projection starts
func (x *runner) preStart(ctx context.Context) error {
	spanCtx, span := telemetry.SpanContext(ctx, "PreStart")
	defer span.End()

	if !x.resetOffsetTo.IsZero() {
		if err := x.offsetsStore.ResetOffset(spanCtx, x.name, x.resetOffsetTo.UnixMilli()); err != nil {
			x.logger.Error(fmt.Errorf("failed to reset projection=%s: %w", x.name, err))
			return err
		}
	}

	return nil
}
