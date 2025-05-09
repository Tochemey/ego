/*
 * MIT License
 *
 * Copyright (c) 2022-2025 Arsene Tochemey Gandote
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
	"time"

	"github.com/flowchartsman/retry"
	"github.com/tochemey/goakt/v3/log"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/tochemey/ego/v3/egopb"
	"github.com/tochemey/ego/v3/internal/ticker"
	"github.com/tochemey/ego/v3/offsetstore"
	"github.com/tochemey/ego/v3/persistence"
	"github.com/tochemey/ego/v3/projection"
)

// projectionRunner defines the projection projectionRunner
type projectionRunner struct {
	// Name specifies the projectionRunner Name
	name string
	// Logger specifies the logger
	logger log.Logger
	// Handler specifies the projection handler
	handler projection.Handler
	// JournalStore specifies the journal store for reading events
	eventsStore persistence.EventsStore
	// OffsetStore specifies the offset store to commit offsets
	offsetsStore offsetstore.OffsetStore
	// Specifies the recovery setting
	recovery *projection.Recovery
	// stop signal
	stopSignal chan struct{}
	// running status
	running *atomic.Bool
	// pull interval. Events are fetched with this interval
	// the default value is 1s
	pullInterval time.Duration
	// defines how many events are fetched
	// the default value is 500
	maxBufferSize int
	// defines the timestamp where to start consuming events
	startingOffset time.Time
	// reset the projection offset to a given timestamp
	resetOffsetTo time.Time
	ticker        *ticker.Ticker
}

// newProjectionRunner create an instance of projectionRunner given the name of the projection, the underlying and the offsets store
// The name of the projection should be unique
func newProjectionRunner(name string,
	handler projection.Handler,
	eventsStore persistence.EventsStore,
	offsetStore offsetstore.OffsetStore,
	opts ...runnerOption) *projectionRunner {
	runner := &projectionRunner{
		name:           name,
		logger:         log.New(log.ErrorLevel, os.Stderr),
		handler:        handler,
		eventsStore:    eventsStore,
		offsetsStore:   offsetStore,
		recovery:       projection.NewRecovery(),
		stopSignal:     make(chan struct{}, 1),
		running:        atomic.NewBool(false),
		pullInterval:   time.Second,
		maxBufferSize:  500,
		startingOffset: ZeroTime,
		resetOffsetTo:  ZeroTime,
	}

	for _, opt := range opts {
		opt.Apply(runner)
	}

	return runner
}

// Start starts the projection projectionRunner
func (x *projectionRunner) Start(ctx context.Context) error {
	if x.running.Load() {
		return nil
	}

	if x.offsetsStore == nil {
		return errors.New("offsets store is not defined")
	}

	if x.eventsStore == nil {
		return errors.New("events store is not defined")
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
	err := retrier.RunContext(ctx, func(ctx context.Context) error {
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

	if err := x.preStart(ctx); err != nil {
		return err
	}

	x.ticker = ticker.New(x.pullInterval)
	x.running.Store(true)

	return nil
}

// Stop stops the projection projectionRunner
func (x *projectionRunner) Stop() error {
	if !x.running.Load() {
		return nil
	}

	x.stopSignal <- struct{}{}
	x.running.Store(false)
	x.ticker.Stop()
	return nil
}

// Name returns the projection projectionRunner Name
func (x *projectionRunner) Name() string {
	return x.name
}

// Run start the projectionRunner
func (x *projectionRunner) Run(ctx context.Context) {
	x.ticker.Start()
	go x.processingLoop(ctx)
}

// processingLoop is a loop that continuously runs to process events persisted onto the journal store until the projection is stopped
func (x *projectionRunner) processingLoop(ctx context.Context) {
	for x.ticker.Ticking() {
		select {
		case <-x.stopSignal:
			return
		case <-x.ticker.Ticks:
			if x.running.Load() {
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
				for range 5 {
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
					x.ticker.Stop()
					_ = x.Stop()
					return
				}
			}
		}
	}
}

// doProcess processes all events of a given persistent entity and hand them over to the handler
func (x *projectionRunner) doProcess(ctx context.Context, shard uint64) error {
	if !x.running.Load() {
		return nil
	}

	projectionID := &egopb.ProjectionId{
		ProjectionName: x.name,
		ShardNumber:    shard,
	}

	offset, err := x.offsetsStore.GetCurrentOffset(ctx, projectionID)
	if err != nil {
		return err
	}

	currOffset := offset.GetValue()
	if !x.startingOffset.IsZero() {
		currOffset = x.startingOffset.UnixMilli()
	}

	events, nextOffset, err := x.eventsStore.GetShardEvents(ctx, shard, currOffset, uint64(x.maxBufferSize))
	if err != nil {
		return err
	}

	length := len(events)

	if length == 0 {
		return nil
	}

	// define a variable that hold the number of events successfully processed
	// iterate the events
	for i := range length {
		envelope := events[i]

		state := envelope.GetResultingState()
		event := envelope.GetEvent()
		seqNr := envelope.GetSequenceNumber()
		persistenceID := envelope.GetPersistenceId()

		// send the request to the underlying based upon the recovery strategy in place
		switch x.recovery.RecoveryPolicy() {
		case projection.Fail:
			// send the data to the underlying. In case of error we log the error and fail the projection
			if err := x.handler.Handle(ctx, persistenceID, event, state, seqNr); err != nil {
				x.logger.Error(fmt.Errorf("failed to process event for persistence id=%s, revision=%d: %w", persistenceID, seqNr, err))
				return err
			}

		case projection.RetryAndFail:
			retries := x.recovery.Retries()
			delay := x.recovery.RetryDelay()
			// create a new exponential backoff that will try a maximum of retries times
			backoff := retry.NewRetrier(int(retries), delay, delay)
			// pass the data to the projection underlying
			if err := backoff.Run(func() error {
				if err := x.handler.Handle(ctx, persistenceID, event, state, seqNr); err != nil {
					x.logger.Error(fmt.Errorf("failed to process event for persistence id=%s, revision=%d: %w", persistenceID, seqNr, err))
					return err
				}
				return nil
			}); err != nil {
				// because we fail we return the error without committing the offset
				return err
			}

		case projection.RetryAndSkip:
			retries := x.recovery.Retries()
			delay := x.recovery.RetryDelay()
			// create a new exponential backoff that will try a maximum of retries times
			backoff := retry.NewRetrier(int(retries), delay, delay)
			// pass the data to the projection underlying
			if err := backoff.Run(func() error {
				return x.handler.Handle(ctx, persistenceID, event, state, seqNr)
			}); err != nil {
				// here we just log the error, but we skip the event and commit the offset
				x.logger.Error(fmt.Errorf("failed to process event for persistence id=%s, revision=%d: %w", persistenceID, seqNr, err))
			}

		case projection.Skip:
			// send the data to the underlying. In case of error we just log the error and skip the event by committing the offset
			if err := x.handler.Handle(ctx, persistenceID, event, state, seqNr); err != nil {
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

		if err := x.offsetsStore.WriteOffset(ctx, offset); err != nil {
			return fmt.Errorf("failed to persist offset for persistence id=%s: %w", persistenceID, err)
		}
	}

	return nil
}

// preStart is used to perform some tasks before the projection starts
func (x *projectionRunner) preStart(ctx context.Context) error {
	if !x.resetOffsetTo.IsZero() {
		if err := x.offsetsStore.ResetOffset(ctx, x.name, x.resetOffsetTo.UnixMilli()); err != nil {
			fmtErr := fmt.Errorf("failed to reset projection=%s: %w", x.name, err)
			x.logger.Error(fmtErr)
			return fmtErr
		}
	}

	return nil
}
