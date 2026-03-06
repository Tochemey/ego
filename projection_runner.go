// MIT License
//
// Copyright (c) 2022-2026 Arsene Tochemey Gandote
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package ego

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime/debug"
	"sync"
	"time"

	"github.com/flowchartsman/retry"
	"github.com/tochemey/goakt/v4/log"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/tochemey/ego/v3/egopb"
	"github.com/tochemey/ego/v3/internal/ticker"
	"github.com/tochemey/ego/v3/offsetstore"
	"github.com/tochemey/ego/v3/persistence"
	"github.com/tochemey/ego/v3/projection"
)

// numWorkers is the fixed size of the persistent shard-processing goroutine pool.
const numWorkers = 5

// shardItem is a unit of work dispatched to the persistent worker pool.
// The embedded WaitGroup pointer lets processingLoop wait for a whole batch.
type shardItem struct {
	shard uint64
	wg    *sync.WaitGroup
}

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

	// worker pool — initialised in Start, torn down in Stop.
	workCh       chan shardItem // shard dispatch channel shared by all workers
	workerErrCh  chan error     // first per-batch error reported by workers
	workerCtx    context.Context
	workerCancel context.CancelFunc

	// retrier is pre-created once for RetryAndFail / RetryAndSkip policies to
	// avoid allocating a new retrier on every event.
	retrier *retry.Retrier

	// wgPool pools *sync.WaitGroup for per-batch synchronisation.
	// Proto message pointers (*egopb.ProjectionId, *egopb.Offset) are NOT
	// pooled: the OffsetStore interface methods accept pointer arguments that
	// implementations (including testify mocks) may retain after the call
	// returns, so pooling and resetting those pointers causes data races.
	wgPool sync.Pool
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
		logger:         log.NewZap(log.ErrorLevel, os.Stderr),
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

	runner.wgPool.New = func() any { return new(sync.WaitGroup) }

	// Pre-create the retrier once; it is stateless between calls so it can be
	// shared safely across concurrent workers.
	if policy := runner.recovery.RecoveryPolicy(); policy == projection.RetryAndFail || policy == projection.RetryAndSkip {
		runner.retrier = retry.NewRetrier(
			int(runner.recovery.Retries()),
			runner.recovery.RetryDelay(),
			runner.recovery.RetryDelay(),
		)
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

	// workerCtx is cancelled by Stop() to drain and exit the worker pool.
	x.workerCtx, x.workerCancel = context.WithCancel(ctx)
	// Buffer of 64 prevents the dispatch loop from blocking on individual shard
	// sends even when all workers are briefly busy.
	x.workCh = make(chan shardItem, 64)
	// One slot per worker is enough: we only ever surface the first error.
	x.workerErrCh = make(chan error, numWorkers)

	x.ticker = ticker.New(x.pullInterval)
	x.running.Store(true)

	for range numWorkers {
		go x.worker(x.workerCtx)
	}

	return nil
}

// Stop stops the projection projectionRunner
func (x *projectionRunner) Stop() error {
	// CompareAndSwap ensures Stop is idempotent and race-free when called
	// concurrently (e.g. from processingLoop on error and from the outside).
	if !x.running.CompareAndSwap(true, false) {
		return nil
	}
	x.stopSignal <- struct{}{}
	x.ticker.Stop()
	x.workerCancel()
	return nil
}

// Name returns the projection projectionRunner Name
func (x *projectionRunner) Name() string {
	return x.name
}

// Run start the projectionRunner
func (x *projectionRunner) Run(_ context.Context) {
	x.ticker.Start()
	// processingLoop receives the worker-pool context so that cancellation from
	// Stop() propagates through both the dispatch select and the workers.
	go x.processingLoop(x.workerCtx)
}

// processingLoop is a loop that continuously runs to process events persisted onto the journal store until the projection is stopped.
// It drives the persistent worker pool: on each tick it fetches all shard
// numbers, dispatches them to workers, and waits for the batch to complete
// before moving to the next tick.  No goroutines or channels are allocated
// per tick.
func (x *projectionRunner) processingLoop(ctx context.Context) {
	for {
		select {
		case <-x.stopSignal:
			return
		case <-ctx.Done():
			return
		case <-x.ticker.Ticks:
			if !x.running.Load() {
				continue
			}

			shards, err := x.eventsStore.ShardNumbers(ctx)
			if err != nil {
				x.logger.Error(fmt.Errorf("failed to fetch the list of shards: %w", err))
				x.ticker.Stop()
				_ = x.Stop()
				return
			}

			if len(shards) == 0 {
				continue
			}

			// Obtain a WaitGroup from the pool to track this batch.
			wg := x.wgPool.Get().(*sync.WaitGroup)
			wg.Add(len(shards))

			dispatched := 0
			for _, shard := range shards {
				select {
				case x.workCh <- shardItem{shard: shard, wg: wg}:
					dispatched++
				case <-ctx.Done():
					// Account for shards that were counted in wg.Add but
					// never dispatched so that wg.Wait() does not deadlock.
					for i := dispatched; i < len(shards); i++ {
						wg.Done()
					}
					wg.Wait()
					x.wgPool.Put(wg)
					return
				}
			}

			wg.Wait()
			x.wgPool.Put(wg)

			// Surface the first worker error for this batch, if any.
			select {
			case err := <-x.workerErrCh:
				x.logger.Error(err)
				x.ticker.Stop()
				_ = x.Stop()
				return
			default:
			}
		}
	}
}

// worker is one member of the persistent goroutine pool.  It reads shard items
// from workCh and processes them until the pool context is cancelled.
func (x *projectionRunner) worker(ctx context.Context) {
	for {
		select {
		case item, ok := <-x.workCh:
			if !ok {
				return
			}
			if err := x.doProcess(ctx, item.shard); err != nil && ctx.Err() == nil {
				// Only forward processing errors, not context-cancellation noise
				// that arises from a concurrent Stop() call.
				select {
				case x.workerErrCh <- err:
				default:
				}
			}
			item.wg.Done()
		case <-ctx.Done():
			return
		}
	}
}

// doProcess processes all events of a given persistent entity and hand them over to the handler
func (x *projectionRunner) doProcess(ctx context.Context, shard uint64) error {
	if !x.running.Load() {
		return nil
	}

	currOffset, err := x.currentOffset(ctx, &egopb.ProjectionId{
		ProjectionName: x.name,
		ShardNumber:    shard,
	})
	if err != nil {
		return err
	}

	events, nextOffset, err := x.eventsStore.GetShardEvents(ctx, shard, currOffset, uint64(x.maxBufferSize))
	if err != nil {
		return err
	}

	if len(events) == 0 {
		return nil
	}

	return x.processEvents(ctx, shard, events, nextOffset)
}

// currentOffset resolves the starting offset for a projection shard.
func (x *projectionRunner) currentOffset(ctx context.Context, projectionID *egopb.ProjectionId) (int64, error) {
	offset, err := x.offsetsStore.GetCurrentOffset(ctx, projectionID)
	if err != nil {
		return 0, err
	}

	currOffset := offset.GetValue()
	if !x.startingOffset.IsZero() {
		currOffset = x.startingOffset.UnixMilli()
	}

	return currOffset, nil
}

// processEvents iterates over events and applies the projection handler.
func (x *projectionRunner) processEvents(ctx context.Context, shard uint64, events []*egopb.Event, nextOffset int64) error {
	for _, envelope := range events {
		if err := x.processEnvelope(ctx, shard, envelope, nextOffset); err != nil {
			return err
		}
	}
	return nil
}

// processEnvelope handles a single event and commits its offset.
func (x *projectionRunner) processEnvelope(ctx context.Context, shard uint64, envelope *egopb.Event, nextOffset int64) error {
	state := envelope.GetResultingState()
	event := envelope.GetEvent()
	seqNr := envelope.GetSequenceNumber()
	persistenceID := envelope.GetPersistenceId()

	if err := x.handleWithPolicy(ctx, persistenceID, event, state, seqNr); err != nil {
		return err
	}

	return x.commitOffset(ctx, shard, nextOffset, persistenceID)
}

// handleWithPolicy executes the handler with the configured recovery policy.
func (x *projectionRunner) handleWithPolicy(ctx context.Context, persistenceID string, event, state *anypb.Any, seqNr uint64) error {
	switch x.recovery.RecoveryPolicy() {
	case projection.Fail:
		if err := x.handleSafely(ctx, persistenceID, event, state, seqNr); err != nil {
			x.logHandlerError(err, persistenceID, seqNr)
			return err
		}
	case projection.RetryAndFail:
		if err := x.retryHandle(ctx, persistenceID, event, state, seqNr, true); err != nil {
			return err
		}
	case projection.RetryAndSkip:
		_ = x.retryHandle(ctx, persistenceID, event, state, seqNr, false)
	case projection.Skip:
		if err := x.handleSafely(ctx, persistenceID, event, state, seqNr); err != nil {
			x.logHandlerError(err, persistenceID, seqNr)
		}
	default:
		if err := x.handleSafely(ctx, persistenceID, event, state, seqNr); err != nil {
			x.logHandlerError(err, persistenceID, seqNr)
			return err
		}
	}

	return nil
}

// retryHandle runs the handler with the pre-created retrier and optional per-attempt logging.
func (x *projectionRunner) retryHandle(ctx context.Context, persistenceID string, event, state *anypb.Any, seqNr uint64, logEachAttempt bool) error {
	err := x.retrier.Run(func() error {
		handleErr := x.handleSafely(ctx, persistenceID, event, state, seqNr)
		if handleErr != nil && logEachAttempt {
			x.logHandlerError(handleErr, persistenceID, seqNr)
		}
		return handleErr
	})
	if err != nil && !logEachAttempt {
		x.logHandlerError(err, persistenceID, seqNr)
	}

	return err
}

// handleSafely invokes the handler and converts panics into errors.
func (x *projectionRunner) handleSafely(ctx context.Context, persistenceID string, event, state *anypb.Any, seqNr uint64) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = newHandlerPanicError(recovered)
		}
	}()

	return x.handler.Handle(ctx, persistenceID, event, state, seqNr)
}

// logHandlerError emits a consistent error message for handler failures.
func (x *projectionRunner) logHandlerError(err error, persistenceID string, seqNr uint64) {
	x.logger.Error(fmt.Errorf("failed to process event for persistence id=%s, revision=%d: %w", persistenceID, seqNr, err))
}

// commitOffset persists the processed offset for the given shard.
// time.Now().UnixMilli() is used directly, avoiding the two intermediate
// allocations that timestamppb.Now().AsTime().UnixMilli() would produce.
// Note: *egopb.Offset is NOT pooled because the OffsetStore interface permits
// implementations to retain the pointer after WriteOffset returns.
func (x *projectionRunner) commitOffset(ctx context.Context, shard uint64, nextOffset int64, persistenceID string) error {
	offset := &egopb.Offset{
		ShardNumber:    shard,
		ProjectionName: x.name,
		Value:          nextOffset,
		Timestamp:      time.Now().UnixMilli(),
	}

	if err := x.offsetsStore.WriteOffset(ctx, offset); err != nil {
		return fmt.Errorf("failed to persist offset for persistence id=%s: %w", persistenceID, err)
	}
	return nil
}

// handlerPanicError wraps panics from projection handlers with stack context.
type handlerPanicError struct {
	value any
	stack []byte
}

// Error formats the panic value and stack for logging.
func (e *handlerPanicError) Error() string {
	return fmt.Sprintf("projection handler panic: %v\n%s", e.value, e.stack)
}

// newHandlerPanicError captures a panic and returns a structured error.
func newHandlerPanicError(value any) error {
	return &handlerPanicError{
		value: value,
		stack: debug.Stack(),
	}
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
