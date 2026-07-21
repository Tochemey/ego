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
	goakt "github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/log"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/tochemey/ego/v4/egopb"
	"github.com/tochemey/ego/v4/encryption"
	"github.com/tochemey/ego/v4/eventadapter"
	"github.com/tochemey/ego/v4/eventstream"
	"github.com/tochemey/ego/v4/internal/ticker"
	"github.com/tochemey/ego/v4/offsetstore"
	"github.com/tochemey/ego/v4/persistence"
	"github.com/tochemey/ego/v4/projection"
)

// numWorkers is the fixed size of the persistent shard-processing goroutine pool.
const numWorkers = 5

const (
	// storeRetryInitialDelay is the backoff delay applied after the first
	// failed store round trip before the pull pass is retried.
	storeRetryInitialDelay = time.Second
	// storeRetryMaxDelay caps the exponential backoff delay between
	// consecutive store retry attempts.
	storeRetryMaxDelay = 30 * time.Second
)

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

	// deadLetterHandler receives events that failed processing after exhausting
	// the recovery policy. When nil, failed events are silently discarded.
	deadLetterHandler projection.DeadLetterHandler

	// eventAdapters transforms persisted events from older schema versions
	// during projection consumption.
	eventAdapters []eventadapter.EventAdapter

	// metrics holds pre-created metric instruments for recording projection metrics.
	metrics *metrics

	// encryptor decrypts event payloads before handing them to the handler.
	encryptor encryption.Encryptor

	// committedOffsets caches the last committed offset per shard, sparing a
	// GetCurrentOffset round trip on every pull. The projection runs as a
	// cluster singleton, so this runner is the sole writer of its offsets
	// and the cache cannot go stale.
	committedOffsetsMu sync.RWMutex
	committedOffsets   map[uint64]int64

	// pendingBuf is reused across pull passes to collect pending shards.
	// Passes are serialized by processingLoop, so no locking is needed.
	pendingBuf []uint64

	// storeFailures counts consecutive failed store round trips and drives the
	// in-loop retry backoff. It resets to zero on the first clean pass.
	// Passes are serialized by processingLoop, so no locking is needed.
	storeFailures int

	// eventsStream, when set, triggers an immediate pull whenever events are
	// persisted on this node. Events persisted on peer nodes are picked up
	// by the interval-based pull.
	eventsStream     eventstream.Stream
	streamSubscriber eventstream.Subscriber

	// nudge triggers an immediate pull pass. Capacity one so sends coalesce.
	nudge chan struct{}

	// sawFullBatch reports that a shard returned a full buffer during the
	// current pass, in which case the loop re-pulls without waiting for the
	// next tick.
	sawFullBatch atomic.Bool

	// pid, when set, receives a *runnerFailed message after the
	// processing loop stops permanently because an event cannot be processed,
	// so the host projection actor can escalate the error through supervision.
	// Failed store round trips never notify the host actor: they are retried
	// in place with exponential backoff.
	pid *goakt.PID
}

// newProjectionRunner create an instance of projectionRunner given the name of the projection, the underlying and the offsets store
// The name of the projection should be unique
func newProjectionRunner(name string,
	handler projection.Handler,
	eventsStore persistence.EventsStore,
	offsetStore offsetstore.OffsetStore,
	opts ...runnerOption) *projectionRunner {
	runner := &projectionRunner{
		name:             name,
		logger:           log.NewZap(log.ErrorLevel, os.Stderr),
		handler:          handler,
		eventsStore:      eventsStore,
		offsetsStore:     offsetStore,
		recovery:         projection.NewRecovery(),
		stopSignal:       make(chan struct{}, 1),
		running:          atomic.NewBool(false),
		pullInterval:     time.Second,
		maxBufferSize:    500,
		startingOffset:   ZeroTime,
		resetOffsetTo:    ZeroTime,
		committedOffsets: make(map[uint64]int64),
		nudge:            make(chan struct{}, 1),
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

	// Subscribe to the in-process events stream so locally persisted events
	// trigger an immediate pull instead of waiting for the next tick.
	if x.eventsStream != nil {
		x.streamSubscriber = x.eventsStream.AddSubscriber()
		x.eventsStream.Subscribe(x.streamSubscriber, eventsTopic)
		go x.nudgeLoop(x.workerCtx)
	}

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
	if x.eventsStream != nil && x.streamSubscriber != nil {
		x.eventsStream.RemoveSubscriber(x.streamSubscriber)
		x.streamSubscriber.Shutdown()
	}
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
// It drives the persistent worker pool: on each pull it resolves the shards
// that are behind, dispatches them to workers, and waits for the batch to
// complete before the next pull.  No goroutines or channels are allocated per
// pull.  A pull is triggered by the ticker, by a nudge from the local events
// stream, or by a full-buffer read reporting that more events are pending.
func (x *projectionRunner) processingLoop(ctx context.Context) {
	for {
		select {
		case <-x.stopSignal:
			return
		case <-ctx.Done():
			return
		case <-x.ticker.Ticks:
		case <-x.nudge:
		}

		if !x.running.Load() {
			continue
		}

		if !x.runPass(ctx) {
			return
		}
	}
}

// runPass executes one full pull pass. It returns false when the projection
// must stop (context cancelled or an unprocessable event surfaced). A failed
// store round trip never stops the loop: offsets advance only after a
// successful batch, so the pass is simply retried with exponential backoff
// until the store recovers.
func (x *projectionRunner) runPass(ctx context.Context) bool {
	shards, err := x.pendingShards(ctx)
	if err != nil {
		return x.retryAfterStoreFailure(ctx, fmt.Errorf("failed to fetch the list of shards: %w", err))
	}

	if len(shards) == 0 {
		x.storeFailures = 0
		return true
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
			return false
		}
	}

	wg.Wait()
	x.wgPool.Put(wg)

	// Drain this batch's worker errors so a failed batch cannot leak stale
	// errors into the next pass. An unprocessable event outranks store
	// failures: it is deterministic, so retrying cannot advance past it.
	var storeErr error
	var eventErr *projectionRunnerError

drain:
	for {
		select {
		case err := <-x.workerErrCh:
			if runnerErr, ok := errors.AsType[*projectionRunnerError](err); ok {
				if eventErr == nil {
					eventErr = runnerErr
				}
				continue
			}
			if storeErr == nil {
				storeErr = err
			}
		default:
			break drain
		}
	}

	if eventErr != nil {
		x.logger.Error(eventErr)
		x.ticker.Stop()
		_ = x.Stop()

		// A failed delivery means the host actor is already stopping, in
		// which case the projection is going down anyway.
		if x.pid != nil {
			_ = goakt.Tell(context.Background(), x.pid, &runnerFailed{err: eventErr})
		}
		return false
	}

	if storeErr != nil {
		return x.retryAfterStoreFailure(ctx, storeErr)
	}

	x.storeFailures = 0

	// A full buffer means more events are pending on some shard: re-pull
	// immediately instead of waiting for the next tick.
	if x.sawFullBatch.Swap(false) {
		x.requestPull()
	}

	return true
}

// retryAfterStoreFailure logs a failed store round trip and waits with
// exponential backoff before the pass loop pulls again. It reports whether
// the loop should keep running: the wait is cut short when the runner stops
// or the context is cancelled.
func (x *projectionRunner) retryAfterStoreFailure(ctx context.Context, err error) bool {
	x.storeFailures++
	delay := storeRetryDelay(x.storeFailures)
	x.logger.Error(fmt.Errorf("projection=(%s) store round trip failed, attempt=%d, retrying in %s: %w", x.name, x.storeFailures, delay, err))

	select {
	case <-x.stopSignal:
		return false
	case <-ctx.Done():
		return false
	case <-time.After(delay):
		return true
	}
}

// storeRetryDelay computes the exponential backoff delay for the nth
// consecutive store failure: min(storeRetryInitialDelay << (n-1),
// storeRetryMaxDelay). Overflowed shifts clamp to the maximum delay.
func storeRetryDelay(failures int) time.Duration {
	delay := storeRetryInitialDelay << (failures - 1)
	if delay <= 0 || delay > storeRetryMaxDelay {
		return storeRetryMaxDelay
	}

	return delay
}

// pendingShards returns the shards whose latest event offset, reported by a
// single ShardOffsets round trip, is past their committed offset. Shards not
// yet in the committed-offset cache are returned unconditionally: doProcess
// resolves their offset on first encounter and an empty read is harmless.
// The returned slice is backed by pendingBuf and only valid until the next
// pass.
func (x *projectionRunner) pendingShards(ctx context.Context) ([]uint64, error) {
	shardOffsets, err := x.eventsStore.ShardOffsets(ctx)
	if err != nil {
		return nil, err
	}

	if len(shardOffsets) == 0 {
		return nil, nil
	}

	shards := x.pendingBuf[:0]

	// A configured starting offset overrides committed offsets on every
	// pull (see currentOffset), so the pending decision must mirror it.
	if !x.startingOffset.IsZero() {
		startOffset := x.startingOffset.UnixMilli()
		for shard, latest := range shardOffsets {
			if latest > startOffset {
				shards = append(shards, shard)
			}
		}
		x.pendingBuf = shards
		return shards, nil
	}

	x.committedOffsetsMu.RLock()
	for shard, latest := range shardOffsets {
		committed, known := x.committedOffsets[shard]
		if !known || latest > committed {
			shards = append(shards, shard)
		}
	}
	x.committedOffsetsMu.RUnlock()

	x.pendingBuf = shards
	return shards, nil
}

// requestPull triggers an immediate pull pass. Safe to call from any
// goroutine; sends coalesce because the nudge channel has capacity one.
func (x *projectionRunner) requestPull() {
	select {
	case x.nudge <- struct{}{}:
	default:
	}
}

// nudgeLoop requests a pull pass whenever events are persisted on this node.
// The pull pass reads events from the journal, so the subscriber queue is
// drained purely for its wake-up signal and the payloads are discarded.
func (x *projectionRunner) nudgeLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-x.streamSubscriber.Ready():
			if !x.streamSubscriber.Active() {
				return
			}
			// Iterator drains the subscriber queue into a snapshot channel;
			// the payloads themselves are discarded.
			if len(x.streamSubscriber.Iterator()) > 0 {
				x.requestPull()
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

	currOffset, err := x.currentOffset(ctx, shard)
	if err != nil {
		return err
	}

	events, nextOffset, err := x.eventsStore.GetShardEvents(ctx, shard, currOffset, uint64(x.maxBufferSize))
	if err != nil {
		return err
	}

	if len(events) >= x.maxBufferSize {
		// A full buffer means more events are pending on this shard.
		x.sawFullBatch.Store(true)
	}

	var attrs attribute.Set
	if x.metrics != nil {
		attrs = attribute.NewSet(
			attribute.String("projection_name", x.name),
			attribute.Int64("shard", int64(shard)),
		)
	}

	if len(events) == 0 {
		// Projection is caught up — reset gauges so Grafana doesn't show stale values.
		if x.metrics != nil {
			x.metrics.projectionLag.Record(ctx, 0, metric.WithAttributeSet(attrs))
			x.metrics.projectionOffset.Record(ctx, currOffset, metric.WithAttributeSet(attrs))
			x.metrics.projectionBehind.Record(ctx, 0, metric.WithAttributeSet(attrs))
		}
		return nil
	}

	// Record projection lag metrics when telemetry is enabled.
	if x.metrics != nil {
		// Use wall-clock lag: how far behind real time the projection is.
		// Event timestamps use time.UnixNano(), so compute lag in nanoseconds
		// and convert to milliseconds for the gauge.
		lagMs := (time.Now().UnixNano() - currOffset) / int64(time.Millisecond)
		if lagMs < 0 || currOffset == 0 {
			lagMs = 0
		}
		x.metrics.projectionLag.Record(ctx, lagMs, metric.WithAttributeSet(attrs))
		x.metrics.projectionOffset.Record(ctx, currOffset, metric.WithAttributeSet(attrs))
		x.metrics.projectionBehind.Record(ctx, int64(len(events)), metric.WithAttributeSet(attrs))
	}

	if err := x.processEvents(ctx, shard, events, nextOffset); err != nil {
		return err
	}

	// A partial buffer means the shard is now caught up. Record that here:
	// caught-up shards are skipped by subsequent pulls, so the gauges would
	// otherwise retain the pre-processing values.
	if x.metrics != nil && len(events) < x.maxBufferSize {
		x.metrics.projectionLag.Record(ctx, 0, metric.WithAttributeSet(attrs))
		x.metrics.projectionOffset.Record(ctx, nextOffset, metric.WithAttributeSet(attrs))
		x.metrics.projectionBehind.Record(ctx, 0, metric.WithAttributeSet(attrs))
	}

	return nil
}

// currentOffset returns the committed offset for a projection shard,
// consulting the offset store only on the first encounter of the shard and
// the in-memory cache afterwards.
func (x *projectionRunner) currentOffset(ctx context.Context, shard uint64) (int64, error) {
	x.committedOffsetsMu.RLock()
	currOffset, cached := x.committedOffsets[shard]
	x.committedOffsetsMu.RUnlock()

	if !cached {
		offset, err := x.offsetsStore.GetCurrentOffset(ctx, &egopb.ProjectionId{
			ProjectionName: x.name,
			ShardNumber:    shard,
		})
		if err != nil {
			return 0, err
		}
		currOffset = offset.GetValue()
		x.committedOffsetsMu.Lock()
		x.committedOffsets[shard] = currOffset
		x.committedOffsetsMu.Unlock()
	}

	if !x.startingOffset.IsZero() {
		currOffset = x.startingOffset.UnixMilli()
	}

	return currOffset, nil
}

// processEvents applies the projection handler to every event of the batch,
// then commits the batch offset once. A failure mid-batch commits nothing, so
// the whole batch is re-pulled on the next pass (at-least-once delivery).
func (x *projectionRunner) processEvents(ctx context.Context, shard uint64, events []*egopb.Event, nextOffset int64) error {
	for _, envelope := range events {
		if err := x.processEnvelope(ctx, envelope); err != nil {
			return &projectionRunnerError{err: err}
		}
	}
	return x.commitOffset(ctx, shard, nextOffset, events[len(events)-1].GetPersistenceId())
}

// processEnvelope handles a single event.
func (x *projectionRunner) processEnvelope(ctx context.Context, envelope *egopb.Event) error {
	event := envelope.GetEvent()
	seqNr := envelope.GetSequenceNumber()
	persistenceID := envelope.GetPersistenceId()

	// Decrypt the event if it was encrypted
	if envelope.GetIsEncrypted() && x.encryptor != nil {
		plaintext, err := x.encryptor.Decrypt(ctx, persistenceID, event.GetValue(), envelope.GetEncryptionKeyId())
		if err != nil {
			return fmt.Errorf("failed to decrypt event for persistence id=%s, revision=%d: %w", persistenceID, seqNr, err)
		}
		decrypted := &anypb.Any{TypeUrl: event.GetTypeUrl()}
		if err := proto.Unmarshal(plaintext, decrypted); err != nil {
			return fmt.Errorf("failed to unmarshal decrypted event for persistence id=%s, revision=%d: %w", persistenceID, seqNr, err)
		}
		event = decrypted
	}

	// apply event adapters
	if len(x.eventAdapters) > 0 {
		adapted, err := eventadapter.Chain(x.eventAdapters, event, seqNr)
		if err != nil {
			return fmt.Errorf("failed to adapt event for persistence id=%s, revision=%d: %w", persistenceID, seqNr, err)
		}
		event = adapted
	}

	if err := x.handleWithPolicy(ctx, persistenceID, event, seqNr); err != nil {
		return err
	}

	if x.metrics != nil {
		x.metrics.projectionHandled.Add(ctx, 1)
	}

	return nil
}

// handleWithPolicy executes the handler with the configured recovery policy.
func (x *projectionRunner) handleWithPolicy(ctx context.Context, persistenceID string, event *anypb.Any, seqNr uint64) error {
	switch x.recovery.RecoveryPolicy() {
	case projection.Fail:
		if err := x.handleSafely(ctx, persistenceID, event, seqNr); err != nil {
			x.logHandlerError(err, persistenceID, seqNr)
			return err
		}
	case projection.RetryAndFail:
		if err := x.retryHandle(ctx, persistenceID, event, seqNr, true); err != nil {
			return err
		}
	case projection.RetryAndSkip:
		if err := x.retryHandle(ctx, persistenceID, event, seqNr, false); err != nil {
			x.sendToDeadLetter(ctx, persistenceID, event, seqNr, err)
		}
	case projection.Skip:
		if err := x.handleSafely(ctx, persistenceID, event, seqNr); err != nil {
			x.logHandlerError(err, persistenceID, seqNr)
			x.sendToDeadLetter(ctx, persistenceID, event, seqNr, err)
		}
	default:
		if err := x.handleSafely(ctx, persistenceID, event, seqNr); err != nil {
			x.logHandlerError(err, persistenceID, seqNr)
			return err
		}
	}

	return nil
}

// retryHandle runs the handler with the pre-created retrier and optional per-attempt logging.
func (x *projectionRunner) retryHandle(ctx context.Context, persistenceID string, event *anypb.Any, seqNr uint64, logEachAttempt bool) error {
	err := x.retrier.Run(func() error {
		handleErr := x.handleSafely(ctx, persistenceID, event, seqNr)
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
func (x *projectionRunner) handleSafely(ctx context.Context, persistenceID string, event *anypb.Any, seqNr uint64) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = newHandlerPanicError(recovered)
		}
	}()

	return x.handler.Handle(ctx, persistenceID, event, seqNr)
}

// sendToDeadLetter forwards a failed event to the dead letter handler if one is configured.
func (x *projectionRunner) sendToDeadLetter(ctx context.Context, persistenceID string, event *anypb.Any, seqNr uint64, cause error) {
	if x.deadLetterHandler == nil {
		return
	}
	if err := x.deadLetterHandler.Handle(ctx, x.name, persistenceID, event, seqNr, cause); err != nil {
		x.logger.Error(fmt.Errorf("dead letter handler failed for persistence id=%s, revision=%d: %w", persistenceID, seqNr, err))
	}
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

	// Keep the committed-offset cache in sync so subsequent pulls skip
	// shards that are already caught up.
	x.committedOffsetsMu.Lock()
	x.committedOffsets[shard] = nextOffset
	x.committedOffsetsMu.Unlock()

	return nil
}

// projectionRunnerError reports an event that cannot be processed: a failed
// decryption, a failed event adaptation, or a handler error under the Fail
// and RetryAndFail recovery policies. Retrying would only replay the same
// event, so the processing loop stops permanently and the host actor
// escalates the error through supervision to make the failure visible.
type projectionRunnerError struct {
	err error
}

// Error returns the underlying runner error message.
func (e *projectionRunnerError) Error() string {
	return e.err.Error()
}

// Unwrap exposes the underlying runner error.
func (e *projectionRunnerError) Unwrap() error {
	return e.err
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
