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
	"maps"
	"os"
	"runtime/debug"
	"slices"
	"strings"
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

	"github.com/tochemey/ego/v5/egopb"
	"github.com/tochemey/ego/v5/encryption"
	"github.com/tochemey/ego/v5/eventadapter"
	"github.com/tochemey/ego/v5/eventstream"
	"github.com/tochemey/ego/v5/internal/ticker"
	"github.com/tochemey/ego/v5/offsetstore"
	"github.com/tochemey/ego/v5/persistence"
	"github.com/tochemey/ego/v5/projection"
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
	// readLag is how far behind the current time the runner reads. An event
	// is stamped just before it is written, so it can still land in the
	// store after a pull already passed its timestamp while the write was in
	// flight. An event is delivered only once it is at least this old, so
	// such an event, or one stamped slightly earlier by a peer node whose
	// clock lags, is still ahead of the committed offset when it becomes
	// visible.
	readLag = 100 * time.Millisecond
)

// frameworkEventPackage is the protobuf package of eGo's own framework events,
// such as the saga status transitions written to a saga journal. They are
// internal bookkeeping and never reach a projection handler. The package is
// taken from the generated descriptor so the check does not depend on the host
// prefix a type URL happens to carry.
var frameworkEventPackage = egopb.File_ego_ego_proto.Package()

// shardOffsetsInvalidator is implemented by an events store that serves
// ShardOffsets from an answer shared across the node. A nudged pull drops that
// answer before reading: a nudge means events were just written on this node,
// and an answer taken before that write would hide them until the next tick.
type shardOffsetsInvalidator interface {
	InvalidateShardOffsets()
}

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

	// workers tracks the worker goroutines started in Start, so that a caller
	// can wait for the last of them to exit. See waitForWorkers.
	workers sync.WaitGroup

	// retrier is pre-created once for RetryAndFail / RetryAndSkip policies to
	// avoid allocating a new retrier on every event.
	retrier *retry.Retrier

	// wgPool pools *sync.WaitGroup for per-batch synchronisation.
	// Proto message pointers (*egopb.EntityOffset) are NOT pooled: the
	// OffsetStore interface methods accept pointer arguments that
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

	// shards holds what the runner knows about every shard it has read: its
	// position, the recently processed persistence IDs and its backtracking
	// progress. A shard's state is loaded from the offset store the first time
	// the shard is read, and dropped after a failed read so the next one starts
	// again from what the offset store holds. The projection runs as a cluster
	// singleton, so this runner is the sole writer of its offsets.
	//
	// shardsMu guards the map only. A shard's state is owned by the one worker
	// reading the shard during a pass, and by the pass loop between passes,
	// while the workers are idle: pendingShards and deleteOldOffsets update it
	// under the read lock for that reason.
	shardsMu sync.RWMutex
	shards   map[uint64]*shardState

	// startFloor is the timestamp the projection starts from on a shard it has
	// no offset on: the later of the configured starting offset and the start
	// offset ResetOffset recorded. Events of a persistence ID whose previous
	// event is older than it are accepted without that previous event.
	startFloor int64

	// offsetSettings governs how offsets are tracked, backtracked and cleaned.
	offsetSettings offsetSettings

	// lastOffsetCleanup is when old offsets were last deleted from the offset store.
	lastOffsetCleanup time.Time

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

	// sawHeldBack reports that a shard batch ended with events younger than
	// readLag during the current pass, in which case the loop pulls again once
	// they have aged instead of waiting for the next tick.
	sawHeldBack atomic.Bool

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
		name:              name,
		logger:            log.NewZap(log.ErrorLevel, os.Stderr),
		handler:           handler,
		eventsStore:       eventsStore,
		offsetsStore:      offsetStore,
		recovery:          projection.NewRecovery(),
		stopSignal:        make(chan struct{}, 1),
		running:           atomic.NewBool(false),
		pullInterval:      time.Second,
		maxBufferSize:     500,
		startingOffset:    ZeroTime,
		resetOffsetTo:     ZeroTime,
		shards:            make(map[uint64]*shardState),
		offsetSettings:    defaultOffsetSettings(),
		lastOffsetCleanup: time.Now(),
		nudge:             make(chan struct{}, 1),
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

	x.workers.Add(numWorkers)
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
// stream, by a full-buffer read reporting that more events are pending, or by
// the read lag elapsing for events that were held back.
func (x *projectionRunner) processingLoop(ctx context.Context) {
	for {
		select {
		case <-x.stopSignal:
			return
		case <-ctx.Done():
			return
		case <-x.ticker.Ticks:
		case <-x.nudge:
			x.invalidateShardOffsets()
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
		// an idle projection still cleans its old offsets up
		x.deleteOldOffsets(ctx)
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

	// Events held back for being younger than readLag are deliverable once
	// they have aged: pull again then rather than at the next tick, so events
	// persisted on this node keep their low latency.
	if x.sawHeldBack.Swap(false) {
		time.AfterFunc(readLag, x.requestPull)
	}

	x.deleteOldOffsets(ctx)
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

// pendingShards returns the shards to read in this pass: a shard the runner
// has not read yet, a shard whose newest event, reported by a single
// ShardOffsets round trip, is past the shard's position, a shard whose last
// read filled the buffer, and a shard that is due for a backtracking read.
// The returned slice is backed by pendingBuf and only valid until the next
// pass.
func (x *projectionRunner) pendingShards(ctx context.Context) ([]uint64, error) {
	shardOffsets, err := x.eventsStore.ShardOffsets(ctx)
	if err != nil {
		return nil, err
	}

	shards := x.pendingBuf[:0]
	now := time.Now()

	// Passes are serialized and the workers are idle while this runs, so the
	// shard states can be updated here without racing a read of the shard.
	x.shardsMu.RLock()
	for shard, newest := range shardOffsets {
		state, known := x.shards[shard]
		if !known || newest > state.latest || state.lastReadFull || state.backtracking {
			shards = append(shards, shard)
			continue
		}

		state.idlePasses++
		if state.shouldBacktrack(x.offsetSettings, x.maxBufferSize, now) {
			state.backtracking = true
			shards = append(shards, shard)
		}
	}
	x.shardsMu.RUnlock()

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
	defer x.workers.Done()

	for {
		select {
		case item, ok := <-x.workCh:
			if !ok {
				return
			}
			if err := x.processShard(ctx, item.shard); err != nil && ctx.Err() == nil {
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

// processShard reads one shard: from its position in a normal read, or behind
// it when the shard is backtracking. A failed read drops the shard's state, so
// the next read starts again from what the offset store holds; every event
// processed before the failure was recorded there already.
func (x *projectionRunner) processShard(ctx context.Context, shard uint64) error {
	if !x.running.Load() {
		return nil
	}

	state, err := x.shardState(ctx, shard)
	if err != nil {
		return err
	}

	if state.backtracking {
		err = x.backtrack(ctx, shard, state)
	}

	// A shard that is done backtracking reads from its position right away,
	// so new events do not wait for the next pass.
	if err == nil && !state.backtracking {
		err = x.readShard(ctx, shard, state)
	}

	if err != nil {
		x.dropShardState(shard)
	}

	return err
}

// shardState returns the state of the shard, loading it from the offset store
// the first time the shard is read.
func (x *projectionRunner) shardState(ctx context.Context, shard uint64) (*shardState, error) {
	x.shardsMu.RLock()
	state, ok := x.shards[shard]
	x.shardsMu.RUnlock()

	if ok {
		return state, nil
	}

	offsets, err := x.offsetsStore.ReadOffsets(ctx, x.name, shard, offsetReadLimit)
	if err != nil {
		return nil, err
	}

	state = newShardState(offsets, x.startFloor, x.offsetSettings.timeWindow)

	x.shardsMu.Lock()
	x.shards[shard] = state
	x.shardsMu.Unlock()

	return state, nil
}

// dropShardState forgets the state of the shard.
func (x *projectionRunner) dropShardState(shard uint64) {
	x.shardsMu.Lock()
	delete(x.shards, shard)
	x.shardsMu.Unlock()
}

// readShard reads the events of the shard from its position on, at most a
// buffer of them, and processes those at least readLag old.
//
// The read starts at the position's timestamp itself: several events can share
// a timestamp, and the projection may have stopped among them. Those it has
// already processed are recognized as duplicates.
func (x *projectionRunner) readShard(ctx context.Context, shard uint64, state *shardState) error {
	from := state.latest
	events, full, err := x.readEvents(ctx, shard, state, from)
	if err != nil {
		return err
	}

	if full {
		// A full buffer means more events are pending on this shard.
		x.sawFullBatch.Store(true)
	}

	state.lastReadFull = full
	events = x.settledEvents(events)
	if len(events) > 0 {
		state.idlePasses = 0
		state.readSinceBacktracking += len(events)
	}

	x.recordLag(ctx, shard, from, len(events))

	if err := x.processEvents(ctx, shard, state, events); err != nil {
		return err
	}

	if state.shouldBacktrack(x.offsetSettings, x.maxBufferSize, time.Now()) {
		state.backtracking = true
	}

	if !full {
		// The shard is caught up: reset the gauges, which would otherwise keep
		// the values recorded before the read.
		x.recordLag(ctx, shard, state.latest, 0)
	}

	return nil
}

// backtrack reads the shard again behind its position, to process the events
// that became visible after the projection had read past their timestamp. Most
// events it reads were processed already and are skipped as duplicates. The
// shard stays in backtracking until a read no longer fills the buffer.
func (x *projectionRunner) backtrack(ctx context.Context, shard uint64, state *shardState) error {
	from, to := state.backtrackingRange(x.offsetSettings, time.Now())
	if from > to {
		state.stopBacktracking()
		return nil
	}

	events, full, err := x.readEvents(ctx, shard, state, from)
	if err != nil {
		return err
	}

	inRange := eventsUpTo(events, to)
	if err := x.processEvents(ctx, shard, state, inRange); err != nil {
		return err
	}

	if full && len(inRange) == len(events) {
		// More events are within the range: carry on at the next pass.
		state.latestBacktracking = inRange[len(inRange)-1].GetTimestamp()
		x.sawFullBatch.Store(true)
		return nil
	}

	state.latestBacktracking = to
	state.stopBacktracking()
	return nil
}

// readEvents reads at most a buffer of events of the shard, from the timestamp
// on, and reports whether more events are pending on the shard past the
// buffer: one more event than the buffer holds is read for that. A read that
// cannot make progress fails (see checkProgress).
func (x *projectionRunner) readEvents(ctx context.Context, shard uint64, state *shardState, from int64) ([]*egopb.Event, bool, error) {
	events, err := x.eventsStore.EventsByShard(ctx, shard, from, uint64(x.maxBufferSize)+1)
	if err != nil {
		return nil, false, err
	}

	full := len(events) > x.maxBufferSize
	if full {
		events = events[:x.maxBufferSize]
	}

	if err := x.checkProgress(shard, state, events, from, full); err != nil {
		return nil, false, err
	}

	return events, full, nil
}

// checkProgress fails a read that cannot make progress: a full buffer of
// events all stamped at the timestamp the read started from, all processed
// already, with more events pending past them. The next read would start there
// and return the same events again, so the projection could never get past
// them: the events sharing that timestamp fill the buffer.
func (x *projectionRunner) checkProgress(shard uint64, state *shardState, events []*egopb.Event, from int64, full bool) error {
	if !full {
		return nil
	}

	for _, envelope := range events {
		if envelope.GetTimestamp() != from {
			return nil
		}

		record, known := state.entities[envelope.GetPersistenceId()]
		if !known || record.check(envelope.GetSequenceNumber()) != duplicate {
			return nil
		}
	}

	return &projectionRunnerError{err: fmt.Errorf("projection=(%s) found %d events on shard %d with the timestamp %d, filling its buffer: increase the projection's buffer size",
		x.name, x.maxBufferSize, shard, from)}
}

// settledEvents drops the tail of a read that is younger than readLag and
// tells the pass loop so: an event is processed only once it is at least that
// old (see readLag).
func (x *projectionRunner) settledEvents(events []*egopb.Event) []*egopb.Event {
	settled := eventsUpTo(events, time.Now().Add(-readLag).UnixNano())
	if len(settled) < len(events) {
		x.sawHeldBack.Store(true)
	}

	return settled
}

// eventsUpTo returns the events of a read stamped at or before the timestamp.
// A read is ordered by timestamp, so the events after it form its tail.
func eventsUpTo(events []*egopb.Event, timestamp int64) []*egopb.Event {
	end := len(events)
	for end > 0 && events[end-1].GetTimestamp() > timestamp {
		end--
	}

	return events[:end]
}

// processEvents processes the events of one read in order, then records the
// last processed event of every persistence ID in the offset store in a single
// write. When an event cannot be processed, the events processed before it are
// still recorded, and the error is returned.
func (x *projectionRunner) processEvents(ctx context.Context, shard uint64, state *shardState, events []*egopb.Event) error {
	offsets := make(map[string]*egopb.EntityOffset)
	consumedAt := time.Now().UnixNano()

	var failure error
	for _, envelope := range events {
		if failure = x.consume(ctx, shard, state, envelope, offsets, consumedAt); failure != nil {
			break
		}
	}

	if err := x.writeOffsets(ctx, shard, state, offsets); err != nil {
		return errors.Join(failure, err)
	}

	state.evict(x.offsetSettings.timeWindow)
	return failure
}

// consume validates an event against the offsets of its persistence ID and
// processes it accordingly: an accepted event is delivered, a duplicate is
// skipped, and an event after a gap makes the projection replay the missing
// events of its persistence ID first.
func (x *projectionRunner) consume(ctx context.Context, shard uint64, state *shardState, envelope *egopb.Event, offsets map[string]*egopb.EntityOffset, consumedAt int64) error {
	outcome, err := x.validate(ctx, shard, state, envelope)
	if err != nil {
		return err
	}

	switch outcome {
	case accepted:
		return x.deliver(ctx, shard, state, envelope, offsets, consumedAt)
	case duplicate:
		return nil
	default:
		return x.replay(ctx, shard, state, envelope, offsets, consumedAt)
	}
}

// validate checks an event against the last processed event of its
// persistence ID, which is loaded from the offset store when the projection
// does not hold it in memory.
//
// An event of a persistence ID the projection has no offset for is accepted
// when it is the persistence ID's first event. A later one is accepted only when
// its previous event no longer exists or is older than the point the
// projection keeps offsets for, or than the point it started from: otherwise
// that previous event has not been processed yet.
func (x *projectionRunner) validate(ctx context.Context, shard uint64, state *shardState, envelope *egopb.Event) (validation, error) {
	persistenceID := envelope.GetPersistenceId()
	sequenceNumber := envelope.GetSequenceNumber()

	if record, known := state.entities[persistenceID]; known {
		return record.check(sequenceNumber), nil
	}

	offset, err := x.offsetsStore.ReadEntityOffset(ctx, x.name, shard, persistenceID)
	if err != nil {
		return rejected, err
	}

	if offset != nil {
		record := entityRecord{sequenceNumber: offset.GetSequenceNumber(), timestamp: offset.GetTimestamp()}
		state.entities[persistenceID] = record
		return record.check(sequenceNumber), nil
	}

	if sequenceNumber == 1 {
		return accepted, nil
	}

	previous, err := x.eventsStore.ReplayEvents(ctx, persistenceID, sequenceNumber-1, sequenceNumber-1, 1)
	if err != nil {
		return rejected, err
	}

	acceptBefore := max(state.latest-x.offsetSettings.deleteAfter.Nanoseconds(), x.startFloor)
	if len(previous) == 0 || previous[0].GetTimestamp() < acceptBefore {
		return accepted, nil
	}

	return rejected, nil
}

// deliver hands an accepted event to the handler and records it as processed.
// eGo's own framework events and events stamped before the point the
// projection starts from are recorded without reaching the handler: they still
// take a sequence number of their persistence ID.
func (x *projectionRunner) deliver(ctx context.Context, shard uint64, state *shardState, envelope *egopb.Event, offsets map[string]*egopb.EntityOffset, consumedAt int64) error {
	if envelope.GetTimestamp() >= x.startFloor {
		if err := x.processEnvelope(ctx, envelope); err != nil {
			return &projectionRunnerError{err: err}
		}
	}

	persistenceID := envelope.GetPersistenceId()
	state.remember(persistenceID, envelope.GetSequenceNumber(), envelope.GetTimestamp())
	offsets[persistenceID] = &egopb.EntityOffset{
		ShardNumber:    shard,
		PersistenceId:  persistenceID,
		SequenceNumber: envelope.GetSequenceNumber(),
		Timestamp:      envelope.GetTimestamp(),
		ConsumedAt:     consumedAt,
	}

	return nil
}

// replay delivers, in sequence order, the events of a persistence ID from the
// one after its last processed event up to the given event, which came after
// a gap. When the events store does not return every one of them, the read
// fails and is retried with backoff: a missing event is never skipped.
func (x *projectionRunner) replay(ctx context.Context, shard uint64, state *shardState, envelope *egopb.Event, offsets map[string]*egopb.EntityOffset, consumedAt int64) error {
	persistenceID := envelope.GetPersistenceId()
	to := envelope.GetSequenceNumber()

	from := uint64(1)
	if record, ok := state.entities[persistenceID]; ok {
		from = record.sequenceNumber + 1
	}

	expected := to - from + 1
	events, err := x.eventsStore.ReplayEvents(ctx, persistenceID, from, to, expected)
	if err != nil {
		return err
	}

	if uint64(len(events)) != expected {
		return fmt.Errorf("projection=(%s) cannot process event %d of %s: %d of its events %d to %d are in the events store",
			x.name, to, persistenceID, len(events), from, to)
	}

	for _, replayed := range events {
		outcome, err := x.validate(ctx, shard, state, replayed)
		if err != nil {
			return err
		}

		if outcome != accepted {
			return fmt.Errorf("projection=(%s) cannot process event %d of %s: its replay is out of sequence",
				x.name, replayed.GetSequenceNumber(), persistenceID)
		}

		if err := x.deliver(ctx, shard, state, replayed, offsets, consumedAt); err != nil {
			return err
		}
	}

	return nil
}

// writeOffsets records the given offsets of the shard in the offset store in
// a single write.
func (x *projectionRunner) writeOffsets(ctx context.Context, shard uint64, state *shardState, offsets map[string]*egopb.EntityOffset) error {
	if len(offsets) == 0 {
		return nil
	}

	batch := slices.SortedFunc(maps.Values(offsets), func(left, right *egopb.EntityOffset) int {
		return strings.Compare(left.GetPersistenceId(), right.GetPersistenceId())
	})

	if err := x.offsetsStore.WriteOffsets(ctx, x.name, batch); err != nil {
		return fmt.Errorf("failed to persist the offsets of shard %d: %w", shard, err)
	}

	state.dirty = true
	return nil
}

// recordLag records the projection gauges of the shard when telemetry is
// enabled: how far behind the current time the read started, where it
// started, and how many events it returned.
func (x *projectionRunner) recordLag(ctx context.Context, shard uint64, from int64, pending int) {
	if x.metrics == nil {
		return
	}

	attrs := attribute.NewSet(
		attribute.String("projection_name", x.name),
		attribute.Int64("shard", int64(shard)),
	)

	lagMs := int64(0)
	if pending > 0 && from > 0 {
		lagMs = max((time.Now().UnixNano()-from)/int64(time.Millisecond), 0)
	}

	x.metrics.projectionLag.Record(ctx, lagMs, metric.WithAttributeSet(attrs))
	x.metrics.projectionOffset.Record(ctx, from, metric.WithAttributeSet(attrs))
	x.metrics.projectionBehind.Record(ctx, int64(pending), metric.WithAttributeSet(attrs))
}

// deleteOldOffsets deletes, at most once per delete interval, the offsets of
// every shard written since the previous deletion that are older than the
// delete window behind the shard's position and were recorded longer ago than
// the consumed window. The newest offset of a shard is never older than its
// position, so it is always kept. A failed deletion is logged and retried at
// the next interval.
func (x *projectionRunner) deleteOldOffsets(ctx context.Context) {
	if time.Since(x.lastOffsetCleanup) < x.offsetSettings.deleteInterval {
		return
	}

	x.lastOffsetCleanup = time.Now()
	consumedBefore := time.Now().Add(-x.offsetSettings.deleteAfterConsumed).UnixNano()

	x.shardsMu.RLock()
	defer x.shardsMu.RUnlock()

	for shard, state := range x.shards {
		if !state.dirty {
			continue
		}

		before := state.latest - x.offsetSettings.deleteAfter.Nanoseconds()
		if err := x.offsetsStore.DeleteOffsetsBefore(ctx, x.name, shard, before, consumedBefore); err != nil {
			x.logger.Error(fmt.Errorf("projection=(%s) failed to delete old offsets of shard %d: %w", x.name, shard, err))
			continue
		}

		state.dirty = false
	}
}

// processEnvelope handles a single event.
//
// eGo's own framework events are skipped before any decryption or adaptation
// happens: they carry no domain meaning. The batch offset is committed by the
// caller, so skipped events do not stall the projection.
func (x *projectionRunner) processEnvelope(ctx context.Context, envelope *egopb.Event) error {
	event := envelope.GetEvent()
	seqNr := envelope.GetSequenceNumber()
	persistenceID := envelope.GetPersistenceId()

	if event.MessageName().Parent() == frameworkEventPackage {
		x.logger.Debugf("projection=(%s) skipped framework event=[persistenceID=%s, revision=%d, type=%s]", x.name, persistenceID, seqNr, event.MessageName())
		return nil
	}

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

// preStart is used to perform some tasks before the projection starts.
//
// Offsets are event timestamps in nanoseconds, so a configured reset time is
// written in the unit the events store compares its events against.
func (x *projectionRunner) preStart(ctx context.Context) error {
	if !x.resetOffsetTo.IsZero() {
		if err := x.offsetsStore.ResetOffset(ctx, x.name, x.resetOffsetTo.UnixNano()); err != nil {
			fmtErr := fmt.Errorf("failed to reset projection=%s: %w", x.name, err)
			x.logger.Error(fmtErr)
			return fmtErr
		}
	}

	startOffset, err := x.offsetsStore.StartOffset(ctx, x.name)
	if err != nil {
		fmtErr := fmt.Errorf("failed to read the start offset of projection=%s: %w", x.name, err)
		x.logger.Error(fmtErr)
		return fmtErr
	}

	x.startFloor = startOffset
	if !x.startingOffset.IsZero() {
		x.startFloor = max(x.startFloor, x.startingOffset.UnixNano())
	}

	return nil
}

// invalidateShardOffsets drops the events store's shared shard-offsets answer
// when the store keeps one, so a nudged pull reads the journal's current state
// rather than an answer taken before the write that nudged it.
func (x *projectionRunner) invalidateShardOffsets() {
	if invalidator, ok := x.eventsStore.(shardOffsetsInvalidator); ok {
		invalidator.InvalidateShardOffsets()
	}
}

// waitForWorkers blocks until every worker goroutine of the pool has exited.
//
// Stop only signals the pool: a worker that is already processing a batch runs
// it to the end and can still commit an offset after Stop has returned.
// Anything that must happen after the runner's very last commit waits here
// first. A runner that was never started returns immediately.
func (x *projectionRunner) waitForWorkers() {
	x.workers.Wait()
}
