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

package benchmark

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/tochemey/ego/v4"
	"github.com/tochemey/ego/v4/egopb"
	samplepb "github.com/tochemey/ego/v4/example/examplepb"
	"github.com/tochemey/ego/v4/persistence"
	"github.com/tochemey/ego/v4/testkit"
)

// ---------------------------------------------------------------------------
// Latency-simulating event store wrapper
// ---------------------------------------------------------------------------

// slowEventsStore wraps a real EventsStore and adds a configurable delay on
// WriteEvents. This simulates the I/O cost of a real database (Postgres, etc.)
// and lets us measure how batch mode amortises that cost.
type slowEventsStore struct {
	persistence.EventsStore
	writeDelay time.Duration
}

func newSlowEventsStore(delegate persistence.EventsStore, writeDelay time.Duration) *slowEventsStore {
	return &slowEventsStore{EventsStore: delegate, writeDelay: writeDelay}
}

func (s *slowEventsStore) WriteEvents(ctx context.Context, events []*egopb.Event) error {
	if s.writeDelay > 0 {
		time.Sleep(s.writeDelay)
	}
	return s.EventsStore.WriteEvents(ctx, events)
}

// ---------------------------------------------------------------------------
// Behavior
// ---------------------------------------------------------------------------

// accountBehavior implements ego.EventSourcedBehavior for benchmarks.
type accountBehavior struct {
	id string
}

var _ ego.EventSourcedBehavior = (*accountBehavior)(nil)

func newAccountBehavior(id string) *accountBehavior {
	return &accountBehavior{id: id}
}

func (a *accountBehavior) ID() string { return a.id }

func (a *accountBehavior) InitialState() ego.State {
	return new(samplepb.Account)
}

func (a *accountBehavior) HandleCommand(_ context.Context, command ego.Command, _ ego.State) ([]ego.Event, error) {
	switch cmd := command.(type) {
	case *samplepb.CreateAccount:
		return []ego.Event{
			&samplepb.AccountCreated{
				AccountId:      cmd.GetAccountId(),
				AccountBalance: cmd.GetAccountBalance(),
			},
		}, nil
	case *samplepb.CreditAccount:
		if cmd.GetAccountId() == a.id {
			return []ego.Event{
				&samplepb.AccountCredited{
					AccountId:      cmd.GetAccountId(),
					AccountBalance: cmd.GetBalance(),
				},
			}, nil
		}
		return nil, errors.New("command sent to the wrong entity")
	default:
		return nil, errors.New("unhandled command")
	}
}

func (a *accountBehavior) HandleEvent(_ context.Context, event ego.Event, priorState ego.State) (ego.State, error) {
	switch evt := event.(type) {
	case *samplepb.AccountCreated:
		return &samplepb.Account{
			AccountId:      evt.GetAccountId(),
			AccountBalance: evt.GetAccountBalance(),
		}, nil
	case *samplepb.AccountCredited:
		account := priorState.(*samplepb.Account)
		bal := account.GetAccountBalance() + evt.GetAccountBalance()
		return &samplepb.Account{
			AccountId:      evt.GetAccountId(),
			AccountBalance: bal,
		}, nil
	default:
		return nil, errors.New("unhandled event")
	}
}

func (a *accountBehavior) MarshalBinary() ([]byte, error) {
	return json.Marshal(struct {
		ID string `json:"id"`
	}{ID: a.id})
}

func (a *accountBehavior) UnmarshalBinary(data []byte) error {
	aux := struct {
		ID string `json:"id"`
	}{}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	a.id = aux.ID
	return nil
}

// ---------------------------------------------------------------------------
// Setup helpers
// ---------------------------------------------------------------------------

// benchEnv holds the shared infrastructure for a benchmark run.
type benchEnv struct {
	ctx    context.Context
	engine *ego.Engine
}

// setupEngine creates an ego Engine backed by the given event store.
func setupEngine(b *testing.B, store persistence.EventsStore) *benchEnv {
	b.Helper()
	ctx := context.Background()

	engine := ego.NewEngine("BenchEngine", store, ego.WithLogger(ego.DiscardLogger))
	if err := engine.Start(ctx); err != nil {
		b.Fatalf("start engine: %v", err)
	}

	time.Sleep(time.Second)

	b.Cleanup(func() {
		_ = engine.Stop(ctx)
	})

	return &benchEnv{ctx: ctx, engine: engine}
}

// connectStore creates and connects an in-memory event store.
func connectStore(b *testing.B) *testkit.EventStore {
	b.Helper()
	ctx := context.Background()
	store := testkit.NewEventsStore()
	if err := store.Connect(ctx); err != nil {
		b.Fatalf("connect event store: %v", err)
	}
	b.Cleanup(func() { _ = store.Disconnect(ctx) })
	return store
}

// spawnEntity registers a non-batched event sourced entity and initialises it.
func (e *benchEnv) spawnEntity(b *testing.B) string {
	b.Helper()
	entityID := uuid.NewString()
	behavior := newAccountBehavior(entityID)

	if err := e.engine.Entity(e.ctx, behavior); err != nil {
		b.Fatalf("create entity: %v", err)
	}

	time.Sleep(time.Second)

	_, _, err := e.engine.SendCommand(e.ctx, entityID, &samplepb.CreateAccount{
		AccountId:      entityID,
		AccountBalance: 1000.00,
	}, 5*time.Second)
	if err != nil {
		b.Fatalf("init account: %v", err)
	}

	return entityID
}

// spawnBatchEntity registers a batched event sourced entity and initialises it.
func (e *benchEnv) spawnBatchEntity(b *testing.B, threshold int, flushWindow time.Duration) string {
	b.Helper()
	entityID := uuid.NewString()
	behavior := newAccountBehavior(entityID)

	if err := e.engine.Entity(e.ctx, behavior,
		ego.WithBatchThreshold(threshold),
		ego.WithBatchFlushWindow(flushWindow),
	); err != nil {
		b.Fatalf("create batch entity: %v", err)
	}

	time.Sleep(time.Second)

	_, _, err := e.engine.SendCommand(e.ctx, entityID, &samplepb.CreateAccount{
		AccountId:      entityID,
		AccountBalance: 1000.00,
	}, 5*time.Second)
	if err != nil {
		b.Fatalf("init account: %v", err)
	}

	return entityID
}

// ---------------------------------------------------------------------------
// Latency recorder — collects per-command latencies for percentile reporting.
// ---------------------------------------------------------------------------

type latencyRecorder struct {
	mu        sync.Mutex
	samples   []time.Duration
	failures  atomic.Int64
	wallStart time.Time
	wallEnd   time.Time
}

func newLatencyRecorder(sizeHint int) *latencyRecorder {
	return &latencyRecorder{
		samples:   make([]time.Duration, 0, sizeHint),
		wallStart: time.Now(),
	}
}

func (r *latencyRecorder) record(d time.Duration) {
	r.mu.Lock()
	r.samples = append(r.samples, d)
	r.mu.Unlock()
}

func (r *latencyRecorder) recordFailure() {
	r.failures.Add(1)
}

// stop marks the wall-clock end time.
func (r *latencyRecorder) stop() {
	r.wallEnd = time.Now()
}

// report computes and logs human-readable metrics on the benchmark.
func (r *latencyRecorder) report(b *testing.B, label string) {
	r.mu.Lock()
	data := make([]time.Duration, len(r.samples))
	copy(data, r.samples)
	r.mu.Unlock()

	n := len(data)
	if n == 0 {
		b.Logf("[%s] no samples collected", label)
		return
	}

	sort.Slice(data, func(i, j int) bool { return data[i] < data[j] })

	total := time.Duration(0)
	for _, d := range data {
		total += d
	}

	avg := total / time.Duration(n)
	p50 := data[percentileIndex(n, 50)]
	p90 := data[percentileIndex(n, 90)]
	p95 := data[percentileIndex(n, 95)]
	p99 := data[percentileIndex(n, 99)]
	minL := data[0]
	maxL := data[n-1]

	wallElapsed := r.wallEnd.Sub(r.wallStart)
	if wallElapsed <= 0 {
		wallElapsed = total // fallback
	}
	wallCmdsPerSec := float64(n) / wallElapsed.Seconds()

	// Compute memory snapshot
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	heapMB := float64(memStats.HeapInuse) / (1024 * 1024)
	totalAllocMB := float64(memStats.TotalAlloc) / (1024 * 1024)

	// Custom metrics visible in the standard benchmark output line
	b.ReportMetric(wallCmdsPerSec, "cmds/s")
	b.ReportMetric(heapMB, "heap-MB")
	b.ReportMetric(totalAllocMB, "total-alloc-MB")
	b.ReportMetric(float64(p50.Microseconds()), "p50-µs")
	b.ReportMetric(float64(p99.Microseconds()), "p99-µs")
	b.ReportMetric(float64(r.failures.Load()), "errors")

	// Human-readable summary logged to test output (visible with -v)
	b.Logf("\n"+
		"╔══════════════════════════════════════════════════════════════╗\n"+
		"║  %s\n"+
		"╠══════════════════════════════════════════════════════════════╣\n"+
		"║  Total commands     : %d\n"+
		"║  Errors             : %d\n"+
		"║  Wall-clock time    : %s\n"+
		"║  Throughput         : %.0f commands/sec\n"+
		"╠══════════════════════════════════════════════════════════════╣\n"+
		"║  Latency per command:\n"+
		"║    min   = %12s\n"+
		"║    avg   = %12s\n"+
		"║    p50   = %12s\n"+
		"║    p90   = %12s\n"+
		"║    p95   = %12s\n"+
		"║    p99   = %12s\n"+
		"║    max   = %12s\n"+
		"╠══════════════════════════════════════════════════════════════╣\n"+
		"║  Memory:\n"+
		"║    heap in-use  = %s\n"+
		"║    heap objects = %d\n"+
		"║    total alloc  = %s\n"+
		"║    GC cycles    = %d\n"+
		"╚══════════════════════════════════════════════════════════════╝",
		label,
		n,
		r.failures.Load(),
		wallElapsed.Round(time.Millisecond),
		wallCmdsPerSec,
		minL, avg, p50, p90, p95, p99, maxL,
		formatBytes(memStats.HeapInuse),
		memStats.HeapObjects,
		formatBytes(memStats.TotalAlloc),
		memStats.NumGC,
	)
}

func percentileIndex(n, pct int) int {
	idx := (n * pct / 100) - 1
	if idx < 0 {
		return 0
	}
	return idx
}

func formatBytes(b uint64) string {
	const (
		kb = 1024
		mb = kb * 1024
		gb = mb * 1024
	)
	switch {
	case b >= gb:
		return fmt.Sprintf("%.2f GB", float64(b)/float64(gb))
	case b >= mb:
		return fmt.Sprintf("%.2f MB", float64(b)/float64(mb))
	case b >= kb:
		return fmt.Sprintf("%.2f KB", float64(b)/float64(kb))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

// ---------------------------------------------------------------------------
// Benchmarks — In-memory store (baseline, no I/O)
// ---------------------------------------------------------------------------

// BenchmarkEventSourcedActor_NoBatch measures sequential command throughput
// without batch processing. Each command triggers a synchronous persist.
func BenchmarkEventSourcedActor_NoBatch(b *testing.B) {
	store := connectStore(b)
	env := setupEngine(b, store)
	entityID := env.spawnEntity(b)

	cmd := &samplepb.CreditAccount{AccountId: entityID, Balance: 1.00}
	rec := newLatencyRecorder(b.N)

	b.ResetTimer()
	b.ReportAllocs()
	rec.wallStart = time.Now()

	for b.Loop() {
		start := time.Now()
		_, _, err := env.engine.SendCommand(env.ctx, entityID, cmd, 5*time.Second)
		rec.record(time.Since(start))
		if err != nil {
			rec.recordFailure()
			b.Fatalf("send command: %v", err)
		}
	}

	rec.stop()
	b.StopTimer()
	rec.report(b, "NoBatch | Sequential | in-memory store")
}

// BenchmarkEventSourcedActor_Batch measures sequential command throughput
// with batch processing. With sequential sends each command waits for the
// flush window — batch mode is designed for concurrent load.
func BenchmarkEventSourcedActor_Batch(b *testing.B) {
	store := connectStore(b)
	env := setupEngine(b, store)
	entityID := env.spawnBatchEntity(b, 10, 5*time.Millisecond)

	cmd := &samplepb.CreditAccount{AccountId: entityID, Balance: 1.00}
	rec := newLatencyRecorder(b.N)

	b.ResetTimer()
	b.ReportAllocs()
	rec.wallStart = time.Now()

	for b.Loop() {
		start := time.Now()
		_, _, err := env.engine.SendCommand(env.ctx, entityID, cmd, 5*time.Second)
		rec.record(time.Since(start))
		if err != nil {
			rec.recordFailure()
			b.Fatalf("send command: %v", err)
		}
	}

	rec.stop()
	b.StopTimer()
	rec.report(b, "Batch(10, 5ms) | Sequential | in-memory store")
}

// BenchmarkEventSourcedActor_NoBatch_Parallel measures concurrent command
// throughput without batch processing.
func BenchmarkEventSourcedActor_NoBatch_Parallel(b *testing.B) {
	store := connectStore(b)
	env := setupEngine(b, store)
	entityID := env.spawnEntity(b)

	cmd := &samplepb.CreditAccount{AccountId: entityID, Balance: 1.00}
	rec := newLatencyRecorder(b.N)

	b.ResetTimer()
	b.ReportAllocs()
	rec.wallStart = time.Now()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			start := time.Now()
			_, _, err := env.engine.SendCommand(env.ctx, entityID, cmd, 5*time.Second)
			rec.record(time.Since(start))
			if err != nil {
				rec.recordFailure()
			}
		}
	})

	rec.stop()
	b.StopTimer()
	rec.report(b, fmt.Sprintf("NoBatch | Parallel(%d goroutines) | in-memory store", runtime.GOMAXPROCS(0)))
}

// BenchmarkEventSourcedActor_Batch_Parallel measures concurrent command
// throughput with batch processing.
func BenchmarkEventSourcedActor_Batch_Parallel(b *testing.B) {
	store := connectStore(b)
	env := setupEngine(b, store)
	entityID := env.spawnBatchEntity(b, 10, 5*time.Millisecond)

	cmd := &samplepb.CreditAccount{AccountId: entityID, Balance: 1.00}
	rec := newLatencyRecorder(b.N)

	b.ResetTimer()
	b.ReportAllocs()
	rec.wallStart = time.Now()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			start := time.Now()
			_, _, err := env.engine.SendCommand(env.ctx, entityID, cmd, 5*time.Second)
			rec.record(time.Since(start))
			if err != nil {
				rec.recordFailure()
			}
		}
	})

	rec.stop()
	b.StopTimer()
	rec.report(b, fmt.Sprintf("Batch(10, 5ms) | Parallel(%d goroutines) | in-memory store", runtime.GOMAXPROCS(0)))
}

// ---------------------------------------------------------------------------
// Benchmarks — Simulated I/O latency
//
// These benchmarks add a write delay to the event store, simulating real
// database latency. This is where batch mode shows its value: it amortises
// a single expensive write across many commands.
// ---------------------------------------------------------------------------

// BenchmarkWriteLatency_NoBatch_vs_Batch compares NoBatch and Batch at various
// simulated write latencies. This is the key benchmark for choosing whether to
// enable batching and at what threshold.
func BenchmarkWriteLatency_NoBatch_vs_Batch(b *testing.B) {
	latencies := []time.Duration{
		100 * time.Microsecond,
		500 * time.Microsecond,
		1 * time.Millisecond,
		5 * time.Millisecond,
	}

	for _, latency := range latencies {
		latLabel := latency.String()

		b.Run(fmt.Sprintf("write_%s/NoBatch", latLabel), func(b *testing.B) {
			base := connectStore(b)
			store := newSlowEventsStore(base, latency)
			env := setupEngine(b, store)
			entityID := env.spawnEntity(b)

			cmd := &samplepb.CreditAccount{AccountId: entityID, Balance: 1.00}
			rec := newLatencyRecorder(b.N)

			b.ResetTimer()
			b.ReportAllocs()
			rec.wallStart = time.Now()

			for b.Loop() {
				start := time.Now()
				_, _, err := env.engine.SendCommand(env.ctx, entityID, cmd, 5*time.Second)
				rec.record(time.Since(start))
				if err != nil {
					rec.recordFailure()
					b.Fatalf("send command: %v", err)
				}
			}

			rec.stop()
			b.StopTimer()
			rec.report(b, fmt.Sprintf("NoBatch | Sequential | write delay=%s", latLabel))
		})

		b.Run(fmt.Sprintf("write_%s/Batch_threshold10", latLabel), func(b *testing.B) {
			base := connectStore(b)
			store := newSlowEventsStore(base, latency)
			env := setupEngine(b, store)
			entityID := env.spawnBatchEntity(b, 10, 5*time.Millisecond)

			cmd := &samplepb.CreditAccount{AccountId: entityID, Balance: 1.00}
			rec := newLatencyRecorder(b.N)

			b.ResetTimer()
			b.ReportAllocs()
			rec.wallStart = time.Now()

			for b.Loop() {
				start := time.Now()
				_, _, err := env.engine.SendCommand(env.ctx, entityID, cmd, 5*time.Second)
				rec.record(time.Since(start))
				if err != nil {
					rec.recordFailure()
					b.Fatalf("send command: %v", err)
				}
			}

			rec.stop()
			b.StopTimer()
			rec.report(b, fmt.Sprintf("Batch(10, 5ms) | Sequential | write delay=%s", latLabel))
		})

		b.Run(fmt.Sprintf("write_%s/NoBatch_Parallel", latLabel), func(b *testing.B) {
			base := connectStore(b)
			store := newSlowEventsStore(base, latency)
			env := setupEngine(b, store)
			entityID := env.spawnEntity(b)

			cmd := &samplepb.CreditAccount{AccountId: entityID, Balance: 1.00}
			rec := newLatencyRecorder(b.N)

			b.ResetTimer()
			b.ReportAllocs()
			rec.wallStart = time.Now()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					start := time.Now()
					_, _, err := env.engine.SendCommand(env.ctx, entityID, cmd, 5*time.Second)
					rec.record(time.Since(start))
					if err != nil {
						rec.recordFailure()
					}
				}
			})

			rec.stop()
			b.StopTimer()
			rec.report(b, fmt.Sprintf("NoBatch | Parallel | write delay=%s", latLabel))
		})

		b.Run(fmt.Sprintf("write_%s/Batch_threshold10_Parallel", latLabel), func(b *testing.B) {
			base := connectStore(b)
			store := newSlowEventsStore(base, latency)
			env := setupEngine(b, store)
			entityID := env.spawnBatchEntity(b, 10, 5*time.Millisecond)

			cmd := &samplepb.CreditAccount{AccountId: entityID, Balance: 1.00}
			rec := newLatencyRecorder(b.N)

			b.ResetTimer()
			b.ReportAllocs()
			rec.wallStart = time.Now()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					start := time.Now()
					_, _, err := env.engine.SendCommand(env.ctx, entityID, cmd, 5*time.Second)
					rec.record(time.Since(start))
					if err != nil {
						rec.recordFailure()
					}
				}
			})

			rec.stop()
			b.StopTimer()
			rec.report(b, fmt.Sprintf("Batch(10, 5ms) | Parallel | write delay=%s", latLabel))
		})
	}
}

// ---------------------------------------------------------------------------
// Benchmarks — Batch threshold sweep
// ---------------------------------------------------------------------------

// BenchmarkBatchThresholds compares throughput across different batch threshold
// sizes with simulated 1ms write latency.
func BenchmarkBatchThresholds(b *testing.B) {
	thresholds := []int{1, 5, 10, 25, 50, 100}

	for _, threshold := range thresholds {
		b.Run(fmt.Sprintf("threshold_%d", threshold), func(b *testing.B) {
			base := connectStore(b)
			store := newSlowEventsStore(base, 1*time.Millisecond)
			env := setupEngine(b, store)
			entityID := env.spawnBatchEntity(b, threshold, 5*time.Millisecond)

			cmd := &samplepb.CreditAccount{AccountId: entityID, Balance: 1.00}
			rec := newLatencyRecorder(b.N)

			b.ResetTimer()
			b.ReportAllocs()
			rec.wallStart = time.Now()

			for b.Loop() {
				start := time.Now()
				_, _, err := env.engine.SendCommand(env.ctx, entityID, cmd, 5*time.Second)
				rec.record(time.Since(start))
				if err != nil {
					rec.recordFailure()
					b.Fatalf("send command: %v", err)
				}
			}

			rec.stop()
			b.StopTimer()
			rec.report(b, fmt.Sprintf("Batch(threshold=%d, 5ms) | Sequential | write delay=1ms", threshold))
		})
	}
}

// ---------------------------------------------------------------------------
// Benchmarks — Concurrent burst (fills batch threshold in one shot)
// ---------------------------------------------------------------------------

// BenchmarkBatch_ConcurrentBurst fires a burst of concurrent commands equal to
// the batch threshold each iteration, measuring batch fill efficiency.
func BenchmarkBatch_ConcurrentBurst(b *testing.B) {
	base := connectStore(b)
	store := newSlowEventsStore(base, 1*time.Millisecond)
	env := setupEngine(b, store)
	entityID := env.spawnBatchEntity(b, 10, 100*time.Millisecond)

	cmd := &samplepb.CreditAccount{AccountId: entityID, Balance: 1.00}
	rec := newLatencyRecorder(b.N * 10)

	b.ResetTimer()
	b.ReportAllocs()
	rec.wallStart = time.Now()

	for b.Loop() {
		var wg sync.WaitGroup
		const concurrency = 10
		wg.Add(concurrency)

		for range concurrency {
			go func() {
				defer wg.Done()
				start := time.Now()
				_, _, err := env.engine.SendCommand(env.ctx, entityID, cmd, 5*time.Second)
				rec.record(time.Since(start))
				if err != nil {
					rec.recordFailure()
				}
			}()
		}

		wg.Wait()
	}

	rec.stop()
	b.StopTimer()
	rec.report(b, "Batch(10, 100ms) | 10 concurrent cmds/iter | write delay=1ms")
}

// ---------------------------------------------------------------------------
// Benchmarks — Multiple entities
// ---------------------------------------------------------------------------

// BenchmarkMultipleEntities_NoBatch measures throughput across 10 non-batched
// entities processing one command each per iteration, concurrently.
func BenchmarkMultipleEntities_NoBatch(b *testing.B) {
	base := connectStore(b)
	store := newSlowEventsStore(base, 1*time.Millisecond)
	env := setupEngine(b, store)

	const numEntities = 10
	entityIDs := make([]string, numEntities)
	for i := range numEntities {
		entityIDs[i] = env.spawnEntity(b)
	}

	rec := newLatencyRecorder(b.N * numEntities)

	b.ResetTimer()
	b.ReportAllocs()
	rec.wallStart = time.Now()

	for b.Loop() {
		var wg sync.WaitGroup
		wg.Add(numEntities)

		for i := range numEntities {
			go func() {
				defer wg.Done()
				cmd := &samplepb.CreditAccount{AccountId: entityIDs[i], Balance: 1.00}
				start := time.Now()
				_, _, err := env.engine.SendCommand(env.ctx, entityIDs[i], cmd, 5*time.Second)
				rec.record(time.Since(start))
				if err != nil {
					rec.recordFailure()
				}
			}()
		}

		wg.Wait()
	}

	rec.stop()
	b.StopTimer()
	rec.report(b, "NoBatch | 10 entities concurrent | write delay=1ms")
}

// BenchmarkMultipleEntities_Batch measures throughput across 10 batched
// entities processing one command each per iteration, concurrently.
func BenchmarkMultipleEntities_Batch(b *testing.B) {
	base := connectStore(b)
	store := newSlowEventsStore(base, 1*time.Millisecond)
	env := setupEngine(b, store)

	const numEntities = 10
	entityIDs := make([]string, numEntities)
	for i := range numEntities {
		entityIDs[i] = env.spawnBatchEntity(b, 10, 5*time.Millisecond)
	}

	rec := newLatencyRecorder(b.N * numEntities)

	b.ResetTimer()
	b.ReportAllocs()
	rec.wallStart = time.Now()

	for b.Loop() {
		var wg sync.WaitGroup
		wg.Add(numEntities)

		for i := range numEntities {
			go func() {
				defer wg.Done()
				cmd := &samplepb.CreditAccount{AccountId: entityIDs[i], Balance: 1.00}
				start := time.Now()
				_, _, err := env.engine.SendCommand(env.ctx, entityIDs[i], cmd, 5*time.Second)
				rec.record(time.Since(start))
				if err != nil {
					rec.recordFailure()
				}
			}()
		}

		wg.Wait()
	}

	rec.stop()
	b.StopTimer()
	rec.report(b, "Batch(10, 5ms) | 10 entities concurrent | write delay=1ms")
}

// ===========================================================================
// Durable State Actor Benchmarks
// ===========================================================================

// ---------------------------------------------------------------------------
// Latency-simulating state store wrapper
// ---------------------------------------------------------------------------

// slowStateStore wraps a real StateStore and adds a configurable delay on
// WriteState. This simulates the I/O cost of a real database.
type slowStateStore struct {
	persistence.StateStore
	writeDelay time.Duration
}

func newSlowStateStore(delegate persistence.StateStore, writeDelay time.Duration) *slowStateStore {
	return &slowStateStore{StateStore: delegate, writeDelay: writeDelay}
}

func (s *slowStateStore) WriteState(ctx context.Context, state *egopb.DurableState) error {
	if s.writeDelay > 0 {
		time.Sleep(s.writeDelay)
	}
	return s.StateStore.WriteState(ctx, state)
}

// ---------------------------------------------------------------------------
// Durable state behavior
// ---------------------------------------------------------------------------

type durableAccountBehavior struct {
	id string
}

var _ ego.DurableStateBehavior = (*durableAccountBehavior)(nil)

func newDurableAccountBehavior(id string) *durableAccountBehavior {
	return &durableAccountBehavior{id: id}
}

func (a *durableAccountBehavior) ID() string { return a.id }

func (a *durableAccountBehavior) InitialState() ego.State {
	return new(samplepb.Account)
}

func (a *durableAccountBehavior) HandleCommand(_ context.Context, command ego.Command, priorVersion uint64, priorState ego.State) (ego.State, uint64, error) {
	switch cmd := command.(type) {
	case *samplepb.CreateAccount:
		return &samplepb.Account{
			AccountId:      cmd.GetAccountId(),
			AccountBalance: cmd.GetAccountBalance(),
		}, priorVersion + 1, nil
	case *samplepb.CreditAccount:
		if cmd.GetAccountId() == a.id {
			account := priorState.(*samplepb.Account)
			bal := account.GetAccountBalance() + cmd.GetBalance()
			return &samplepb.Account{
				AccountId:      cmd.GetAccountId(),
				AccountBalance: bal,
			}, priorVersion + 1, nil
		}
		return nil, 0, errors.New("command sent to the wrong entity")
	default:
		return nil, 0, errors.New("unhandled command")
	}
}

func (a *durableAccountBehavior) MarshalBinary() ([]byte, error) {
	return json.Marshal(struct {
		ID string `json:"id"`
	}{ID: a.id})
}

func (a *durableAccountBehavior) UnmarshalBinary(data []byte) error {
	aux := struct {
		ID string `json:"id"`
	}{}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	a.id = aux.ID
	return nil
}

// ---------------------------------------------------------------------------
// Durable state setup helpers
// ---------------------------------------------------------------------------

// durableBenchEnv holds the shared infrastructure for a durable state benchmark.
type durableBenchEnv struct {
	ctx    context.Context
	engine *ego.Engine
}

// setupDurableEngine creates an ego Engine with a durable state store.
func setupDurableEngine(b *testing.B, store persistence.StateStore) *durableBenchEnv {
	b.Helper()
	ctx := context.Background()

	engine := ego.NewEngine("BenchDurableEngine", nil,
		ego.WithStateStore(store),
		ego.WithLogger(ego.DiscardLogger),
	)
	if err := engine.Start(ctx); err != nil {
		b.Fatalf("start engine: %v", err)
	}

	time.Sleep(time.Second)

	b.Cleanup(func() {
		_ = engine.Stop(ctx)
	})

	return &durableBenchEnv{ctx: ctx, engine: engine}
}

// connectDurableStore creates and connects an in-memory durable state store.
func connectDurableStore(b *testing.B) *testkit.DurableStore {
	b.Helper()
	ctx := context.Background()
	store := testkit.NewDurableStore()
	if err := store.Connect(ctx); err != nil {
		b.Fatalf("connect durable store: %v", err)
	}
	b.Cleanup(func() { _ = store.Disconnect(ctx) })
	return store
}

// spawnDurableEntity registers a durable state entity and initialises it.
func (e *durableBenchEnv) spawnDurableEntity(b *testing.B) string {
	b.Helper()
	entityID := uuid.NewString()
	behavior := newDurableAccountBehavior(entityID)

	if err := e.engine.DurableStateEntity(e.ctx, behavior); err != nil {
		b.Fatalf("create durable entity: %v", err)
	}

	time.Sleep(time.Second)

	_, _, err := e.engine.SendCommand(e.ctx, entityID, &samplepb.CreateAccount{
		AccountId:      entityID,
		AccountBalance: 1000.00,
	}, 5*time.Second)
	if err != nil {
		b.Fatalf("init durable account: %v", err)
	}

	return entityID
}

// ---------------------------------------------------------------------------
// Durable state benchmarks — In-memory store (baseline)
// ---------------------------------------------------------------------------

// BenchmarkDurableStateActor_Sequential measures sequential command throughput
// for durable state entities.
func BenchmarkDurableStateActor_Sequential(b *testing.B) {
	store := connectDurableStore(b)
	env := setupDurableEngine(b, store)
	entityID := env.spawnDurableEntity(b)

	cmd := &samplepb.CreditAccount{AccountId: entityID, Balance: 1.00}
	rec := newLatencyRecorder(b.N)

	b.ResetTimer()
	b.ReportAllocs()
	rec.wallStart = time.Now()

	for b.Loop() {
		start := time.Now()
		_, _, err := env.engine.SendCommand(env.ctx, entityID, cmd, 5*time.Second)
		rec.record(time.Since(start))
		if err != nil {
			rec.recordFailure()
			b.Fatalf("send command: %v", err)
		}
	}

	rec.stop()
	b.StopTimer()
	rec.report(b, "DurableState | Sequential | in-memory store")
}

// BenchmarkDurableStateActor_Parallel measures concurrent command throughput
// for durable state entities.
func BenchmarkDurableStateActor_Parallel(b *testing.B) {
	store := connectDurableStore(b)
	env := setupDurableEngine(b, store)
	entityID := env.spawnDurableEntity(b)

	cmd := &samplepb.CreditAccount{AccountId: entityID, Balance: 1.00}
	rec := newLatencyRecorder(b.N)

	b.ResetTimer()
	b.ReportAllocs()
	rec.wallStart = time.Now()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			start := time.Now()
			_, _, err := env.engine.SendCommand(env.ctx, entityID, cmd, 5*time.Second)
			rec.record(time.Since(start))
			if err != nil {
				rec.recordFailure()
			}
		}
	})

	rec.stop()
	b.StopTimer()
	rec.report(b, fmt.Sprintf("DurableState | Parallel(%d goroutines) | in-memory store", runtime.GOMAXPROCS(0)))
}

// ---------------------------------------------------------------------------
// Durable state benchmarks — Simulated I/O latency
// ---------------------------------------------------------------------------

// BenchmarkDurableStateActor_WriteLatency compares durable state throughput
// at various simulated write latencies.
func BenchmarkDurableStateActor_WriteLatency(b *testing.B) {
	latencies := []time.Duration{
		100 * time.Microsecond,
		500 * time.Microsecond,
		1 * time.Millisecond,
		5 * time.Millisecond,
	}

	for _, latency := range latencies {
		latLabel := latency.String()

		b.Run(fmt.Sprintf("write_%s/Sequential", latLabel), func(b *testing.B) {
			base := connectDurableStore(b)
			store := newSlowStateStore(base, latency)
			env := setupDurableEngine(b, store)
			entityID := env.spawnDurableEntity(b)

			cmd := &samplepb.CreditAccount{AccountId: entityID, Balance: 1.00}
			rec := newLatencyRecorder(b.N)

			b.ResetTimer()
			b.ReportAllocs()
			rec.wallStart = time.Now()

			for b.Loop() {
				start := time.Now()
				_, _, err := env.engine.SendCommand(env.ctx, entityID, cmd, 5*time.Second)
				rec.record(time.Since(start))
				if err != nil {
					rec.recordFailure()
					b.Fatalf("send command: %v", err)
				}
			}

			rec.stop()
			b.StopTimer()
			rec.report(b, fmt.Sprintf("DurableState | Sequential | write delay=%s", latLabel))
		})

		b.Run(fmt.Sprintf("write_%s/Parallel", latLabel), func(b *testing.B) {
			base := connectDurableStore(b)
			store := newSlowStateStore(base, latency)
			env := setupDurableEngine(b, store)
			entityID := env.spawnDurableEntity(b)

			cmd := &samplepb.CreditAccount{AccountId: entityID, Balance: 1.00}
			rec := newLatencyRecorder(b.N)

			b.ResetTimer()
			b.ReportAllocs()
			rec.wallStart = time.Now()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					start := time.Now()
					_, _, err := env.engine.SendCommand(env.ctx, entityID, cmd, 5*time.Second)
					rec.record(time.Since(start))
					if err != nil {
						rec.recordFailure()
					}
				}
			})

			rec.stop()
			b.StopTimer()
			rec.report(b, fmt.Sprintf("DurableState | Parallel | write delay=%s", latLabel))
		})
	}
}

// ---------------------------------------------------------------------------
// Durable state benchmarks — Multiple entities
// ---------------------------------------------------------------------------

// BenchmarkDurableStateActor_MultipleEntities measures throughput across 10
// durable state entities processing commands concurrently.
func BenchmarkDurableStateActor_MultipleEntities(b *testing.B) {
	base := connectDurableStore(b)
	store := newSlowStateStore(base, 1*time.Millisecond)
	env := setupDurableEngine(b, store)

	const numEntities = 10
	entityIDs := make([]string, numEntities)
	for i := range numEntities {
		entityIDs[i] = env.spawnDurableEntity(b)
	}

	rec := newLatencyRecorder(b.N * numEntities)

	b.ResetTimer()
	b.ReportAllocs()
	rec.wallStart = time.Now()

	for b.Loop() {
		var wg sync.WaitGroup
		wg.Add(numEntities)

		for i := range numEntities {
			go func() {
				defer wg.Done()
				cmd := &samplepb.CreditAccount{AccountId: entityIDs[i], Balance: 1.00}
				start := time.Now()
				_, _, err := env.engine.SendCommand(env.ctx, entityIDs[i], cmd, 5*time.Second)
				rec.record(time.Since(start))
				if err != nil {
					rec.recordFailure()
				}
			}()
		}

		wg.Wait()
	}

	rec.stop()
	b.StopTimer()
	rec.report(b, "DurableState | 10 entities concurrent | write delay=1ms")
}
