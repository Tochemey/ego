# Benchmark Makefile
#
# By default benchmarks run against the released ego module.
# Use `make <target>-local` to benchmark against your local working copy.
#
# Examples:
#   make bench                        # all benchmarks against released module
#   make bench-nobatch-local          # NoBatch sequential against local module
#   make bench-write-latency BENCHTIME=5s  # write latency sweep, 5s per bench

BENCHTIME  ?= 3s
BENCHCOUNT ?= 1
GO_TEST     = go test -v -run='^$$' -benchtime=$(BENCHTIME) -count=$(BENCHCOUNT)

# ---------------------------------------------------------------------------
# Helpers for local module replacement
# ---------------------------------------------------------------------------
.PHONY: _use-local _use-released

_use-local:
	@go mod edit -replace github.com/tochemey/ego/v4=../
	@go mod tidy

_use-released:
	@go mod edit -dropreplace github.com/tochemey/ego/v4 2>/dev/null || true
	@go mod tidy

# ---------------------------------------------------------------------------
# All benchmarks
# ---------------------------------------------------------------------------
.PHONY: bench bench-local bench-clean

## bench: run all benchmarks against the released module
bench:
	$(GO_TEST) -bench='.' .

## bench-local: run all benchmarks against the local working copy
bench-local: _use-local
	$(GO_TEST) -bench='.' .
	@$(MAKE) _use-released

## bench-clean: remove any leftover local replace directive
bench-clean: _use-released

# ---------------------------------------------------------------------------
# In-memory store — sequential
# ---------------------------------------------------------------------------
.PHONY: bench-nobatch bench-nobatch-local bench-batch bench-batch-local

## bench-nobatch: NoBatch sequential (in-memory store)
bench-nobatch:
	$(GO_TEST) -bench='BenchmarkEventSourcedActor_NoBatch$$' .

bench-nobatch-local: _use-local
	$(GO_TEST) -bench='BenchmarkEventSourcedActor_NoBatch$$' .
	@$(MAKE) _use-released

## bench-batch: Batch sequential (in-memory store)
bench-batch:
	$(GO_TEST) -bench='BenchmarkEventSourcedActor_Batch$$' .

bench-batch-local: _use-local
	$(GO_TEST) -bench='BenchmarkEventSourcedActor_Batch$$' .
	@$(MAKE) _use-released

# ---------------------------------------------------------------------------
# In-memory store — parallel
# ---------------------------------------------------------------------------
.PHONY: bench-nobatch-parallel bench-nobatch-parallel-local \
        bench-batch-parallel bench-batch-parallel-local

## bench-nobatch-parallel: NoBatch parallel (in-memory store)
bench-nobatch-parallel:
	$(GO_TEST) -bench='BenchmarkEventSourcedActor_NoBatch_Parallel$$' .

bench-nobatch-parallel-local: _use-local
	$(GO_TEST) -bench='BenchmarkEventSourcedActor_NoBatch_Parallel$$' .
	@$(MAKE) _use-released

## bench-batch-parallel: Batch parallel (in-memory store)
bench-batch-parallel:
	$(GO_TEST) -bench='BenchmarkEventSourcedActor_Batch_Parallel$$' .

bench-batch-parallel-local: _use-local
	$(GO_TEST) -bench='BenchmarkEventSourcedActor_Batch_Parallel$$' .
	@$(MAKE) _use-released

# ---------------------------------------------------------------------------
# Simulated write latency — NoBatch vs Batch at various delays
# ---------------------------------------------------------------------------
.PHONY: bench-write-latency bench-write-latency-local

## bench-write-latency: compare NoBatch vs Batch across write delays (100µs–5ms)
bench-write-latency:
	$(GO_TEST) -bench='BenchmarkWriteLatency_NoBatch_vs_Batch' .

bench-write-latency-local: _use-local
	$(GO_TEST) -bench='BenchmarkWriteLatency_NoBatch_vs_Batch' .
	@$(MAKE) _use-released

# ---------------------------------------------------------------------------
# Batch threshold sweep
# ---------------------------------------------------------------------------
.PHONY: bench-thresholds bench-thresholds-local

## bench-thresholds: sweep batch thresholds (1, 5, 10, 25, 50, 100) with 1ms write delay
bench-thresholds:
	$(GO_TEST) -bench='BenchmarkBatchThresholds' .

bench-thresholds-local: _use-local
	$(GO_TEST) -bench='BenchmarkBatchThresholds' .
	@$(MAKE) _use-released

# ---------------------------------------------------------------------------
# Concurrent burst
# ---------------------------------------------------------------------------
.PHONY: bench-burst bench-burst-local

## bench-burst: 10 concurrent commands per iteration (fills batch threshold)
bench-burst:
	$(GO_TEST) -bench='BenchmarkBatch_ConcurrentBurst$$' .

bench-burst-local: _use-local
	$(GO_TEST) -bench='BenchmarkBatch_ConcurrentBurst$$' .
	@$(MAKE) _use-released

# ---------------------------------------------------------------------------
# Multiple entities
# ---------------------------------------------------------------------------
.PHONY: bench-multi-nobatch bench-multi-nobatch-local \
        bench-multi-batch bench-multi-batch-local

## bench-multi-nobatch: 10 NoBatch entities concurrent (1ms write delay)
bench-multi-nobatch:
	$(GO_TEST) -bench='BenchmarkMultipleEntities_NoBatch$$' .

bench-multi-nobatch-local: _use-local
	$(GO_TEST) -bench='BenchmarkMultipleEntities_NoBatch$$' .
	@$(MAKE) _use-released

## bench-multi-batch: 10 Batch entities concurrent (1ms write delay)
bench-multi-batch:
	$(GO_TEST) -bench='BenchmarkMultipleEntities_Batch$$' .

bench-multi-batch-local: _use-local
	$(GO_TEST) -bench='BenchmarkMultipleEntities_Batch$$' .
	@$(MAKE) _use-released

# ---------------------------------------------------------------------------
# Durable state benchmarks
# ---------------------------------------------------------------------------
.PHONY: bench-durable bench-durable-local \
        bench-durable-parallel bench-durable-parallel-local \
        bench-durable-write-latency bench-durable-write-latency-local \
        bench-durable-multi bench-durable-multi-local

## bench-durable: DurableState sequential (in-memory store)
bench-durable:
	$(GO_TEST) -bench='BenchmarkDurableStateActor_Sequential$$' .

bench-durable-local: _use-local
	$(GO_TEST) -bench='BenchmarkDurableStateActor_Sequential$$' .
	@$(MAKE) _use-released

## bench-durable-parallel: DurableState parallel (in-memory store)
bench-durable-parallel:
	$(GO_TEST) -bench='BenchmarkDurableStateActor_Parallel$$' .

bench-durable-parallel-local: _use-local
	$(GO_TEST) -bench='BenchmarkDurableStateActor_Parallel$$' .
	@$(MAKE) _use-released

## bench-durable-write-latency: DurableState across write delays (100us-5ms)
bench-durable-write-latency:
	$(GO_TEST) -bench='BenchmarkDurableStateActor_WriteLatency' .

bench-durable-write-latency-local: _use-local
	$(GO_TEST) -bench='BenchmarkDurableStateActor_WriteLatency' .
	@$(MAKE) _use-released

## bench-durable-multi: 10 DurableState entities concurrent (1ms write delay)
bench-durable-multi:
	$(GO_TEST) -bench='BenchmarkDurableStateActor_MultipleEntities$$' .

bench-durable-multi-local: _use-local
	$(GO_TEST) -bench='BenchmarkDurableStateActor_MultipleEntities$$' .
	@$(MAKE) _use-released

# ---------------------------------------------------------------------------
# Help
# ---------------------------------------------------------------------------
.PHONY: help

## help: show available targets
help:
	@echo "Available targets (append -local to run against local ego module):"
	@echo ""
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/^## /  /' | sort
	@echo ""
	@echo "Variables:"
	@echo "  BENCHTIME  = $(BENCHTIME)   (duration per benchmark)"
	@echo "  BENCHCOUNT = $(BENCHCOUNT)       (number of runs)"
