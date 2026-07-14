# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### 🐛 Bug Fixes

- **Remote entity spawns no longer fail with `dependency type is not registered` in cluster mode.**
  `Engine.Entity`, `Engine.DurableStateEntity`, and `Engine.Saga` registered the behavior's dependency type
  only on the calling node, at spawn time. With the default `RoundRobin` placement, `SpawnOn` routes most
  spawns to a peer, and the receiving node deserializes the spawn request's dependencies (the behavior plus
  eGo's internal spawn-configuration types) against **its own** registry — failing unless that node had
  already spawned the same kind itself. On a fresh N-node cluster, the first spawn of any entity kind failed
  roughly (N−1)/N of the time.

  `NewEngine` now registers eGo's internal spawn-configuration dependency types (which application code
  cannot reach) on every node, together with the behavior kinds supplied via the new `WithEntityKinds`
  option:

  - `ego.EntityKind` — alias for goakt's `extension.Dependency`; every `EventSourcedBehavior`,
    `DurableStateBehavior`, and `SagaBehavior` value is an `EntityKind`.
  - `ego.WithEntityKinds(kinds ...EntityKind) Option` — lists the behavior types this node can host.
    Pass one value per behavior type (a zero value is fine; only the concrete type is registered).

  **Cluster deployments must now build every node's `Config` with `WithEntityKinds`, listing every entity,
  durable-state, and saga behavior the cluster hosts** — the same contract `ClusterKinds()` already
  establishes for actor kinds:

  ```go
  cfg := ego.NewConfig(eventsStore,
      ego.WithEntityKinds(new(AccountBehavior), new(OrderBehavior)),
      // ...
  )
  ```

  Single-node deployments are unaffected and may omit the option: the lazy registration done by
  `Entity`, `DurableStateEntity`, and `Saga` remains as a local-node fallback.

## [v4.2.1] - 2026-06-20

### 🐛 Bug Fixes

- **Publisher and saga event-stream loops no longer busy-spin a CPU core when idle** (`6789f66`, #292).
  `eventstream.Subscriber.Iterator()` returns a closed snapshot channel whenever the queue is empty, so the
  engine's `sendEvent`/`sendState` publisher loops and the saga actor's receive loop — which `select`ed on
  `Iterator()` directly — spun at 100% CPU per loop while waiting for messages. The `Subscriber` interface
  gained a `Ready() <-chan struct{}` signal that fires when messages are enqueued or the subscriber shuts
  down; the loops now block on `Ready()` while idle and only drain the `Iterator()` snapshot once woken.
  If you implement `eventstream.Subscriber` yourself, you must add the `Ready()` method.

### 🧹 Improvements

- **Logger adapter refactor** (`8f0a73d`, `491c480`). The goakt logger adapter avoids `fmt.Sprint`
  reflection on the common single-string-message path, delegates the `*Context` variants to their
  non-context counterparts instead of duplicating their bodies, and replaces ad-hoc level strings with
  named constants. Dead code and redundant tests were removed alongside.

## [v4.2.0] - 2026-05-17

### 💥 Breaking Changes

- **eGo Is Now a Meta-Framework on Top of Go-Akt** — eGo no longer constructs, starts, or stops the underlying
  `goakt.ActorSystem`. Cluster discovery, TLS, remoting, partitioning, supervisors, and any other actor-runtime
  concern are now configured directly through Go-Akt's APIs. eGo contributes its event-sourcing, durable-state,
  projection, and saga primitives as Go-Akt extensions and plugs into an actor system the developer has built.

  **New surface:**

  - `ego.Config` — captures every option an engine needs (events store, plus state store, offset store,
    projection, snapshot store, event adapters, telemetry, encryptor, logger). Built once with `ego.NewConfig`
    and reused twice: once to build the actor system, once to plug in the engine.
  - `ego.NewConfig(eventsStore, opts ...Option) *Config` — constructs the config. Pass `nil` for `eventsStore`
    in durable-state-only deployments.
  - `(*Config).GoaktOptions() []goakt.Option` — returns the Go-Akt options eGo needs at actor-system
    construction: engine extensions (events store, event stream, plus state store, offset store, projection,
    snapshot store, event adapters, telemetry, encryptor whenever those are configured), pubsub, the logger
    adapter, and the default supervisor. Pass to `goakt.NewActorSystem(...)`.
  - `ego.ClusterKinds() []goakt.Actor` — returns the four actor kinds eGo needs registered in the cluster config
    (`EventSourcedActor`, `DurableStateActor`, `SagaActor`, `ProjectionActor`). Pass to
    `goakt.NewClusterConfig().WithKinds(...)` for relocation to work in cluster mode.
  - `ego.NewEngine(sys goakt.ActorSystem, cfg *Config) (*Engine, error)` — plugs eGo into an already-running
    actor system. Returns an error if `sys` is not running or if a required extension is missing from `sys`.
    The engine name is taken from `sys.Name()` — there is no separate name parameter. `Engine.Stop` does not
    call `sys.Stop`; the caller owns the actor-system lifecycle.

  **Typical bootstrap (any deployment shape):**

  ```go
  cfg := ego.NewConfig(eventsStore, opts...)

  sys, _ := goakt.NewActorSystem("MyApp", cfg.GoaktOptions()...)
  sys.Start(ctx)
  defer sys.Stop(ctx)

  engine, _ := ego.NewEngine(sys, cfg)
  engine.Start(ctx)
  defer engine.Stop(ctx)
  ```

  The same four-line shape works for local development and cluster mode; cluster users append Go-Akt's
  `WithCluster(...)` (with `ego.ClusterKinds()` registered) and `WithRemote(...)` to `cfg.GoaktOptions()`
  when calling `NewActorSystem`.

  **Removed:**

  - `WithCluster`, `WithClusterOption`, `ClusterOption`, `WithClusterConfigurator`, `ClusterProvider` (cluster
    configuration is now done directly with `goakt.NewClusterConfig(...)`).
  - `WithTLS`, `TLS` (use `goakt.WithTLS(...)` directly).
  - `WithRoles` (use `goakt.NewClusterConfig().WithRoles(...)` directly).
  - `WithRemoteOptions`, `WithActorSystemOptions` (compose Go-Akt options directly when calling
    `goakt.NewActorSystem`).
  - `WithActorSystemBuilder`, `ActorSystemBuilder`, `BuildActorSystem` (no longer needed — the actor system is
    always built by the caller).
  - `Engine` constructor argument: the `name string` parameter is removed; the engine name is derived from
    `sys.Name()`.

  **Why the change.** The previous design tried to be both a Go-Akt wrapper and a Go-Akt collaborator at the
  same time. That created precedence and cluster-coordination problems whenever a caller needed control over
  the runtime, and it hid Go-Akt capabilities developers legitimately need (custom cluster config, custom
  discovery, additional actors on the same system, TLS specifics). Treating eGo as a meta-framework on top of
  Go-Akt — analogous to how `database/sql` sits on top of a driver, or `net/http` sits on top of a `Listener` —
  resolves both concerns by exposing the primitives eGo needs and letting the developer compose the runtime.

  See [option.go](option.go), [engine.go](engine.go), and the rewritten [readme.md](./readme.md) for the full
  bootstrap pattern.

### 📝 Migration Notes

Upgrading from `v4.1.x` to this release requires reshaping the engine bootstrap:

1. Replace `ego.NewEngine("myapp", eventsStore, opts...)` with:
   ```go
   cfg := ego.NewConfig(eventsStore, opts...)
   sys, _ := goakt.NewActorSystem("myapp", cfg.GoaktOptions()...)
   sys.Start(ctx)
   engine, _ := ego.NewEngine(sys, cfg)
   engine.Start(ctx)
   ```
2. Move cluster configuration from `ego.WithClusterOption(...)` to a direct
   `goakt.NewClusterConfig(...).WithKinds(ego.ClusterKinds()...)` call passed to
   `goakt.NewActorSystem` via `goakt.WithCluster(...)`. Discovery providers come from the `discovery` packages
   of Go-Akt.
3. Move TLS from `ego.WithTLS(...)` to `goakt.WithTLS(...)`.
4. Drop `ego.WithRoles(...)`; use `goakt.NewClusterConfig(...).WithRoles(...)` instead.
5. Stop calling `Engine.Stop` for actor-system shutdown — call `sys.Stop(ctx)` yourself after `engine.Stop(ctx)`.
6. Check the `error` returned by the new `ego.NewEngine` signature; it now reports missing required extensions
   and unstarted actor systems instead of deferring those errors to `Engine.Start`.

### 🐛 Bug Fixes & Internal Changes

- **Event timestamps now use nanosecond resolution (was: seconds).** The `Timestamp` field on
  `egopb.Event`, `egopb.DurableState`, `egopb.Snapshot`, and `egopb.StateReply` is now populated via
  `time.Now().UnixNano()` instead of `time.Now().Unix()`. The underlying type stays `int64`, so wire
  compatibility is preserved, but the **semantic units change**.
  - Anything that interprets these timestamps as seconds (custom event publishers, external dashboards
    reading the raw column, ad-hoc SQL using `to_timestamp(timestamp / 1.0)`, etc.) must be updated to
    treat them as nanoseconds — e.g. `to_timestamp(timestamp / 1e9)` in Postgres.
  - `Engine.ProjectionLag(...)` continues to return a `time.Duration`; the value is now correct at
    nanosecond resolution instead of being quantised to whole seconds.
  - **Why.** Pre-fix, the projection runner's offset cursor was an event timestamp at 1-second
    resolution, and the poll predicate (`WHERE timestamp > committed_offset`) would silently skip any
    event written with the same second as the previously committed offset between two polls. Under
    sub-second burst load, events at the second boundary were dropped (reproduced by `make test` in
    `example/cluster`: 1000 + 30×10 − 10×5 expected = 1250, observed = 1270 because four debits were
    skipped). Bumping the timestamp to nanoseconds makes co-timestamp collisions across polls
    astronomically improbable and eliminates the race for any realistic workload.

- **In-process pub/sub topics collapsed to a single events topic and a single states topic.** Entity
  actors previously published to `topic.events.<shard>` / `topic.states.<shard>` and subscribers
  (engine publishers, sagas, `Engine.Subscribe()` consumers) had to enumerate every per-shard topic.
  With goakt's default 271 partitions hardcoded into the subscribe loop, any deployment configured
  with `goakt.NewClusterConfig().WithPartitionCount(N)` where `N > 271` would silently drop events
  for entities mapped to shards ≥ 271.
  - Now all entity events publish to a single `topic.events` (and durable state to `topic.states`).
    The shard each event belongs to travels in the payload (`egopb.Event.Shard`,
    `egopb.DurableState.Shard`), so downstream consumers can still filter by shard.
  - The internal `Engine.publisherTopicsPartitionCount` helper and the per-shard `generateTopics`
    helper have been removed. This is internal-only, but anyone who reached into the eventstream
    extension directly and subscribed to `topic.events.<n>` should switch to the single topic.

- **Sagas now observe events from every shard.** `SagaActor.PreStart` previously subscribed to
  topics `topic.events.0` … `topic.events.<ownShard>` (only as many topics as the saga's own shard
  index), so a saga placed in shard 5 missed events from shards 6 and above. Folded into the
  single-topic change above: sagas now subscribe to `topic.events` once and see every event.

- **`example/cluster/projection.go`** — fixed `AccountDebited` upsert that emitted `VALUES ($1, -$2, $3)`,
  which Postgres rejected with "operator is not unique: - unknown (SQLSTATE 42725)" because the prefix
  `-` could not pick an overload against an untyped placeholder. The handler now pre-negates the delta
  in Go and uses `EXCLUDED.balance` symmetrically with the credit branch.

- **`example/cluster/Makefile`** — the load-distribution probe now hits `/accounts/{id}` instead of
  `/healthz`. Kind's NGINX Ingress Controller exposes its own `/healthz` on the data port (80) and
  answers it before the request reaches the app, so the previous probe never received the app's
  `X-Served-By` header and the assertion was a no-op.

## [v4.1.2] - 2026-04-26

### 🚀 New Features

- **`Engine.ActorSystem()` Accessor** — Added `Engine.ActorSystem()` to expose the underlying `goakt.ActorSystem`
  powering the engine. Returns `nil` before `Start` and after `Stop`. Intended for callers that need direct access to
  actor-system primitives — for example, spawning auxiliary actors alongside ego entities or inspecting cluster
  topology — without having to wire the actor system separately. The returned reference is a snapshot and should not
  be retained across engine restarts. See [engine.go](engine.go).

### 🧹 Improvements

- **Lock-Free Hot Read Paths** — To make the new accessor safe for concurrent use, the underlying `goakt.ActorSystem`
  and its `NoSender` PID are now bundled in a single `atomic.Pointer[actorSystemRef]` and published atomically by
  `Start`. All hot read paths (`AddProjection`, `RemoveProjection`, `IsProjectionRunning`, `Entity`, `EntityExists`,
  `DurableStateEntity`, `SendCommand`, `Saga`, `SagaStatus`) now load the reference with a single atomic load instead
  of taking a mutex. `Stop` swaps the reference to `nil` so callers racing with shutdown fail fast with
  `ErrEngineNotStarted` rather than operating on a system in mid-teardown.
- **`RWMutex` for Remaining Snapshot Reads** — The engine mutex has been promoted from `sync.Mutex` to `sync.RWMutex`.
  The remaining field-snapshot reads (`offsetStore`, `eventStream`, `stateStore`, `eventsStore`, `snapshotStore`)
  acquire it via `RLock`, allowing concurrent readers. Writers (`AddEventPublishers`, `AddStatePublishers`) keep the
  exclusive `Lock`.
- **Test Coverage** — Added `TestActorSystem` covering the unstarted, started, stopped, and concurrent-reader cases
  (the latter exercised under `-race`), and `TestActorSystemRefGuard` exercising the defensive `ErrEngineNotStarted`
  branch on every hot read path.

## [v4.1.1] - 2026-04-18

### 🚀 New Features

- **Entity Existence Probe** — Added `Engine.EntityExists(ctx, entityID)` to check whether an entity is currently alive
  in the cluster without spawning it or replaying its journal. Works for both event-sourced and durable state entities,
  and is intended as a lightweight liveness check for callers that need to branch on entity presence without paying the
  cost of materialization. See [engine.go](engine.go).

### 🐛 Bug Fixes

- **Projection Lag Computation** — Fixed `Engine.ProjectionLag` to correctly compute the per-shard delta between the
  newest persisted event and the projection's committed offset. The result is now clamped at zero so a projection
  running ahead of the probe window is no longer reported as negative, and the implementation documents the
  unit (seconds) and bounded-scan trade-off used to locate the latest event in a shard. See [engine.go](engine.go).

### 🧹 Improvements

- Added `SECURITY.md` describing the project's security analysis and disclosure process.

## [v4.1.0] - 2026-04-02

### 🚀 New Features

- **Event Batching** — Accumulate events from multiple commands and flush them in a single store write, amortizing
  persistence cost under concurrent load. Two new spawn options control batching:
  - `WithBatchThreshold(n)` — flush after `n` accumulated events (0 disables batching, which is the default)
  - `WithBatchFlushWindow(d)` — flush after duration `d`, whichever comes first
  While a batch is being written, the actor stashes incoming commands and replays them after the write completes.

- **Benchmark Suite** — Added a comprehensive benchmark suite (`benchmark/`) measuring throughput, latency percentiles
  (p50/p90/p95/p99), heap usage, and GC cycles across sequential, parallel, batched, and unbatched workloads with
  simulated I/O latencies.

### ⚡ Performance Improvements

- **Async Persistence Pipeline** — Restructured event-sourced entity persistence into three specialized child actors:
  - `EventsWriterActor` — synchronous (Ask) event persistence and publishing; correctness requires write confirmation
  - `SnapshotsWriterActor` — asynchronous (Tell) snapshot persistence with encryption and exponential-backoff retry
  - `EventsJanitorActor` — asynchronous (Tell) retention policy enforcement after snapshot writes
  Snapshots and retention no longer block command processing, eliminating the primary synchronous bottleneck.

- **Batch Event Tracking** — Batch threshold now counts accumulated events rather than commands, correctly handling
  commands that produce multiple events.

### 🧹 Improvements

- Added retry utility (`retry.go`) with exponential backoff and jitter (up to 3 retries, capped at 2s) for async
  persistence operations
- Added Performance Tuning section to README covering batch threshold selection, snapshot configuration, retention
  policies, allocation optimization, and horizontal scaling guidance
- Added Persistence Stores section to README documenting ego-contrib store implementations (Postgres, MongoDB)
- Refreshed README header layout and removed emojis from section headings
- Comprehensive test coverage for events writer, snapshots writer, and events janitor actors
- Added batch trace assertion tests
- Excluded benchmark tests from code coverage metrics
- Updated publisher modules to ego v4.0.0

### ⬆️ Dependencies

- `github.com/tochemey/goakt/v4` v4.1.0 → v4.2.0
- `github.com/jackc/pgx/v5` → v5.9.1
- `github.com/klauspost/compress` v1.18.4 → v1.18.5
- `github.com/fxamacker/cbor/v2` v2.9.0 → v2.9.1
- `github.com/andybalholm/brotli` v1.2.0 → v1.2.1
- `golangci-lint` → v2.11.4
- `codecov/codecov-action` → v6

## [v4.0.0] - 2026-03-21

### 🚀 New Features

- **📸 Snapshot Store** — Introduced a dedicated `SnapshotStore` interface (`persistence/snapshot_store.go`) for
  persisting entity state snapshots independently from events. This decouples state recovery from event replay,
  significantly improving recovery performance for entities with long event histories. Configurable via
  `WithSnapshotStore()` engine option and `WithSnapshotInterval()` spawn option.

- **🔄 Event Adapters (Schema Evolution)** — Added the `eventadapter` package with an `EventAdapter` interface and a
  `Chain()` function for composing adapters. Event adapters transform persisted events from older schema versions into
  the current shape during replay and projection consumption, enabling seamless event schema evolution without rewriting
  stored data.

- **📊 OpenTelemetry Integration** — First-class observability via the new `Telemetry` struct and `WithTelemetry()`
  engine option. Includes:
  - Trace spans on command processing (`ego.command`) with `ego.persistence_id` and `ego.command_type` attributes
  - Metrics:
    - `ego.commands.total` (counter) — total number of commands processed
    - `ego.commands.duration` (histogram, ms) — command processing latency
    - `ego.events.persisted` (counter) — total number of events persisted
    - `ego.projection.events.processed` (counter) — total events processed by projections
    - `ego.entities.active` (up/down counter) — number of currently active entities
    - `ego.projections.active` (up/down counter) — number of currently active projections

- **💀 Dead Letter Handler** — Added the `DeadLetterHandler` interface (`projection/deadletter.go`) for receiving events
  that a projection failed to process after exhausting its recovery policy. Includes a `DiscardDeadLetterHandler` as the
  default no-op implementation. Configurable via `WithProjection()`.

- **🔁 Projection Rebuild** — New `Engine.RebuildProjection(ctx, name, from)` API method that stops a running projection,
  resets its offset to a given timestamp, and restarts it. Enables re-processing events from any point in time.

- **🧪 Testkit Scenarios** — Fluent Given/When/Then API for testing behaviors without starting an engine:
  - `EventSourcedScenario` — `Given(events...)`, `When(command)`, then assert with `ThenEvents()`, `ThenState()`,
      `ThenError()`, or `ThenNoEvents()`
  - `DurableStateScenario` — `Given(state, version)`, `When(command)`, then assert with `ThenState()`,
      `ThenVersion()`, or `ThenError()`

- **🔀 Migration Utility** — One-time `Migrator` (`migration/`) that reads legacy events (which embedded
  `resulting_state` at proto field 5) and extracts that state into the new `SnapshotStore`. Supports configurable page
  size and logging. Idempotent and safe to run multiple times.

- **📈 Projection Lag Monitoring** — Operators can now observe how far behind each projection is relative to the latest
  events in the store. Includes:
  - New metrics:
    - `ego.projection.lag_ms` (gauge, ms) — per-projection, per-shard lag
    - `ego.projection.latest_offset` (gauge, ms) — current projection offset timestamp per shard
    - `ego.projection.events_behind` (gauge) — approximate number of unprocessed events per shard
  - New `Engine.ProjectionLag(ctx, projectionName)` API returning per-shard lag as `map[uint64]time.Duration`

- **🗑️ Snapshot/Event Retention Policies** — Automatic cleanup of old events and snapshots after a snapshot has been
  successfully written, preventing unbounded storage growth. Configurable via `WithRetentionPolicy()` spawn option with:
  - `DeleteEventsOnSnapshot` — delete events up to the snapshot sequence number
  - `DeleteSnapshotsOnSnapshot` — delete older snapshots, keeping only the latest
  - `EventsRetentionCount` — number of events to retain before the snapshot point as a safety margin

- **🔐 Event Encryption / GDPR Support** — Transparent encryption of event and snapshot payloads at rest with
  crypto-shredding support for GDPR "right to erasure". Includes:
  - New `encryption` package with `Encryptor` and `KeyStore` interfaces
  - Default AES-256-GCM implementation (`encryption.AESEncryptor`)
  - New `encryption_key_id` and `is_encrypted` fields on `Event` and `Snapshot` protobuf messages
  - Events encrypted before persistence, decrypted during entity recovery and projection consumption
  - `Engine.EraseEntity(ctx, persistenceID, full)` API for GDPR erasure (physical deletion of events/snapshots)
  - `WithEncryptor()` engine option to enable encryption
  - In-memory `testkit.KeyStore` for testing

- **📝 Pluggable Logger Interface** — Introduced a minimal `Logger` interface (`logger.go`) that lets developers plug in
  any logging backend (zap, zerolog, slog, logrus, etc.). Methods follow the slog convention with structured key-value
  pairs. The engine now stores `Logger` directly and wraps it via `loggerAdapter` when passing to the underlying actor
  system. Includes:
  - `Logger` interface with `Debug`, `Info`, `Warn`, `Error` methods
  - Optional `LeveledLogger` interface for engine-side log gating
  - `DiscardLogger` — exported no-op logger for tests or silent operation
  - Default `slog`-based logger used when no logger is explicitly configured
  - `WithLogger()` engine option now accepts `Logger` instead of `log.Logger`

- **🔄 Saga/Process Manager** — First-class abstraction for long-running business processes that coordinate multiple
  entities with compensation logic for rollback on failures. Includes:
  - `SagaBehavior` interface with `HandleEvent`, `HandleResult`, `HandleError`, `ApplyEvent`, and `Compensate` methods
  - `SagaAction` type for declaring commands to send, events to persist, and completion/compensation signals
  - `SagaCommand` type for targeting commands to specific entities with configurable timeouts
  - Event-sourced saga actor that subscribes to the event stream, persists its own events, and recovers after restarts
  - `Engine.Saga(ctx, behavior, timeout)` API to start a saga
  - `Engine.SagaStatus(ctx, sagaID, timeout)` API to query saga state
  - Automatic compensation on timeout

### 💥 Breaking Changes

- **📦 Module Path** — Module path changed from `github.com/tochemey/ego/v3` to `github.com/tochemey/ego/v4`. All import
  paths must be updated.

- **🗃️ Event Proto Schema** — The `resulting_state` field (field 5) has been **removed** from the `Event` protobuf
  message and marked as reserved. Events are now "pure" — they no longer carry inline entity state. State is managed
  separately via the new Snapshot Store.

- **📐 Projection Handler Signature** — `projection.Handler.Handle()` signature changed: the `state *anypb.Any` parameter
  has been removed.
  - **Before:** `Handle(ctx, persistenceID, event, state, revision)`
  - **After:** `Handle(ctx, persistenceID, event, revision)`

- **🔧 Event-Sourced Recovery Rewrite** — Entity recovery no longer reads state from the latest event's
  `resulting_state`. Recovery now loads the latest snapshot (if available) and replays only subsequent events, applying
  event adapters in the chain.

- **📝 Logger Interface** — `WithLogger()` now accepts `ego.Logger` instead of `goakt/log.Logger`. Callers that
  previously passed a GoAkt logger (e.g. `log.DiscardLogger`) must switch to the new `ego.Logger` interface (e.g.
  `ego.DiscardLogger`). The engine's internal logging calls use `Logger.Debug/Info/Warn/Error` instead of `Debugf/Infof`.

- **🏗️ Internal Extension Constructors** — `extensions.NewProjectionExtension()` now requires an additional
  `DeadLetterHandler` parameter.

- **🏗️ EventSourcedActor Constructor** — `newEventSourcedActor()` no longer accepts arguments. Per-entity
  configuration (snapshot interval, retention policy) is now passed via the `extensions.EntityConfig` dependency,
  ensuring correct behavior during cluster relocation.

- **🌐 Cluster discovery API** — `WithCluster()` now accepts `ego.ClusterProvider` instead of GoAkt’s
  `discovery.Provider`. The engine wraps your implementation when wiring the actor system, so application code no longer
  depends on GoAkt’s discovery package for this option. Implement `ClusterProvider` (`ID`, `Start`, `DiscoverPeers`,
  `Stop`) or add a thin adapter around a GoAkt discovery implementation if you still use one.

### ⬆️ Dependencies

- **Go OpenTelemetry** — `go.opentelemetry.io/otel`, `go.opentelemetry.io/otel/metric`, and
  `go.opentelemetry.io/otel/trace` v1.42.0 promoted from indirect to direct dependencies
- **Publisher modules** (Kafka, NATS, Pulsar, WebSocket) — Import paths updated to v4; no functional changes

### 🧹 Improvements

- **Event proto** — Added `encryption_key_id` and `is_encrypted` fields to `Event` and `Snapshot` protobuf messages
- **Event-sourced actor** — Snapshot-based recovery with configurable intervals reduces event replay overhead
- **Event-sourced actor** — Retention policy cleanup runs after snapshot write, deleting old events and snapshots
- **Event-sourced actor** — Transparent encrypt/decrypt of event and snapshot payloads when an encryptor is configured
- **Projection runner** — Event adapters applied during projection consumption; dead letter forwarding on `RetryAndSkip`
  and `Skip` recovery policies
- **Durable state actor** — Added OpenTelemetry tracing and metrics on command processing
- **Test coverage** — Added `internal/extensions/extensions_test.go` and comprehensive tests for all new packages
- **Testkit** — Added in-memory `SnapshotStore` implementation (`testkit/snapshotstore.go`) for testing
- **Testkit** — Added in-memory `KeyStore` implementation (`testkit/keystore.go`) for encryption testing
- **Projection runner** — Decrypts encrypted events before handing them to the handler and event adapters
- **Projection runner** — Records per-shard lag, offset, and events-behind metrics when telemetry is enabled
- **Clustering** — `ClusterProvider` documentation aligned with engine lifecycle; peer discovery is configured only
  through ego’s `ClusterProvider` surface (no direct `discovery.Provider` on `WithCluster`)

### 📖 Migration Guide

To upgrade from v3 to v4:

1. **Update import paths** — Replace all `github.com/tochemey/ego/v3` imports with `github.com/tochemey/ego/v4`
2. **Update projection handlers** — Remove the `state *anypb.Any` parameter from your `Handle()` implementations
3. **Run the migration utility** — Use `migration.NewMigrator()` to extract inline state from legacy events into the new
   snapshot store
4. **Configure a snapshot store** — Pass a `SnapshotStore` implementation via `WithSnapshotStore()` for optimal recovery
   performance
5. **Update logger usage** — Replace `WithLogger(log.DiscardLogger)` or any `goakt/log.Logger` value with an
   `ego.Logger` implementation (e.g. `ego.DiscardLogger`). If you have a custom GoAkt logger, wrap it in the new
   `Logger` interface instead
6. **Regenerate protobuf** — If you depend on the `Event` message directly, regenerate from the updated `.proto` files
7. **Cluster mode** — Replace `WithCluster(goaktDiscoveryProvider, ...)` with `WithCluster(ego.ClusterProvider, ...)`.
   Map GoAkt’s `Initialize`/`Close` to `Start`/`Stop`, and `DiscoverPeers()` to `DiscoverPeers(ctx)`; `Register`/`Deregister`
   can be no-ops if your backend folds them into start/stop
