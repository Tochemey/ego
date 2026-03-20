# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v4.0.0] - Unreleased

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
