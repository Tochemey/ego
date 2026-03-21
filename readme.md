# eGo

[![build](https://img.shields.io/github/actions/workflow/status/Tochemey/ego/build.yml?branch=main)](https://github.com/Tochemey/ego/actions/workflows/build.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/tochemey/ego.svg)](https://pkg.go.dev/github.com/tochemey/ego)
[![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/tochemey/ego)](https://go.dev/doc/install)
[![GitHub Release](https://img.shields.io/github/v/release/Tochemey/ego)](https://github.com/Tochemey/ego/releases)
[![codecov](https://codecov.io/gh/Tochemey/ego/branch/main/graph/badge.svg?token=Z5b9gM6Mnt)](https://codecov.io/gh/Tochemey/ego)

eGo is a minimal library for building event-sourced and durable-state CQRS applications in Go.
It lets you model your **commands**, **events**, and **state** with Protocol Buffers while relying on
[Go-Akt](https://github.com/Tochemey/goakt) for actor-based execution, clustering, and scalable persistence.

The `main` branch currently targets the `v4` API surface. See the [changelog](./CHANGELOG.md) for the full release
history and migration details.

## 💻 Installation

```bash
go get github.com/tochemey/ego/v4
```

## 🌟 Why eGo

- 🧩 Protobuf-first modeling for commands, events, and state
- ⚡ Event-sourced and durable-state behaviors under one API
- 📚 Built-in support for CQRS projections and event streaming
- 📸 Snapshot-based recovery for faster replay
- 🔄 Event adapters for schema evolution
- 📊 OpenTelemetry-ready tracing and metrics
- 🔐 Encryption support for events and snapshots
- 🔁 Saga/process manager support for long-running workflows
- 🧪 Testkit and mocks for fast feedback

## 🚀 What's New in v4

Version `v4` introduces a major refresh across persistence, observability, operability, and developer experience.

- 📸 **Snapshot Store**: persist snapshots independently from events with `SnapshotStore`, `WithSnapshotStore()`, and
  `WithSnapshotInterval()`
- 🔄 **Event Adapters**: evolve persisted event schemas during replay and projection consumption with the
  `eventadapter` package
- 📊 **OpenTelemetry Integration**: instrument command processing, persistence, projections, and active entities with
  traces and metrics via `WithTelemetry()`
- 💀 **Dead Letter Handler**: capture projection failures after retry exhaustion with `DeadLetterHandler`
- 🔁 **Projection Rebuild**: replay a projection from a chosen point in time using `Engine.RebuildProjection()`
- 🧪 **Scenario-based Testkit**: Given/When/Then APIs for event-sourced and durable-state behaviors
- 🔀 **Migration Utility**: migrate legacy inline state into snapshots with the `migration` package
- 📈 **Projection Lag Monitoring**: inspect lag per shard with `Engine.ProjectionLag()` and dedicated metrics
- 🗑️ **Retention Policies**: automatically clean up old events and snapshots after successful snapshot writes
- 🔐 **Encryption / GDPR Support**: encrypt payloads at rest and erase entity data with `Engine.EraseEntity()`
- 📝 **Pluggable Logger**: use any logging backend through the `Logger` interface and `WithLogger()`
- 🔄 **Saga / Process Manager**: coordinate long-running workflows with compensation support

## 🧱 Core Building Blocks

### 🗂️ Event-Sourced Behavior

`EventSourcedBehavior` is eGo's core abstraction for domain models where state changes are represented as events.
Commands are processed sequentially, resulting events are persisted, and state is rebuilt by replaying those events.

In `v4`, event-sourced entities also benefit from:

- 📸 snapshot-based recovery to reduce replay time
- 🔄 event adapters for schema evolution
- 🗑️ retention policies to control storage growth
- 🔐 transparent encryption of events and snapshots

Typical workflow:

1. Define protobuf messages for state, commands, and events.
2. Implement your `EventSourcedBehavior`.
3. Provide an `EventsStore` and, ideally, a `SnapshotStore`.
4. Configure runtime options such as snapshots, retention, telemetry, or encryption.
5. Start the entity with the eGo engine.

### 💾 Durable-State Behavior

`DurableStateBehavior` is a simpler model for cases where you only need the latest state instead of a full event log.
Each successful command produces a new state version, and only the latest version is persisted.

This is a good fit for:

- ⚙️ configuration-style entities
- 🪶 lower-overhead persistence needs
- 📦 use cases where audit replay is not required

Typical workflow:

1. Define protobuf messages for state and commands.
2. Implement your `DurableStateBehavior`.
3. Provide a durable state store with `WithStateStore()`.
4. Start the entity with the engine.

### ⚖️ Event-Sourced vs Durable-State

| Aspect            | `EventSourcedBehavior`                    | `DurableStateBehavior`       |
|-------------------|-------------------------------------------|------------------------------|
| Persistence model | Persists domain events                    | Persists the latest state    |
| Recovery          | Replays events, optionally from snapshots | Loads the latest state       |
| History           | Full audit trail                          | No historical log            |
| Complexity        | Higher                                    | Lower                        |
| Best fit          | Traceable, business-critical workflows    | Simpler CRUD-like aggregates |

## 📡 Streaming, Projections, and Publishers

eGo pushes persisted domain changes to a stream so read models and integrations can react without reading directly from
the primary store.

### 🔭 Projections

Projections consume the write-model stream and build read models using an offset store.
In `v4`, projections gain several operator-friendly improvements:

- 💀 dead letter handling for unrecoverable projection failures
- 🔁 rebuild support from a chosen timestamp
- 📈 lag, offset, and events-behind metrics per shard
- 🔄 event adapter support during consumption
- 🔐 automatic decryption before handlers process encrypted events

### 📢 Publishers

eGo provides publisher APIs for both write models:

- `EventPublisher` for `EventSourcedBehavior` events
- `StatePublisher` for `DurableStateBehavior` state updates

Available connectors:

- Kafka
- Pulsar
- NATS
- WebSocket

## 🔍 Observability

Observability is a first-class concern in `v4`.
With `WithTelemetry()`, eGo can emit:

- trace spans for command processing
- command counters and latency histograms
- persisted-event counters
- projection processing counters
- active entity and projection counts
- projection lag, latest offset, and approximate events-behind metrics

## 🔐 Reliability and Operations

eGo now includes several production-focused capabilities:

- 📸 faster recovery through snapshots
- 🗑️ storage cleanup through retention policies
- 🔐 at-rest encryption for events and snapshots
- 🧹 GDPR-style erasure with `Engine.EraseEntity()`
- 📝 pluggable structured logging

## 🔄 Sagas and Process Managers

`v4` adds first-class saga support for long-running business processes that coordinate multiple entities.

You can:

- start a saga with `Engine.Saga()`
- inspect it with `Engine.SagaStatus()`
- model compensation logic for timeouts and failures
- persist saga state using the same event-sourced foundations

## 🌐 Clustering

eGo uses [Go-Akt](https://github.com/Tochemey/goakt#clustering) for clustering and entity distribution.
In `v4`, `WithCluster()` accepts `ego.ClusterProvider`, keeping your application code on eGo's abstraction instead of
depending directly on Go-Akt discovery types.

## 🧪 Testkit and Mocks

The [`testkit`](./testkit) package helps you validate behavior quickly with in-memory stores and scenario-based testing.
eGo also ships [`mocks`](./mocks) to simplify isolated unit tests around your stores and integrations.

## 📖 Migration Notes

Upgrading from `v3` to `v4` mainly involves:

1. Updating imports from `github.com/tochemey/ego/v3` to `github.com/tochemey/ego/v4`
2. Removing the `state *anypb.Any` parameter from projection handlers
3. Migrating inline event state into snapshots with the migration utility
4. Configuring a `SnapshotStore` for optimal event-sourced recovery
5. Updating `WithLogger()` usage to the new `ego.Logger` interface
6. Switching cluster integrations to `ego.ClusterProvider`

See [`CHANGELOG.md`](./CHANGELOG.md) for the full breaking-change list.

## 🎯 Examples

Browse the [examples](./example) directory for runnable samples and integration ideas.

## 🤝 Contributing

Please follow the [contribution guide](./contributing.md).
