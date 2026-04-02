<p align="center">
  <img src="assets/logo.png" alt="eGo Logo" width="480" />
</p>

<p align="center">
  <a href="https://github.com/Tochemey/ego/actions/workflows/build.yml"><img src="https://img.shields.io/github/actions/workflow/status/Tochemey/ego/build.yml?branch=main" alt="build"></a>
  <a href="https://pkg.go.dev/github.com/tochemey/ego"><img src="https://pkg.go.dev/badge/github.com/tochemey/ego.svg" alt="Go Reference"></a>
  <a href="https://go.dev/doc/install"><img src="https://img.shields.io/github/go-mod/go-version/tochemey/ego" alt="Go version"></a>
  <a href="https://github.com/Tochemey/ego/releases"><img src="https://img.shields.io/github/v/release/tochemey/ego?sort=semver&display_name=release" alt="GitHub Release"></a>
  <a href="https://codecov.io/gh/Tochemey/ego"><img src="https://codecov.io/gh/Tochemey/ego/branch/main/graph/badge.svg?token=Z5b9gM6Mnt" alt="codecov"></a>
</p>

eGo is a minimal library for building event-sourced and durable-state CQRS applications in Go.
It lets you model your **commands**, **events**, and **state** with Protocol Buffers while relying on
[Go-Akt](https://github.com/Tochemey/goakt) for actor-based execution, clustering, and scalable persistence.

The `main` branch currently targets the `v4` API surface. See the [changelog](./CHANGELOG.md) for the full release
history and migration details.

## Installation

```bash
go get github.com/tochemey/ego/v4
```

## Why eGo

- Protobuf-first modeling for commands, events, and state
- Event-sourced and durable-state behaviors under one API
- Built-in support for CQRS projections and event streaming
- Snapshot-based recovery for faster replay
- Event batching to amortize persistence cost across commands
- Event adapters for schema evolution
- OpenTelemetry-ready tracing and metrics
- Encryption support for events and snapshots
- Saga/process manager support for long-running workflows
- Testkit and mocks for fast feedback

## What's New in v4

Version `v4` introduces a major refresh across persistence, observability, operability, and developer experience.

- **Snapshot Store**: persist snapshots independently from events with `SnapshotStore`, `WithSnapshotStore()`, and
  `WithSnapshotInterval()`
- **Event Batching**: amortize store round-trips by accumulating events across multiple commands and flushing them in a
  single write with `WithBatchThreshold()` and `WithBatchFlushWindow()`
- **Event Adapters**: evolve persisted event schemas during replay and projection consumption with the
  `eventadapter` package
- **OpenTelemetry Integration**: instrument command processing, persistence, projections, and active entities with
  traces and metrics via `WithTelemetry()`
- **Dead Letter Handler**: capture projection failures after retry exhaustion with `DeadLetterHandler`
- **Projection Rebuild**: replay a projection from a chosen point in time using `Engine.RebuildProjection()`
- **Scenario-based Testkit**: Given/When/Then APIs for event-sourced and durable-state behaviors
- **Migration Utility**: migrate legacy inline state into snapshots with the `migration` package
- **Projection Lag Monitoring**: inspect lag per shard with `Engine.ProjectionLag()` and dedicated metrics
- **Retention Policies**: automatically clean up old events and snapshots after successful snapshot writes
- **Encryption / GDPR Support**: encrypt payloads at rest and erase entity data with `Engine.EraseEntity()`
- **Pluggable Logger**: use any logging backend through the `Logger` interface and `WithLogger()`
- **Saga / Process Manager**: coordinate long-running workflows with compensation support

## Core

### Event-Sourced Behavior

`EventSourcedBehavior` is eGo's core abstraction for domain models where state changes are represented as events.
Commands are processed sequentially, resulting events are persisted, and state is rebuilt by replaying those events.

In `v4`, event-sourced entities also benefit from:

- Snapshot-based recovery to reduce replay time
- Event batching for higher throughput under sustained command load
- Event adapters for schema evolution
- Retention policies to control storage growth
- Transparent encryption of events and snapshots

Typical workflow:

1. Define protobuf messages for state, commands, and events.
2. Implement your `EventSourcedBehavior`.
3. Provide an `EventsStore` and, ideally, a `SnapshotStore`.
4. Configure runtime options such as snapshots, batching, retention, telemetry, or encryption.
5. Start the entity with the eGo engine.

#### Event Batching

When an entity handles a high volume of commands, each individual store write becomes a throughput bottleneck.
Event batching addresses this by accumulating events from multiple commands and flushing them in a single write.

Enable batching with two spawn options:

```go
engine.Entity(ctx, behavior,
    ego.WithBatchThreshold(10),          // flush after 10 commands
    ego.WithBatchFlushWindow(5*time.Millisecond), // or after 5ms, whichever comes first
)
```

While a batch is being written, the actor stashes incoming commands and replays them once the write completes.
Commands are processed optimistically against pending state, so callers do not observe any ordering anomalies.

A threshold of `0` (the default) disables batching entirely, preserving the one-command-one-write behavior.

### Durable-State Behavior

`DurableStateBehavior` is a simpler model for cases where you only need the latest state instead of a full event log.
Each successful command produces a new state version, and only the latest version is persisted.

This is a good fit for:

- Configuration-style entities
- Lower-overhead persistence needs
- Use cases where audit replay is not required

Typical workflow:

1. Define protobuf messages for state and commands.
2. Implement your `DurableStateBehavior`.
3. Provide a durable state store with `WithStateStore()`.
4. Start the entity with the engine.

### Event-Sourced vs Durable-State

| Aspect            | `EventSourcedBehavior`                    | `DurableStateBehavior`       |
|-------------------|-------------------------------------------|------------------------------|
| Persistence model | Persists domain events                    | Persists the latest state    |
| Recovery          | Replays events, optionally from snapshots | Loads the latest state       |
| History           | Full audit trail                          | No historical log            |
| Complexity        | Higher                                    | Lower                        |
| Best fit          | Traceable, business-critical workflows    | Simpler CRUD-like aggregates |

## Persistence Stores

eGo ships with in-memory stores for testing, but production deployments require durable persistence backends.
The [ego-contrib](https://github.com/Tochemey/ego-contrib) repository provides ready-to-use store implementations:

- **Postgres** event store, snapshot store, offset store, and durable state store
- **MongoDB** event store, snapshot store, offset store, and durable state store

To use a contrib store, import the relevant module alongside eGo:

```go
import (
    "github.com/tochemey/ego-contrib/eventstore/postgres"
    "github.com/tochemey/ego-contrib/snapshotstore/postgres"
    "github.com/tochemey/ego-contrib/offsetstore/postgres"
)
```

> You can also implement `persistence.EventsStore`, `persistence.SnapshotStore`, or `persistence.StateStore`
> directly to plug in any storage backend.

## Performance Tuning

eGo can sustain hundreds of thousands of commands per second on a single node with an in-memory store, and
tens of thousands with durable backends like Postgres. This section outlines the recommended approach to maximize
throughput and minimize memory cost.

### Enable event batching under concurrent load

Batching amortizes the cost of a single store write across multiple commands. It is most effective when:

- The entity receives commands concurrently (multiple goroutines or upstream services)
- The persistence store has non-trivial write latency (e.g. database round-trip > 100us)

```go
engine.Entity(ctx, behavior,
    ego.WithBatchThreshold(10),                    // flush after 10 accumulated events
    ego.WithBatchFlushWindow(5*time.Millisecond),  // or after 5ms, whichever comes first
)
```

Sequential command streams do not benefit from batching because each command waits for the flush window
before the batch is written. For purely sequential workloads, leave batching disabled (the default).

### Choose the right batch threshold

| Write latency       | Recommended threshold | Rationale                                        |
|---------------------|-----------------------|--------------------------------------------------|
| < 100us (in-memory) | Disabled (0)          | Batching adds overhead with no I/O to amortize   |
| 100us - 1ms         | 5 - 10                | Small batches reduce flush window wait           |
| 1ms - 10ms          | 10 - 50               | Larger batches amortize the I/O cost well        |
| > 10ms              | 50 - 100              | Maximize events per write to offset high latency |

### Configure snapshots to reduce recovery time

Without snapshots, recovery replays the full event history. Configure a snapshot interval to bound replay length:

```go
engine.Entity(ctx, behavior,
    ego.WithSnapshotInterval(100),  // snapshot every 100 events
)
```

Pair this with a `SnapshotStore` via `ego.WithSnapshotStore()` on the engine.

### Add retention policies to control storage growth

When snapshots are enabled, old events and snapshots can be cleaned up automatically:

```go
engine.Entity(ctx, behavior,
    ego.WithSnapshotInterval(100),
    ego.WithRetentionPolicy(ego.RetentionPolicy{
        DeleteEventsOnSnapshot:    true,
        DeleteSnapshotsOnSnapshot: true,
        EventsRetentionCount:     200,  // keep 200 most recent events
    }),
)
```

### Minimize allocations for high throughput

eGo's hot path is optimized for low allocation overhead (~22 heap allocations per command round-trip).
The dominant allocation cost comes from Protocol Buffers serialization, which is inherent to the persistence model.
To keep allocation pressure low:

- **Keep command and event protos small.** Smaller messages reduce marshal/unmarshal cost.
- **Use snapshots.** They reduce recovery replay length and the number of events held in the store.
- **Avoid large state protos.** The state is serialized on every reply; smaller states mean fewer bytes and less GC pressure.

### Scale horizontally with clustering

For workloads beyond what a single node can handle, enable clustering to distribute entities across nodes:

```go
engine := ego.NewEngine("myapp", eventStore,
    ego.WithCluster(provider, partitions, replicaCount, host, remotingPort, gossipPort, clusterPort),
)
```

Each entity is hashed to a partition and placed on a node. Commands are routed transparently. See the
[clustering documentation](https://github.com/Tochemey/goakt#clustering) for details on discovery providers
and topology configuration.

## Projections & Publishers

eGo pushes persisted domain changes to a stream so read models and integrations can react without reading directly from
the primary store.

### Projections

Projections consume the write-model stream and build read models using an offset store.
In `v4`, projections gain several operator-friendly improvements:

- Dead letter handling for unrecoverable projection failures
- Rebuild support from a chosen timestamp
- Lag, offset, and events-behind metrics per shard
- Event adapter support during consumption
- Automatic decryption before handlers process encrypted events

### Publishers

eGo provides publisher APIs for both write models:

- `EventPublisher` for `EventSourcedBehavior` events
- `StatePublisher` for `DurableStateBehavior` state updates

Available connectors:

- Kafka
- Pulsar
- NATS
- WebSocket

## Observability

Observability is a first-class concern in `v4`.
With `WithTelemetry()`, eGo can emit:

- Trace spans for command processing
- Command counters and latency histograms
- Persisted-event counters
- Projection processing counters
- Active entity and projection counts
- Projection lag, latest offset, and approximate events-behind metrics

## Reliability & Operations

eGo includes several production-focused capabilities:

- Faster recovery through snapshots
- Storage cleanup through retention policies
- At-rest encryption for events and snapshots
- GDPR-style erasure with `Engine.EraseEntity()`
- Pluggable structured logging

## Sagas & Process Managers

`v4` adds first-class saga support for long-running business processes that coordinate multiple entities.

You can:

- Start a saga with `Engine.Saga()`
- Inspect it with `Engine.SagaStatus()`
- Model compensation logic for timeouts and failures
- Persist saga state using the same event-sourced foundations

## Clustering

eGo uses [Go-Akt](https://github.com/Tochemey/goakt#clustering) for clustering and entity distribution.
In `v4`, `WithCluster()` accepts `ego.ClusterProvider`, keeping your application code on eGo's abstraction instead of
depending directly on Go-Akt discovery types.

## Testkit & Mocks

The [`testkit`](./testkit) package helps you validate behavior quickly with in-memory stores and scenario-based testing.
eGo also ships [`mocks`](./mocks) to simplify isolated unit tests around your stores and integrations.

## Migration Notes

Upgrading from `v3` to `v4` mainly involves:

1. Updating imports from `github.com/tochemey/ego/v3` to `github.com/tochemey/ego/v4`
2. Removing the `state *anypb.Any` parameter from projection handlers
3. Migrating inline event state into snapshots with the migration utility
4. Configuring a `SnapshotStore` for optimal event-sourced recovery
5. Updating `WithLogger()` usage to the new `ego.Logger` interface
6. Switching cluster integrations to `ego.ClusterProvider`

See [`CHANGELOG.md`](./CHANGELOG.md) for the full breaking-change list.

## Examples

Browse the [examples](./example) directory for runnable samples and integration ideas.

## Contributions

Please follow the [contribution guide](./contributing.md).
