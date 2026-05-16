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

eGo is a **meta-framework** for building event-sourced and durable-state CQRS applications in Go using **protocol buffers** for _commands_, _events_ and _states_.
It rides on top of [Go-Akt](https://github.com/Tochemey/goakt) and contributes its event-sourcing, durable-state,
projection, and saga primitives as Go-Akt extensions. The actor system itself — including clustering, TLS,
remoting, discovery, supervision strategies, and any non-eGo actors — is built and operated directly by the
developer using Go-Akt's APIs.

> **Prerequisite.** You should be comfortable with the basics of [Go-Akt](https://github.com/Tochemey/goakt)
> before adopting eGo. At a minimum, you should know how to:
> - construct a `goakt.ActorSystem` with `goakt.NewActorSystem(name, opts...)`,
> - start and stop it with `sys.Start(ctx)` / `sys.Stop(ctx)`,
> - configure clustering with `goakt.NewClusterConfig(...).WithKinds(...)`,
> - spawn your own actors when needed.
>
If those are new, skim the Go-Akt [readme](https://github.com/Tochemey/goakt#readme) first.
the rest of this document assumes them.
> 
## Installation

```bash
go get github.com/tochemey/ego/v4
```

## Table of Contents

- [Installation](#installation)
- [Why eGo](#why-ego)
- [Quick Start](#quick-start)
    - [Bootstrap](#bootstrap)
    - [Cluster mode](#cluster-mode)
- [Features](#features)
    - [Event-Sourced Behavior](#event-sourced-behavior)
        - [Event Batching](#event-batching)
    - [Durable-State Behavior](#durable-state-behavior)
    - [Event-Sourced vs Durable-State](#event-sourced-vs-durable-state)
- [Persistence Stores](#persistence-stores)
- [Performance Tuning](#performance-tuning)
    - [Enable event batching under concurrent load](#enable-event-batching-under-concurrent-load)
    - [Choose the right batch threshold](#choose-the-right-batch-threshold)
    - [Configure snapshots to reduce recovery time](#configure-snapshots-to-reduce-recovery-time)
    - [Add retention policies to control storage growth](#add-retention-policies-to-control-storage-growth)
    - [Minimize allocations for high throughput](#minimize-allocations-for-high-throughput)
    - [Scale horizontally with clustering](#scale-horizontally-with-clustering)
- [Projections & Publishers](#projections--publishers)
    - [Projections](#projections)
    - [Publishers](#publishers)
- [Observability](#observability)
- [Reliability & Operations](#reliability--operations)
- [Sagas & Process Managers](#sagas--process-managers)
- [Testkit & Mocks](#testkit--mocks)
- [Migrating From Earlier v4 Releases](#migrating-from-earlier-v4-releases)
- [Examples](#examples)
- [Contributions](#contributions)

## Why eGo

- Protobuf-first modeling for commands, events, and state
- Event-sourced and durable-state behaviors under one API
- Built-in support for CQRS projections and event streaming
- Snapshot-based recovery for faster replay
- Event batching to amortize persistence cost across commands
- Event adapters for schema evolution
- OpenTelemetry-ready tracing and metrics
- Encryption support for events and snapshots
- Saga / process manager support for long-running workflows
- Testkit and mocks for fast feedback
- **Composes cleanly with non-eGo actors on the same Go-Akt actor system**

## Quick Start

### Bootstrap

eGo plugs into a Go-Akt actor system that you construct and own. The shape is the same whether you're running
locally or in a cluster:

```go
import (
    "context"

    goakt "github.com/tochemey/goakt/v4/actor"
    "github.com/tochemey/ego/v4"
    "github.com/tochemey/ego/v4/testkit"
)

ctx := context.Background()

// 1. Build the eGo Config once. It captures every eGo-specific concern —
//    stores, projections, telemetry, encryption, ... — in a single object
//    that is then used both to build the actor system and to plug in the
//    engine.
eventsStore := testkit.NewEventsStore()
config := ego.NewConfig(eventsStore,
    ego.WithLogger(ego.DefaultLogger),
    // ego.WithStateStore(...), ego.WithOffsetStore(...), ego.WithProjection(...), etc.
)

// 2. Build the actor system. config.GoaktOptions() returns the goakt.Options
//    eGo needs (extensions for the events store / event stream / other
//    configured stores, pubsub, the logger adapter, the default supervisor).
sys, err := goakt.NewActorSystem("MyApp", config.GoaktOptions()...)
if err != nil { /* ... */ }

if err := sys.Start(ctx); err != nil { /* ... */ }
defer sys.Stop(ctx)

// 3. (Optional) spawn your own actors on the same system.
//    They live next to eGo's entities and share the same runtime.
// _, _ = sys.Spawn(ctx, "user-service", NewUserActor())

// 4. Plug eGo in. NewEngine validates that every required extension is registered
//    on `sys`, returning an error if anything is missing.
engine, err := ego.NewEngine(sys, config)
if err != nil { /* ... */ }

if err := engine.Start(ctx); err != nil { /* ... */ }
defer engine.Stop(ctx)
```

Two things to note:

- **`engine.Stop(ctx)` does not stop the actor system.** The actor system is your resource; you stop it.
- **The engine name comes from `sys.Name()`.** There is no separate name parameter on `NewEngine`.

### Cluster mode

Cluster mode is configured the Go-Akt way. eGo only contributes the four actor kinds it needs registered for
relocation:

```go
import (
    "github.com/tochemey/goakt/v4/discovery"  // or your discovery provider package
    goakt "github.com/tochemey/goakt/v4/actor"
    "github.com/tochemey/goakt/v4/remote"
    "github.com/tochemey/ego/v4"
)

clusterCfg := goakt.NewClusterConfig().
    WithDiscovery(provider).
    WithDiscoveryPort(gossipPort).
    WithPeersPort(peersPort).
    WithPartitionCount(partitions).
    WithMinimumPeersQuorum(quorum).
    WithReplicaCount(replicas).
    WithKinds(ego.ClusterKinds()...). // ← eGo's actor kinds, required for relocation
    // ... your own kinds here, alongside eGo's

config := ego.NewConfig(eventsStore, opts...)

sys, _ := goakt.NewActorSystem("MyApp",
    append(
        config.GoaktOptions(),
        goakt.WithCluster(clusterCfg),
        goakt.WithRemote(remote.NewConfig(host, remotingPort)),
        goakt.WithTLS(&tlsInfo),
    )...,
)
sys.Start(ctx)
defer sys.Stop(ctx)

engine, _ := ego.NewEngine(sys, config)
engine.Start(ctx)
defer engine.Stop(ctx)
```

You retain full control over discovery, partitioning, quorum, replicas, TLS, remoting, and any additional
cluster knobs Go-Akt exposes. eGo derives cluster behavior (e.g. running projections as singletons) directly
from `sys.InCluster()` at runtime — no separate cluster flag to keep in sync.

## Features

### Event-Sourced Behavior

`EventSourcedBehavior` is eGo's core abstraction for domain models where state changes are represented as
events. Commands are processed sequentially, resulting events are persisted, and state is rebuilt by replaying
those events.

Event-sourced entities also benefit from:

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
5. Bootstrap the actor system and the engine as shown above.
6. Start the entity with `engine.Entity(ctx, behavior, ...)`.

#### Event Batching

When an entity handles a high volume of commands, each individual store write becomes a throughput bottleneck.
Event batching addresses this by accumulating events from multiple commands and flushing them in a single write.

Enable batching with two spawn options:

```go
engine.Entity(ctx, behavior,
    ego.WithBatchThreshold(10),                    // flush after 10 commands
    ego.WithBatchFlushWindow(5*time.Millisecond),  // or after 5ms, whichever comes first
)
```

While a batch is being written, the actor stashes incoming commands and replays them once the write completes.
Commands are processed optimistically against pending state, so callers do not observe any ordering anomalies.

A threshold of `0` (the default) disables batching entirely, preserving the one-command-one-write behavior.

### Durable-State Behavior

`DurableStateBehavior` is a simpler model for cases where you only need the latest state instead of a full
event log. Each successful command produces a new state version, and only the latest version is persisted.

This is a good fit for:

- Configuration-style entities
- Lower-overhead persistence needs
- Use cases where audit replay is not required

Typical workflow:

1. Define protobuf messages for state and commands.
2. Implement your `DurableStateBehavior`.
3. Provide a durable state store with `ego.WithStateStore(...)`.
4. Start the entity with `engine.DurableStateEntity(ctx, behavior, ...)`.

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
The [ego-contrib](https://github.com/Tochemey/ego-contrib) repository provides ready-to-use store
implementations:

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

You can also implement `persistence.EventsStore`, `persistence.SnapshotStore`, or `persistence.StateStore`
directly to plug in any storage backend.
> ## Performance Tuning
> 

eGo can sustain hundreds of thousands of commands per second on a single node with an in-memory store, and
tens of thousands with durable backends like Postgres. This section outlines the recommended approach to
maximize throughput and minimize memory cost.

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

Without snapshots, recovery replays the full event history. Configure a snapshot interval to bound replay
length:

```go
engine.Entity(ctx, behavior,
    ego.WithSnapshotInterval(100),  // snapshot every 100 events
)
```

Pair this with a `SnapshotStore` via `ego.WithSnapshotStore(...)` on the engine options.

### Add retention policies to control storage growth

When snapshots are enabled, old events and snapshots can be cleaned up automatically:

```go
engine.Entity(ctx, behavior,
    ego.WithSnapshotInterval(100),
    ego.WithRetentionPolicy(ego.RetentionPolicy{
        DeleteEventsOnSnapshot:    true,
        DeleteSnapshotsOnSnapshot: true,
        EventsRetentionCount:      200, // keep 200 most recent events
    }),
)
```

### Minimize allocations for high throughput

eGo's hot path is optimized for low allocation overhead (~22 heap allocations per command round-trip).
The dominant allocation cost comes from Protocol Buffers serialization, which is inherent to the persistence
model. To keep allocation pressure low:

- **Keep command and event protos small.** Smaller messages reduce marshal/unmarshal cost.
- **Use snapshots.** They reduce recovery replay length and the number of events held in the store.
- **Avoid large state protos.** The state is serialized on every reply; smaller states mean fewer bytes and
  less GC pressure.

### Scale horizontally with clustering

See the [Cluster mode](#cluster-mode) section. For workloads beyond what a single node can handle, build a
clustered Go-Akt actor system and plug eGo into it the same way you do for the local case.

## Projections & Publishers

eGo pushes persisted domain changes to a stream so read models and integrations can react without reading
directly from the primary store.

### Projections

Projections consume the write-model stream and build read models using an offset store. They support:

- Dead letter handling for unrecoverable projection failures
- Rebuild support from a chosen timestamp via `Engine.RebuildProjection(...)`
- Lag, offset, and events-behind metrics per shard
- Event adapter support during consumption
- Automatic decryption before handlers process encrypted events

In cluster mode, projections run as singletons on the oldest node — eGo detects cluster mode via
`sys.InCluster()` at registration time, so you don't need to tell it twice.

### Publishers

eGo provides publisher APIs for both write models:

- `EventPublisher` for `EventSourcedBehavior` events
- `StatePublisher` for `DurableStateBehavior` state updates

Custom publisher implementations receive `*egopb.Event` / `*egopb.DurableState`
whose `Timestamp` field is populated via `time.Now().UnixNano()` — Unix epoch
nanoseconds. The shard the event was produced from travels in the same
payload (`Event.Shard`, `DurableState.Shard`) so downstream consumers can
filter or partition by shard without depending on internal pub/sub topic
naming.

Available connectors:

- Kafka
- Pulsar
- NATS
- WebSocket

## Observability

With `ego.WithTelemetry(...)` eGo emits:

- Trace spans for command processing
- Command counters and latency histograms
- Persisted-event counters
- Projection processing counters
- Active entity and projection counts
- Projection lag, latest offset, and approximate events-behind metrics

The telemetry option produces a Go-Akt extension automatically; it is included in `cfg.GoaktOptions()`
when set, so the same actor-system bootstrap covers it.

## Reliability & Operations

eGo includes several production-focused capabilities:

- Faster recovery through snapshots
- Storage cleanup through retention policies
- At-rest encryption for events and snapshots
- GDPR-style erasure with `Engine.EraseEntity(...)`
- Pluggable structured logging via `ego.WithLogger(...)`

## Sagas & Process Managers

eGo includes first-class saga support for long-running business processes that coordinate multiple entities.

You can:

- Start a saga with `Engine.Saga(...)`
- Inspect it with `Engine.SagaStatus(...)`
- Model compensation logic for timeouts and failures
- Persist saga state using the same event-sourced foundations

## Testkit & Mocks

The [`testkit`](./testkit) package helps you validate behavior quickly with in-memory stores and
scenario-based testing. eGo also ships [`mocks`](./mocks) to simplify isolated unit tests around your stores
and integrations.

## Migrating From Earlier v4 Releases

If you're upgrading from a `v4.1.x` release where eGo built and managed the actor system, the bootstrap
reshapes from:

```go
// before
engine := ego.NewEngine("myapp", eventsStore,
    ego.WithLogger(logger),
    ego.WithStateStore(stateStore),
    // ego.WithClusterOption(...) / ego.WithTLS(...) / etc.
)
engine.Start(ctx)
```

to:

```go
// after
cfg := ego.NewConfig(eventsStore,
    ego.WithLogger(logger),
    ego.WithStateStore(stateStore),
)

sys, _ := goakt.NewActorSystem("myapp",
    append(
        cfg.GoaktOptions(),
        // goakt.WithCluster(clusterCfg.WithKinds(ego.ClusterKinds()...)),
        // goakt.WithTLS(&tlsInfo),
        // goakt.WithRemote(remote.NewConfig(...)),
    )...,
)
sys.Start(ctx)
defer sys.Stop(ctx)

engine, _ := ego.NewEngine(sys, cfg)
engine.Start(ctx)
defer engine.Stop(ctx)
```

Cluster, TLS, and remoting configuration moves from eGo options to Go-Akt ones; the previous
`ego.WithClusterOption`, `ego.WithTLS`, `ego.WithRoles`, `ego.WithClusterConfigurator`, and
`ego.WithActorSystemOptions` / `ego.WithRemoteOptions` are removed. See
[`CHANGELOG.md`](./CHANGELOG.md) for the full removal list.

In addition to the bootstrap reshape, this release also changes the **resolution** of the `Timestamp`
field on `egopb.Event`, `egopb.DurableState`, `egopb.Snapshot`, and `egopb.StateReply` from Unix
seconds to Unix **nanoseconds** (the underlying `int64` is unchanged). Custom event publishers,
external dashboards, or ad-hoc SQL reading the raw `timestamp` column must be updated accordingly
(for example, `to_timestamp(timestamp / 1e9)` in Postgres instead of `to_timestamp(timestamp)`).
This is the underlying fix for a projection-runner race where events written in the same second as
the projection's committed offset could be silently skipped between polls — see
[`CHANGELOG.md`](./CHANGELOG.md) for the details.

## Examples

Browse the [examples](./example) directory for runnable samples and integration ideas.

## Contributions

Please follow the [contribution guide](./contributing.md).
