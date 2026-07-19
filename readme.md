<!-- markdownlint-disable MD033 MD041 -->

<p align="center">
  <img src="assets/logo.png" alt="eGo" width="480" />
</p>

<p align="center">
  <a href="https://github.com/Tochemey/ego/actions/workflows/build.yml"><img src="https://img.shields.io/github/actions/workflow/status/Tochemey/ego/build.yml?branch=main" alt="Build status"></a>
  <a href="https://pkg.go.dev/github.com/tochemey/ego/v4"><img src="https://pkg.go.dev/badge/github.com/tochemey/ego/v4.svg" alt="Go reference"></a>
  <a href="https://go.dev/doc/install"><img src="https://img.shields.io/github/go-mod/go-version/Tochemey/ego" alt="Go version"></a>
  <a href="https://codecov.io/gh/Tochemey/ego"><img src="https://codecov.io/gh/Tochemey/ego/branch/main/graph/badge.svg?token=Z5b9gM6Mnt" alt="Code coverage"></a>
  <a href="https://github.com/Tochemey/ego/releases/latest"><img src="https://img.shields.io/github/v/release/Tochemey/ego?label=release" alt="Latest release"></a>
  <a href="https://github.com/Tochemey/ego/tags"><img src="https://img.shields.io/github/v/tag/Tochemey/ego?label=tag" alt="Pre-release"></a>
  <a href="https://human-oss.dev"><img src="https://human-oss.dev/badge.svg" alt="Open Source AI Manifesto"></a>
</p>

eGo is a protobuf-first framework for building event-sourced and durable-state CQRS applications in Go. It runs on [Go-Akt](https://github.com/Tochemey/goakt) and adds persistence, projections, publishers, sagas, encryption, and observability to an actor system that your application owns.

eGo deliberately does not hide the actor runtime. Your application creates and operates the Go-Akt actor system, including clustering, discovery, remoting, TLS, supervision, and non-eGo actors. eGo contributes the extensions and actor kinds needed for its persistence model.

## Table of contents

- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Quick start](#quick-start)
- [Modeling entities](#modeling-entities)
    - [Event-sourced vs durable-state](#event-sourced-vs-durable-state)
- [Configuration](#configuration)
- [Snapshots and retention](#snapshots-and-retention)
- [Event batching](#event-batching)
- [Performance tuning](#performance-tuning)
- [Projections](#projections)
- [Publishers](#publishers)
- [Sagas and process managers](#sagas-and-process-managers)
- [Clustering](#clustering)
- [Persistence](#persistence)
- [Encryption and schema evolution](#encryption-and-schema-evolution)
- [Observability](#observability)
- [Reliability and operations](#reliability-and-operations)
- [Testing](#testing)
- [Examples](#examples)
- [Upgrading](#upgrading)
- [Contributing](#contributing)

## Features

- Event-sourced entities with deterministic recovery
- Durable-state entities that persist only the latest state
- Named, independently configured CQRS projections
- Snapshots, retention policies, and event batching
- Event adapters for protobuf schema evolution
- Event and state publishers for Kafka, NATS, Pulsar, and WebSocket
- Saga/process-manager support with compensation
- OpenTelemetry traces and metrics
- AES-256-GCM event and snapshot encryption
- Entity passivation, placement, relocation, and supervision controls
- In-memory stores, behavior scenarios, and generated mocks for testing

## Requirements

- Go 1.26 or later
- Basic familiarity with [Go-Akt](https://github.com/Tochemey/goakt#readme)
- Protobuf messages for commands, events, and state

For production use, provide durable implementations of the stores your application needs. The stores in [`testkit`](./testkit) are intended for tests and local examples.

## Installation

```bash
go get github.com/tochemey/ego/v4
```

## Quick start

Build one `ego.Config`, use it to construct the Go-Akt actor system, start that system, and then plug in the eGo engine:

```go
package main

import (
    "context"
    "log"
    "time"

    accountpb "example.com/myapp/gen/account/v1"
    goakt "github.com/tochemey/goakt/v4/actor"
    "github.com/tochemey/ego/v4"
    "github.com/tochemey/ego/v4/projection"
    "github.com/tochemey/ego/v4/testkit"
)

func main() {
    ctx := context.Background()

    // Use a durable EventsStore in production.
    eventsStore := testkit.NewEventsStore()
    if err := eventsStore.Connect(ctx); err != nil {
        log.Fatal(err)
    }
    defer eventsStore.Disconnect(ctx)

    offsetStore := testkit.NewOffsetStore()
    if err := offsetStore.Connect(ctx); err != nil {
        log.Fatal(err)
    }
    defer offsetStore.Disconnect(ctx)

    cfg := ego.NewConfig(eventsStore,
        ego.WithOffsetStore(offsetStore),
        ego.WithProjection("account-balances", &projection.Options{
            Handler:      NewAccountBalancesProjection(),
            BufferSize:   100,
            PullInterval: 500 * time.Millisecond,
        }),
        ego.WithProjection("account-audit", &projection.Options{
            Handler:      NewAccountAuditProjection(),
            BufferSize:   100,
            PullInterval: time.Second,
        }),
    )

    sys, err := goakt.NewActorSystem("accounts", cfg.GoaktOptions()...)
    if err != nil {
        log.Fatal(err)
    }
    if err := sys.Start(ctx); err != nil {
        log.Fatal(err)
    }
    defer sys.Stop(ctx)

    engine, err := ego.NewEngine(sys, cfg)
    if err != nil {
        log.Fatal(err)
    }
    if err := engine.Start(ctx); err != nil {
        log.Fatal(err)
    }
    defer engine.Stop(ctx)

    if err := engine.StartProjection(ctx, "account-balances"); err != nil {
        log.Fatal(err)
    }
    if err := engine.StartProjection(ctx, "account-audit"); err != nil {
        log.Fatal(err)
    }

    account := NewAccountBehavior("account-123")
    if err := engine.Entity(ctx, account); err != nil {
        log.Fatal(err)
    }

    state, revision, err := engine.SendCommand(
        ctx,
        account.ID(),
        &accountpb.OpenAccount{InitialBalance: 1000},
        5*time.Second,
    )
    if err != nil {
        log.Fatal(err)
    }

    log.Printf("state=%v revision=%d", state, revision)
}
```

`NewEngine` requires a running actor system built with `cfg.GoaktOptions()`. The same `Config` must be passed to both calls so the engine and actor-system extensions stay in sync.

`NewAccountBalancesProjection` and `NewAccountAuditProjection` represent application handlers that each implement `projection.Handler`. Each named projection keeps independent offsets and runtime settings.

The engine does not own the actor system. Stop the engine before stopping the actor system, as the deferred calls above do.

## Modeling entities

All commands, events, and states are protobuf messages.

An event-sourced behavior implements `ego.EventSourcedBehavior`:

```go
type EventSourcedBehavior interface {
    InitialState() ego.State
    HandleCommand(
        context.Context,
        ego.Command,
        ego.State,
    ) ([]ego.Event, error)
    HandleEvent(
        context.Context,
        ego.Event,
        ego.State,
    ) (ego.State, error)
}
```

`HandleCommand` validates a command and returns zero or more events. eGo persists those events before committing the resulting state. `HandleEvent` must be deterministic because it is also used during recovery.

A durable-state behavior implements `ego.DurableStateBehavior`:

```go
type DurableStateBehavior interface {
    InitialState() ego.State
    HandleCommand(
        context.Context,
        ego.Command,
        uint64,
        ego.State,
    ) (newState ego.State, newVersion uint64, err error)
}
```

Configure a state store and spawn the behavior with `DurableStateEntity`:

```go
cfg := ego.NewConfig(nil, ego.WithStateStore(stateStore))

// Build and start the actor system and engine as shown above.
if err := engine.DurableStateEntity(ctx, behavior); err != nil {
    return err
}
```

Behavior values are Go-Akt dependencies. In addition to the methods above, they provide an `ID` and binary marshalling methods so they can travel with cluster spawn requests. See the [event-sourced](./example/eventssourced), [durable-state](./example/durablestate), and [saga](./example/saga) examples for complete implementations.

### Event-sourced vs durable-state

| Aspect            | `EventSourcedBehavior`                    | `DurableStateBehavior`       |
|-------------------|-------------------------------------------|------------------------------|
| Persistence model | Persists domain events                    | Persists the latest state    |
| Recovery          | Replays events, optionally from snapshots | Loads the latest state       |
| History           | Full audit trail                          | No historical log            |
| Complexity        | Higher                                    | Lower                        |
| Best fit          | Traceable, business-critical workflows    | Simpler CRUD-like aggregates |

## Configuration

Engine-wide options are passed to `ego.NewConfig`:

- `WithStateStore` enables durable-state entities.
- `WithSnapshotStore` enables event-sourced snapshots.
- `WithOffsetStore` supplies durable projection offsets.
- `WithProjection` registers a named projection and its handler.
- `WithEventAdapters` applies schema transformations during recovery and projection consumption.
- `WithTelemetry` enables OpenTelemetry instrumentation.
- `WithEncryptor` encrypts persisted event and snapshot payloads.
- `WithEntityKinds` registers behavior types on every cluster node.
- `WithLogger` configures logging for eGo and the underlying actor system.

Entity-specific options are passed when an entity is spawned:

- `WithPassivateAfter`
- `WithRelocation`
- `WithSupervisorDirective`
- `WithPlacement`

Event-sourced entities additionally support `WithSnapshotInterval`, `WithRetentionPolicy`, `WithBatchThreshold`, and `WithBatchFlushWindow`.

API details and defaults are documented on [pkg.go.dev](https://pkg.go.dev/github.com/tochemey/ego/v4).

## Snapshots and retention

Snapshots reduce recovery work by restoring the most recent state and replaying only later events:

```go
cfg := ego.NewConfig(eventsStore,
    ego.WithSnapshotStore(snapshotStore),
)

err := engine.Entity(ctx, behavior,
    ego.WithSnapshotInterval(100),
)
```

A snapshot interval of `0` disables automatic snapshots. Retention runs only after a snapshot has been successfully written:

```go
err := engine.Entity(ctx, behavior,
    ego.WithSnapshotInterval(100),
    ego.WithRetentionPolicy(ego.RetentionPolicy{
        DeleteEventsOnSnapshot:    true,
        DeleteSnapshotsOnSnapshot: true,
        EventsRetentionCount:      200,
    }),
)
```

Your `EventsStore` and `SnapshotStore` implementations must support the corresponding delete operations.

## Event batching

Batching combines events produced by multiple commands into fewer store writes. It is disabled by default:

```go
err := engine.Entity(ctx, behavior,
    ego.WithBatchThreshold(10),
    ego.WithBatchFlushWindow(5*time.Millisecond),
)
```

The threshold or flush window, whichever is reached first, triggers the write. If batching is enabled without a flush window, eGo uses a 5 ms default. Benchmark the settings with your command pattern and persistence backend; batching trades additional latency for fewer writes and does not have one ideal threshold.

## Performance tuning

eGo can sustain hundreds of thousands of commands per second on a single node with an in-memory store, and tens of thousands with durable backends like Postgres. This section outlines the recommended approach to maximize throughput and minimize memory cost.

### Enable event batching under concurrent load

Batching amortizes the cost of a single store write across multiple commands. It is most effective when:

- The entity receives commands concurrently (multiple goroutines or upstream services)
- The persistence store has non-trivial write latency (e.g. database round-trip > 100us)

Sequential command streams do not benefit from batching because each command waits for the flush window before the batch is written. For purely sequential workloads, leave batching disabled (the default).

### Choose the right batch threshold

| Write latency       | Recommended threshold | Rationale                                        |
|---------------------|-----------------------|--------------------------------------------------|
| < 100us (in-memory) | Disabled (0)          | Batching adds overhead with no I/O to amortize   |
| 100us - 1ms         | 5 - 10                | Small batches reduce flush window wait           |
| 1ms - 10ms          | 10 - 50               | Larger batches amortize the I/O cost well        |
| > 10ms              | 50 - 100              | Maximize events per write to offset high latency |

### Minimize allocations for high throughput

eGo's hot path is optimized for low allocation overhead (~22 heap allocations per command round-trip). The dominant allocation cost comes from Protocol Buffers serialization, which is inherent to the persistence model. To keep allocation pressure low:

- **Keep command and event protos small.** Smaller messages reduce marshal/unmarshal cost.
- **Use snapshots.** They reduce recovery replay length and the number of events held in the store.
- **Avoid large state protos.** The state is serialized on every reply; smaller states mean fewer bytes and less GC pressure.

### Scale horizontally with clustering

For workloads beyond what a single node can handle, build a clustered Go-Akt actor system and plug eGo into it as described in [Clustering](#clustering).

## Projections

Each projection has its own name, handler, offsets, and recovery settings. Register projections on the `Config`, then start them after the engine:

```go
cfg := ego.NewConfig(eventsStore,
    ego.WithOffsetStore(offsetStore),
    ego.WithProjection("account-balances", &projection.Options{
        Handler:      accountBalancesHandler,
        BufferSize:   100,
        PullInterval: 500 * time.Millisecond,
        Recovery:     projection.NewRecovery(
            projection.WithRecoveryPolicy(projection.RetryAndFail),
            projection.WithRetries(5),
            projection.WithRetryDelay(time.Second),
        ),
    }),
    ego.WithProjection("account-audit", &projection.Options{
        Handler:           accountAuditHandler,
        BufferSize:        250,
        PullInterval:      time.Second,
        Recovery:          projection.NewRecovery(
            projection.WithRecoveryPolicy(projection.RetryAndSkip),
            projection.WithRetries(3),
            projection.WithRetryDelay(2*time.Second),
        ),
        DeadLetterHandler: accountAuditDeadLetterHandler,
    }),
)

// Build and start the actor system and engine as shown above.
if err := engine.StartProjection(ctx, "account-balances"); err != nil {
    return err
}
if err := engine.StartProjection(ctx, "account-audit"); err != nil {
    return err
}
```

The two projections consume the same event journal independently. Each uses its own handler, buffer, pull interval, recovery policy, dead-letter handling, and offsets.

Projection handlers receive events with at-least-once delivery. They must be idempotent and safe for concurrent calls across different shards; events within one shard are delivered sequentially.

The engine also supports:

- `StopProjection` to stop a running projection
- `IsProjectionRunning` to inspect its runtime state
- `RebuildProjection` to reset offsets and replay from a timestamp
- `ProjectionLag` to report lag by shard
- Recovery policies and dead-letter handlers for processing failures

`Engine.Stop` does not stop projection actors because they belong to the caller-owned Go-Akt actor system. In the normal shutdown sequence, call `engine.Stop(ctx)` and then `sys.Stop(ctx)`; stopping the actor system terminates all projections. If the actor system must remain running, call `engine.StopProjection(ctx, name)` for each projection before stopping the engine.

In a cluster, a projection runs as a singleton. Every node must register the same named projections because the hosting node resolves each handler from its local `Config`.

## Publishers

Call `AddEventPublishers` or `AddStatePublishers` after the engine starts and before producing changes:

```go
if err := engine.AddEventPublishers(eventPublisher); err != nil {
    return err
}
```

eGo includes connector modules for:

- [Kafka](./publisher/kafka)
- [NATS](./publisher/nats)
- [Pulsar](./publisher/pulsar)
- [WebSocket](./publisher/websocket)

You can also implement `ego.EventPublisher` or `ego.StatePublisher`. Publisher payload timestamps are Unix nanoseconds, and each payload includes its source shard.

## Sagas and process managers

eGo includes first-class saga support for long-running business processes that coordinate multiple entities. You can:

- Start a saga with `Engine.Saga(...)`
- Inspect it with `Engine.SagaStatus(...)`
- Model compensation logic for timeouts and failures
- Persist saga state using the same event-sourced foundations

See the [fund-transfer saga example](./example/saga) for a complete implementation.

## Clustering

Cluster, discovery, remoting, and TLS are configured with Go-Akt. Register eGo's actor kinds in the cluster configuration:

```go
clusterConfig := goakt.NewClusterConfig().
    WithDiscovery(discoveryProvider).
    WithDiscoveryPort(gossipPort).
    WithPeersPort(peersPort).
    WithPartitionCount(partitions).
    WithMinimumPeersQuorum(quorum).
    WithReplicaCount(replicas).
    WithKinds(ego.ClusterKinds()...) // eGo's actor kinds, required for relocation
```

Also register every event-sourced, durable-state, and saga behavior type on every node:

```go
cfg := ego.NewConfig(eventsStore,
    ego.WithEntityKinds(
        new(AccountBehavior),
        new(OrderBehavior),
        new(CheckoutSaga),
    ),
)
```

Then compose `cfg.GoaktOptions()` with Go-Akt's cluster, remote, TLS, and application-specific options when constructing the actor system:

```go
sys, err := goakt.NewActorSystem("accounts",
    append(
        cfg.GoaktOptions(),
        goakt.WithCluster(clusterConfig),
        goakt.WithRemote(remote.NewConfig(host, remotingPort)),
        goakt.WithTLS(&tlsInfo),
    )...,
)
```

You retain full control over discovery, partitioning, quorum, replicas, TLS, remoting, and any additional cluster knobs Go-Akt exposes. eGo derives cluster behavior (e.g. running projections as singletons) directly from `sys.InCluster()` at runtime — no separate cluster flag to keep in sync.

Single-node deployments do not need `ClusterKinds` or `WithEntityKinds`.

The [Kubernetes cluster example](./example/cluster) demonstrates a three-node deployment with PostgreSQL, Kubernetes discovery, a singleton projection, OpenTelemetry, Prometheus, Jaeger, and Grafana.

## Persistence

eGo defines small interfaces for:

- [`persistence.EventsStore`](./persistence/events_store.go)
- [`persistence.SnapshotStore`](./persistence/snapshot_store.go)
- [`persistence.StateStore`](./persistence/state_store.go)
- [`offsetstore.OffsetStore`](./offsetstore/offset_store.go)

Applications may implement these interfaces directly. The [ego-contrib](https://github.com/Tochemey/ego-contrib) project provides ready-to-use implementations:

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

Applications own store connectivity: connect stores before starting the actor system and disconnect them after the engine and actor system have stopped.

## Encryption and schema evolution

`WithEncryptor` transparently encrypts event and snapshot payloads before persistence and decrypts them during entity recovery and projection processing. The built-in `encryption.AESEncryptor` uses AES-256-GCM and a pluggable `encryption.KeyStore`.

`WithEventAdapters` registers transformations for events written with older protobuf schemas. Adapters run in registration order during recovery and projection consumption.

## Observability

`WithTelemetry` accepts an OpenTelemetry tracer and meter:

```go
cfg := ego.NewConfig(eventsStore,
    ego.WithTelemetry(&ego.Telemetry{
        Tracer: tracer,
        Meter:  meter,
    }),
)
```

Instrumentation covers command dispatch and handling, event persistence, active entities and projections, projection processing, offsets, lag, and approximate events behind.

## Reliability and operations

eGo includes several production-focused capabilities:

- Faster recovery through [snapshots](#snapshots-and-retention)
- Storage cleanup through [retention policies](#snapshots-and-retention)
- At-rest [encryption](#encryption-and-schema-evolution) for events and snapshots
- GDPR-style erasure with `Engine.EraseEntity(...)`
- Pluggable structured logging via `ego.WithLogger(...)`

## Testing

The [`testkit`](./testkit) package provides in-memory event, snapshot, state, offset, and key stores. Its scenario API tests behavior logic without starting an actor system:

```go
testkit.ForEventSourcedBehavior(behavior).
    Given(previousEvents...).
    When(command).
    ThenEvents(t, expectedEvents...).
    ThenState(t, expectedState)
```

Generated mocks for persistence, encryption, adapters, offsets, and publishers are available under [`mocks`](./mocks).

## Examples

- [Event-sourced entity](./example/eventssourced)
- [Durable-state entity](./example/durablestate)
- [Fund-transfer saga](./example/saga)
- [Three-node Kubernetes cluster](./example/cluster)
- [Benchmarks](./benchmark)

Run the local examples with:

```bash
make run-eventsourced
make run-durablestate
make run-saga
```

## Upgrading

See the [changelog](./CHANGELOG.md) for breaking changes and version-specific migration instructions.

## Contributing

Contributions are welcome. Read the [contribution guide](./contributing.md) before opening a pull request.
