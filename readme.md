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
  <a href="https://human-oss.dev"><img src="https://human-oss.dev/badge.svg" alt="Open Source AI Manifesto"></a>
</p>

eGo is a protobuf-first framework for building event-sourced and durable-state CQRS applications in Go. It runs on [Go-Akt](https://github.com/Tochemey/goakt) and adds persistence, projections, publishers, sagas, encryption, and observability to an actor system that your application owns.

eGo deliberately does not hide the actor runtime. Your application creates and operates the Go-Akt actor system, including clustering, discovery, remoting, TLS, supervision, and non-eGo actors. eGo contributes the extensions and actor kinds needed for its persistence model.

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

## Clustering

Cluster, discovery, remoting, and TLS are configured with Go-Akt. Register eGo's actor kinds in the cluster configuration:

```go
clusterConfig := goakt.NewClusterConfig().
    WithDiscovery(discoveryProvider).
    WithKinds(ego.ClusterKinds()...)
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

Then compose `cfg.GoaktOptions()` with Go-Akt's cluster, remote, TLS, and application-specific options when constructing the actor system. Single-node deployments do not need `ClusterKinds` or `WithEntityKinds`.

The [Kubernetes cluster example](./example/cluster) demonstrates a three-node deployment with PostgreSQL, Kubernetes discovery, a singleton projection, OpenTelemetry, Prometheus, Jaeger, and Grafana.

## Persistence

eGo defines small interfaces for:

- [`persistence.EventsStore`](./persistence/events_store.go)
- [`persistence.SnapshotStore`](./persistence/snapshot_store.go)
- [`persistence.StateStore`](./persistence/state_store.go)
- [`offsetstore.OffsetStore`](./offsetstore/offset_store.go)

Applications may implement these interfaces directly. The [ego-contrib](https://github.com/Tochemey/ego-contrib) project provides PostgreSQL and MongoDB implementations.

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

## License

eGo is released under the [MIT License](./LICENSE).
