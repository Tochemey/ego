# eGo

[![build](https://img.shields.io/github/actions/workflow/status/Tochemey/ego/build.yml?branch=main)](https://github.com/Tochemey/ego/actions/workflows/build.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/tochemey/ego.svg)](https://pkg.go.dev/github.com/tochemey/ego)
[![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/tochemey/ego)](https://go.dev/doc/install)
![GitHub Release](https://img.shields.io/github/v/release/Tochemey/ego)
[![codecov](https://codecov.io/gh/Tochemey/ego/branch/main/graph/badge.svg?token=Z5b9gM6Mnt)](https://codecov.io/gh/Tochemey/ego)

eGo is a minimal library that helps build event-sourcing and CQRS applications through a simple interface, and it allows
developers to describe their **_commands_**, **_events_** and **_states_** _**are defined using google protocol buffers**_.
Under the hood, ego leverages [Go-Akt](https://github.com/Tochemey/goakt) to scale out and guarantee performant,
reliable persistence.

## Table of Content

- [Installation](#-installation)
- [Versioning](#-versioning)
- [Features](#-features)
    - [Event Sourced Behavior](#event-sourced-behavior)
        - [Command Handlers](#command-handlers)
        - [Event Handlers](#event-handlers)
        - [Howto](#howto)
        - [Events Stream](#events-stream)
        - [Projection](#projection)
        - [Events Store](#events-store)
        - [Offset Store](#offsets-store)
    - [Durable State Behavior](#durable-state-behavior)
        - [State Recovery andPersistence](#state-recovery-and-persistence) 
        - [State Store](#state-store)
        - [Howto](#howto-1)
        - [State Stream](#events-stream-1)
    - [Event Sourced vs. Durable State Behavior](#event-sourced-vs-durable-state-behavior)
    - [Publisher APIs](#publishers)
- [Cluster](#-cluster)
- [Testkit](#-testkit)
- [Mocks](#-mocks)
- [Examples](#-examples)
- [Contribution](#-contribution)

## 💻 Installation

```bash
go get github.com/tochemey/ego/v3
```

> Note: eGo requires **Go 1.25.3** or higher.

## 🔢 Versioning

The version system adopted in eGo deviates a bit from the standard semantic versioning system.
The version format is as follows:

- The `MAJOR` part of the version will stay at `v3` for the meantime.
- The `MINOR` part of the version will cater for any new _features_, _breaking changes_ with a note on the breaking
  changes.
- The `PATCH` part of the version will cater for dependency upgrades, bug fixes, security patches, and co.

The versioning will remain like `v3.x.x` until further notice. The current version is **`v3.6.1`**

## ✨ Features

### Event Sourced Behavior

The [`EventSourcedBehavior`](./behavior.go) is central to maintaining data consistency, particularly in distributed systems. 
It defines how to handle commands—requests to perform actions—which are always directed at the event-sourced entity. 
In eGo, `EventSourcedBehavior` instances are **serializable**, allowing them to be transported over the wire during relocation (e.g., in a clustered environment).

Commands sent to an `EventSourcedBehavior` are processed **sequentially**. 
When a command is handled, it may produce one or more **events**, which are then persisted in an **event store**. 
Each persisted event is tagged with a **revision number** and a **timestamp**, enabling precise tracking and versioning.

The `EventSourcedBehavior` is also responsible for defining how these events are **applied** to update the internal state of the entity. 
The ultimate goal of event handling is to **rebuild the current state** from a history of past events. 
When running in **cluster mode**, aggregate roots are automatically **sharded** for scalability and fault tolerance.

#### Command Handlers
Command handlers define the business logic of the event-sourced actor. They are responsible for:

 - Validating incoming commands against the current state. 
 - Deciding which events, if any, should be generated and persisted. 
 - Returning nil for no-op operations when no state changes are needed.

A command handler acts as the **gatekeeper** of your system’s business rules, ensuring that commands are only applied when valid. 
If validation succeeds, one or more **events** are returned, which express the state mutations. These events are then persisted and applied to produce a **new, valid state**.

#### Event Handlers

Event handlers define how the state should be updated in response to events. 
These functions must be **pure and deterministic**, as they are used both when initially handling commands and when **replaying** the event log to reconstruct the entity’s state.

#### Howto

To define an event-sourced entity, one needs to:

1. define the state of the event-sourced entity using google protocol buffers message
2. define the various commands that will be handled by the event-sourced entity
3. define the various events that are a result of the command handlers and that will be handled by the event sourced
   entity to return the new state of the event-sourced entity
4. define and make sure the [`events store`](./persistence/events_store.go) is properly implemented.
5. implement the [`EventSourcedBehavior`](./behavior.go) interface.
6. call the `Entity` method of eGo [engine](./engine.go)

#### Events Stream

Every event handled by an event-sourced entity is pushed to an events stream. That enables real-time processing of events
without having to interact with the events store.
Just use `Subscribe` method of [Engine](./engine.go) and start iterating through the messages and cast every message to
the [Event](./protos/ego/v3/ego.proto).

#### Projection

One can add a projection to the eGo engine to help build a read model. Projections in eGo rely on an [offset store](#offsets-store) to
track how far they have consumed events
persisted by the write-model. The offset used in eGo is a _timestamp-based offset_. One can also:

- remove a given projection: this will stop the projection and remove it from the system
- check the status of a given projection

#### Events Store

One can implement a custom events store. See [EventsStore](persistence/events_store.go).
There are some pre-built events stores one can use out of the box. See [Contrib](https://github.com/Tochemey/ego-contrib/tree/main/eventstore)

#### Offsets Store

One can implement a custom offsets store. See [OffsetStore](./offsetstore/iface.go).
There are some pre-built offsets stores one can use out of the box. See [Contrib](https://github.com/Tochemey/ego-contrib/tree/main/offsetstore)

### Durable State Behavior

The [`DurableStateBehavior`](./behavior.go) represents a type of actor that **persists its entire state** after processing each command—unlike event-sourced actors, which persist only the events. 
Like its event-sourced counterpart, DurableStateBehavior is **serializable**, meaning the actor can be moved across the network during relocation in distributed systems.

This actor maintains its current state **in memory** while handling commands. Based on the outcome of a command, it **persists the full**, **updated state** to a **durable store** (such as a SQL or NoSQL database). 
The behavior follows a simple and predictable model:

```
 (State, Command) => State
```

Each command results in a **new version** of the actor’s state. Only **the latest version** is stored—there is no retained event history. 
Therefore, `DurableStateBehavior` is suitable for use cases where **audit trails or state reconstruction** are not required.

Although history is not tracked, each state version is tagged with a **version number**. 
This version must **increment by one** with every successful state transition. It is the responsibility of the **command handler** to ensure that the new state has a version exactly one greater than the previous

#### State Recovery and Persistence

- Upon startup, `DurableStateBehavior` will attempt to **recover the last known state** from the durable store. 
- During a graceful shutdown, it **persists the current state** before stopping. 
- This ensures **consistency and resilience**, even in clustered or distributed deployments.

`DurableStateBehavior` is ideal for scenarios where simplicity, low-overhead persistence, and state durability are required—without the complexity of full event sourcing.

#### Durable Store

One can implement a custom state store. See [Durable Store](persistence/state_store.go).
There are some pre-built durable stores one can use out of the box. See [Contrib](https://github.com/Tochemey/ego-contrib/tree/main/durablestore)

#### Howto

To define a durable state entity, one needs to:

1. define the state of the entity using google protocol buffers message
2. define the various commands that will be handled by the entity
3. define and make sure the [`durable state store`](./persistence/state_store.go) is properly implemented.
4. implements the [`DurableStateBehavior`](./behavior.go) interface
5. start eGo engine with the option durable store using `WithStateStore`
6. call the `DurableStateEntity` method of eGo [engine](./engine.go)

### Event Sourced vs. Durable State Behavior

| Aspect                   | `EventSourcedBehavior`                                        | `DurableStateBehavior`                                |
|--------------------------|---------------------------------------------------------------|-------------------------------------------------------|
| **Persistence Model**    | Persists events that describe state changes                   | Persists the full state after each command            |
| **State Reconstruction** | Rebuilds state by replaying stored events                     | Loads the latest persisted state directly             |
| **History Tracking**     | Full event history is retained                                | No event history, only latest state is kept           |
| **Versioning**           | Revision number per event                                     | Version number per full state snapshot                |
| **Command Handlers**     | Produces one or more events from each command                 | Produces a new state directly from each command       |
| **Event Handlers**       | Required to evolve state based on events                      | Not required (no events are emitted)                  |
| **Auditability**         | High – event log can be replayed for audit or debugging       | Low – only the final state is available               |
| **Complexity**           | Higher – requires modeling both events and state evolution    | Lower – simpler, especially for CRUD-style operations |
| **Storage**              | Typically event stores (e.g., Kafka, EventStoreDB)            | SQL, NoSQL, or any key-value store                    |
| **Use Cases**            | Financial ledgers, domain-driven designs, traceable workflows | Caches, configuration entities, simple aggregates     |
| **Recovery**             | Via event replay                                              | Via full state rehydration                            |
| **Serialization**        | Serializable and relocatable                                  | Serializable and relocatable                          |


#### Events Stream

[`DurableStateBehavior`](./behavior.go) full state is pushed to an events stream.
That enables real-time processing of state without having to interact with the state store.
Just use `Subscribe` method of [Engine](./engine.go) and start iterating through the messages and cast every message to
the [DurableState](./protos/ego/v3/ego.proto).

### Publishers

eGo offers the following publisher APIs:

* [EventPublisher](./publisher.go) - publishes `EventSourcedBehavior` events to any streaming platform
* [StatePublisher](./publisher.go) - publishes `DurableStateBehavior` state to any streaming platform

The following streaming connectors are implemented out of the box:

* [Kafka](https://github.com/Tochemey/ego/tree/publisher/kafka/v0.1.0)
* [Pulsar](https://github.com/Tochemey/ego/tree/publisher/pulsar/v0.1.0)
* [NATs](https://github.com/Tochemey/ego/tree/publisher/nats/v0.1.0)
* [Websocket](https://github.com/Tochemey/ego/tree/publisher/websocket/v0.1.0)

## 🌐 Cluster

The cluster mode heavily relies on [Go-Akt](https://github.com/Tochemey/goakt#clustering) clustering. To enable clustering one need to use `WithCluster` option
when creating the eGo engine.

## 🧪 Testkit

eGo comes bundle with in-memory datastore that can be found in the [testkit](./testkit) package. This can help play with eGo.

## 🏗️ Mocks

eGo ships in some [mocks](./mocks) that can help mock the data stores for unit tests purpose.

## 🎯 Examples

Check the [examples](./example)

## 🤲 Contribution

kindly follow the instructions in the [contribution doc](./contributing.md)
