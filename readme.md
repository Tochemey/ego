# eGo

[![build](https://img.shields.io/github/actions/workflow/status/Tochemey/ego/build.yml?branch=main)](https://github.com/Tochemey/ego/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/Tochemey/ego/branch/main/graph/badge.svg?token=Z5b9gM6Mnt)](https://codecov.io/gh/Tochemey/ego)
[![GitHub tag (with filter)](https://img.shields.io/github/v/tag/tochemey/ego)](https://github.com/Tochemey/ego/tags)

eGo is a minimal library that help build event-sourcing and CQRS application through a simple interface, and it allows developers to describe their commands, events and states are defined using google protocol buffers.
Under the hood, ego leverages [goakt](https://github.com/Tochemey/goakt) to scale out and guarantee performant, reliable persistence.

## Features

- Write Model:
    - Commands handler: The command handlers define how to handle each incoming command,
      which validations must be applied, and finally, which events will be persisted if any. When there is no event to be persisted a nil can
      be returned as a no-op. Command handlers are the meat of the event sourced actor.
      They encode the business rules of your event sourced actor and act as a guardian of the Aggregate consistency.
      The command handler must first validate that the incoming command can be applied to the current model state.
      Any decision should be solely based on the data passed in the commands and the state of the Behavior.
      In case of successful validation, one or more events expressing the mutations are persisted. The following replies to a given command are:
        - [StateReply](protos): this message is returned when an event is the product of the command handler. The message contains:
            - the entity id
            - the resulting state
            - the actual event to be persisted
            - the sequence number
            - the event timestamp
        - [NoReply](protos): this message is returned when the command does not need a reply.
        - [ErrorReply](protos): is used when a command processing has failed. This message contains the error message.
          Once the events are persisted, they are applied to the state producing a new valid state.
    - Events handler: The event handlers are used to mutate the state of the Aggregate by applying the events to it.
      Event handlers must be pure functions as they will be used when instantiating the Aggregate and replaying the event store.
    - Extensible events store
    - Built-in events store
        - Postgres
        - Memory (only for testing purpose)
    - [Cluster Mode](https://github.com/Tochemey/goakt#clustering)
- Examples (check the [examples](./example))

## Installation

```bash
go get github.com/tochemey/ego
```

## Contribution

Contributions are welcome!
The project adheres to [Semantic Versioning](https://semver.org) and [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/).
This repo uses [Earthly](https://earthly.dev/get-earthly).

To contribute please:

- Fork the repository
- Create a feature branch
- Submit a [pull request](https://help.github.com/articles/using-pull-requests)

### Test & Linter

Prior to submitting a [pull request](https://help.github.com/articles/using-pull-requests), please run:

```bash
earthly +test
```
