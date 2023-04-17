# Ego

[![build](https://img.shields.io/github/actions/workflow/status/Tochemey/ego/build.yml?branch=main)](https://github.com/Tochemey/ego/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/Tochemey/ego/branch/main/graph/badge.svg?token=Z5b9gM6Mnt)](https://codecov.io/gh/Tochemey/ego)

Ego is a minimal library that help build event-sourcing and CQRS application through a simple interface, and it allows developers to describe their schema with Protobuf.

## Features

- [x] Write Model:
    - [x] Commands handler: The command handlers define how to handle each incoming command,
      which validations must be applied, and finally, which events will be persisted if any. When there is no event to be persisted a nil can
      be returned as a no-op. Command handlers are the meat of the event sourced actor.
      They encode the business rules of your event sourced actor and act as a guardian of the event sourced actor consistency.
      The command handler must first validate that the incoming command can be applied to the current model state.
      Any decision should be solely based on the data passed in the commands and the state of the Behavior.
      In case of successful validation, one or more events expressing the mutations are persisted.
      Once the events are persisted, they are applied to the state producing a new valid state.
    - [x] Events handler: The event handlers are used to mutate the state of the event sourced actor by applying the events to it.
      Event handlers must be pure functions as they will be used when instantiating the event sourced actor and replaying the event store.
    - [x] Extensible events store 
    - [x] Built-in events store
      - [x] Postgres
      - [x] Memory
- [ ] Read Model

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
