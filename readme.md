## eGo

[![build](https://img.shields.io/github/actions/workflow/status/Tochemey/ego/build.yml?branch=main)](https://github.com/Tochemey/ego/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/Tochemey/ego/branch/main/graph/badge.svg?token=Z5b9gM6Mnt)](https://codecov.io/gh/Tochemey/ego)
[![GitHub tag (with filter)](https://img.shields.io/github/v/tag/tochemey/ego)](https://github.com/Tochemey/ego/tags)

eGo is a minimal library that help build event-sourcing and CQRS application through a simple interface, and it allows developers to describe their commands, events and states are defined using google protocol buffers.
Under the hood, ego leverages [goakt](https://github.com/Tochemey/goakt) to scale out and guarantee performant, reliable persistence.

### Features

- Write Model:
    - Commands handler: The command handlers define how to handle each incoming command,
      which validations must be applied, and finally, which events will be persisted if any. When there is no event to be persisted a nil can
      be returned as a no-op. Command handlers are the meat of the event sourced actor.
      They encode the business rules of your event sourced actor and act as a guardian of the Aggregate consistency.
      The command handler must first validate that the incoming command can be applied to the current model state.
      Any decision should be solely based on the data passed in the commands and the state of the Behavior.
      In case of successful validation, one or more events expressing the mutations are persisted. Once the events are persisted, they are applied to the state producing a new valid state.
    - Events handler: The event handlers are used to mutate the state of the Aggregate by applying the events to it.
      Event handlers must be pure functions as they will be used when instantiating the Aggregate and replaying the event store.
    - Extensible events store
    - Built-in events store:
        - Postgres: Schema can be found [here](./resources/eventstore_postgres.sql)
        - Memory (for testing purpose only)
    - [Cluster Mode](https://github.com/Tochemey/goakt#clustering)
- Read Model:
    - Projection: The engine that help build a read model
    - Extensible Offset store: Helps store offsets of events consumed and processed by projections
    - Built-in offset stores:
        - Postgres: Schema can be found [here](./resources/offsetstore_postgres.sql)
        - Memory (for testing purpose only)
- Examples (check the [examples](./example))

### Installation

```bash
go get github.com/tochemey/ego
```

### Sample

```go
package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/google/uuid"
	"github.com/tochemey/ego"
	"github.com/tochemey/ego/eventstore/memory"
	samplepb "github.com/tochemey/ego/example/pbs/sample/pb/v1"
	"google.golang.org/protobuf/proto"
)

func main() {
	// create the go context
	ctx := context.Background()
	// create the event store
	eventStore := memory.NewEventsStore()
	// create the ego engine
	e := ego.NewEngine("Sample", eventStore)
	// start ego engine
	_ = e.Start(ctx)
	// create a persistence id
	entityID := uuid.NewString()
	// create an entity behavior with a given id
	behavior := NewAccountBehavior(entityID)
	// create an entity
	entity, _ := ego.NewEntity[*samplepb.Account](ctx, behavior, e)

	// send some commands to the pid
	var command proto.Message
	// create an account
	command = &samplepb.CreateAccount{
		AccountId:      entityID,
		AccountBalance: 500.00,
	}
	// send the command to the actor. Please don't ignore the error in production grid code
	account, _, _ := entity.SendCommand(ctx, command)

	log.Printf("current balance: %v", account.GetAccountBalance())

	// send another command to credit the balance
	command = &samplepb.CreditAccount{
		AccountId: entityID,
		Balance:   250,
	}
	account, _, _ = entity.SendCommand(ctx, command)
	log.Printf("current balance: %v", account.GetAccountBalance())

	// capture ctrl+c
	interruptSignal := make(chan os.Signal, 1)
	signal.Notify(interruptSignal, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-interruptSignal

	// stop the actor system
	_ = e.Stop(ctx)
	os.Exit(0)
}

// AccountBehavior implements persistence.Behavior
type AccountBehavior struct {
	id string
}

// make sure that AccountBehavior is a true persistence behavior
var _ ego.EntityBehavior[*samplepb.Account] = &AccountBehavior{}

// NewAccountBehavior creates an instance of AccountBehavior
func NewAccountBehavior(id string) *AccountBehavior {
	return &AccountBehavior{id: id}
}

// ID returns the id
func (a *AccountBehavior) ID() string {
	return a.id
}

// InitialState returns the initial state
func (a *AccountBehavior) InitialState() *samplepb.Account {
	return new(samplepb.Account)
}

// HandleCommand handles every command that is sent to the persistent behavior
func (a *AccountBehavior) HandleCommand(_ context.Context, command ego.Command, _ *samplepb.Account) (event ego.Event, err error) {
	switch cmd := command.(type) {
	case *samplepb.CreateAccount:
		// TODO in production grid app validate the command using the prior state
		return &samplepb.AccountCreated{
			AccountId:      cmd.GetAccountId(),
			AccountBalance: cmd.GetAccountBalance(),
		}, nil

	case *samplepb.CreditAccount:
		// TODO in production grid app validate the command using the prior state
		return &samplepb.AccountCredited{
			AccountId:      cmd.GetAccountId(),
			AccountBalance: cmd.GetBalance(),
		}, nil

	default:
		return nil, errors.New("unhandled command")
	}
}

// HandleEvent handles every event emitted
func (a *AccountBehavior) HandleEvent(_ context.Context, event ego.Event, priorState *samplepb.Account) (state *samplepb.Account, err error) {
	switch evt := event.(type) {
	case *samplepb.AccountCreated:
		return &samplepb.Account{
			AccountId:      evt.GetAccountId(),
			AccountBalance: evt.GetAccountBalance(),
		}, nil

	case *samplepb.AccountCredited:
		bal := priorState.GetAccountBalance() + evt.GetAccountBalance()
		return &samplepb.Account{
			AccountId:      evt.GetAccountId(),
			AccountBalance: bal,
		}, nil

	default:
		return nil, errors.New("unhandled event")
	}
}
```


### Contribution

Contributions are welcome!
The project adheres to [Semantic Versioning](https://semver.org) and [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/).
This repo uses [Earthly](https://earthly.dev/get-earthly).

To contribute please:

- Fork the repository
- Create a feature branch
- Submit a [pull request](https://help.github.com/articles/using-pull-requests)

#### Test & Linter

Prior to submitting a [pull request](https://help.github.com/articles/using-pull-requests), please run:

```bash
earthly +test
```
