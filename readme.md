# Ego [Experimental]

[![build](https://img.shields.io/github/actions/workflow/status/Tochemey/ego/build.yml?branch=main)](https://github.com/Tochemey/ego/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/Tochemey/ego/branch/main/graph/badge.svg?token=Z5b9gM6Mnt)](https://codecov.io/gh/Tochemey/ego)

Ego is a minimal library that help build event-sourcing and CQRS application through a simple interface, and it allows developers to describe their commands, events and states are defined using google protocol buffers.
Under the hood, ego leverages [goakt](https://github.com/Tochemey/goakt) to scale out and guarantee performant, reliable persistence.

## Features

- [x] Write Model:
    - [x] Commands handler: The command handlers define how to handle each incoming command,
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
    - [x] Events handler: The event handlers are used to mutate the state of the Aggregate by applying the events to it.
      Event handlers must be pure functions as they will be used when instantiating the Aggregate and replaying the event store.
    - [x] Extensible events store 
    - [x] Built-in events store
      - [x] Postgres
      - [x] Memory
- [ ] Read Model

## Installation
```bash
go get github.com/tochemey/ego
```

## Example
```go
package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/tochemey/ego/aggregate"
	"github.com/tochemey/ego/egopb"
	samplepb "github.com/tochemey/ego/example/pbs/sample/pb/v1"
	"github.com/tochemey/ego/storage/memory"
	goakt "github.com/tochemey/goakt/actors"
	"google.golang.org/protobuf/proto"
)

func main() {
	// create the go context
	ctx := context.Background()
	// create the actor system
	// create the actor system configuration. kindly in real-life application handle the error
	config, _ := goakt.NewConfig("SampleActorSystem", "127.0.0.1:0",
		goakt.WithPassivationDisabled(),
		goakt.WithActorInitMaxRetries(1))

	// create the actor system. kindly in real-life application handle the error
	actorSystem, _ := goakt.NewActorSystem(config)

	// start the actor system
	_ = actorSystem.Start(ctx)

	// create the event store
	eventStore := memory.NewEventsStore()

	// create a persistence id
	entityID := uuid.NewString()

	// create an aggregate behavior with a given id
	behavior := NewAccountBehavior(entityID)

	// create the given aggregate
	aggregate := aggregate.New[*samplepb.Account](behavior, eventStore)
	// spawn an actor
	pid := actorSystem.StartActor(ctx, behavior.ID(), aggregate)
	// send some commands to the pid
	var command proto.Message
	// create an account
	command = &samplepb.CreateAccount{
		AccountId:      entityID,
		AccountBalance: 500.00,
	}
	// send the command to the actor. Please don't ignore the error in production grid code
	reply, _ := goakt.SendSync(ctx, pid, command, time.Second)
	// cast the reply to a command reply because we know the persistence actor will always send a command reply
	commandReply := reply.(*egopb.CommandReply)
	state := commandReply.GetReply().(*egopb.CommandReply_StateReply)
	log.Printf("resulting sequence number: %d", state.StateReply.GetSequenceNumber())

	account := new(samplepb.Account)
	_ = state.StateReply.GetState().UnmarshalTo(account)

	log.Printf("current balance: %v", account.GetAccountBalance())

	// send another command to credit the balance
	command = &samplepb.CreditAccount{
		AccountId: entityID,
		Balance:   250,
	}
	reply, _ = goakt.SendSync(ctx, pid, command, time.Second)
	commandReply = reply.(*egopb.CommandReply)
	state = commandReply.GetReply().(*egopb.CommandReply_StateReply)
	log.Printf("resulting sequence number: %d", state.StateReply.GetSequenceNumber())

	account = new(samplepb.Account)
	_ = state.StateReply.GetState().UnmarshalTo(account)

	log.Printf("current balance: %v", account.GetAccountBalance())

	// capture ctrl+c
	interruptSignal := make(chan os.Signal, 1)
	signal.Notify(interruptSignal, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-interruptSignal

	// stop the actor system
	_ = actorSystem.Stop(ctx)
	os.Exit(0)

}

// AccountBehavior implements persistence.Behavior
type AccountBehavior struct {
	id string
}

// make sure that AccountBehavior is a true persistence behavior
var _ aggregate.Behavior[*samplepb.Account] = &AccountBehavior{}

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
func (a *AccountBehavior) HandleCommand(ctx context.Context, command aggregate.Command, priorState *samplepb.Account) (event aggregate.Event, err error) {
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
func (a *AccountBehavior) HandleEvent(ctx context.Context, event aggregate.Event, priorState *samplepb.Account) (state *samplepb.Account, err error) {
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
