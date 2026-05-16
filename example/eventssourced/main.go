// MIT License
//
// Copyright (c) 2022-2026 Arsene Tochemey Gandote
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	goakt "github.com/tochemey/goakt/v4/actor"
	"google.golang.org/protobuf/proto"

	"github.com/tochemey/ego/v4"
	samplepb "github.com/tochemey/ego/v4/example/examplepb"
	"github.com/tochemey/ego/v4/testkit"
)

func main() {
	// create the go context
	ctx := context.Background()
	// create the event store
	eventStore := testkit.NewEventsStore()
	// connect the event store
	_ = eventStore.Connect(ctx)
	// build the eGo Config once and reuse it for both the actor system and
	// the engine. cfg.GoaktOptions() returns the goakt.Option list eGo needs
	// (extensions, pubsub, logger adapter, default supervisor); the caller
	// composes anything else (cluster, TLS, custom actors) directly via goakt.
	cfg := ego.NewConfig(eventStore)
	sys, err := goakt.NewActorSystem("Sample", cfg.GoaktOptions()...)
	if err != nil {
		log.Fatalf("failed to build actor system: %v", err)
	}
	if err := sys.Start(ctx); err != nil {
		log.Fatalf("failed to start actor system: %v", err)
	}
	// plug eGo in
	engine, err := ego.NewEngine(sys, cfg)
	if err != nil {
		log.Fatalf("failed to create ego engine: %v", err)
	}
	// start ego engine
	_ = engine.Start(ctx)
	// create a persistence id
	entityID := uuid.NewString()
	// create an entity behavior with a given id
	behavior := NewAccountBehavior(entityID)
	// create an entity
	_ = engine.Entity(ctx, behavior)

	// send some commands to the pid
	var command proto.Message
	// create an account
	command = &samplepb.CreateAccount{
		AccountId:      entityID,
		AccountBalance: 500.00,
	}
	// send the command to the actor. Please don't ignore the error in production grid code
	reply, _, _ := engine.SendCommand(ctx, entityID, command, time.Minute)
	account := reply.(*samplepb.Account)
	log.Printf("current balance on opening: %v", account.GetAccountBalance())

	// send another command to credit the balance
	command = &samplepb.CreditAccount{
		AccountId: entityID,
		Balance:   250,
	}

	reply, _, _ = engine.SendCommand(ctx, entityID, command, time.Minute)
	account = reply.(*samplepb.Account)
	log.Printf("current balance after a credit of 250: %v", account.GetAccountBalance())

	// capture ctrl+c
	interruptSignal := make(chan os.Signal, 1)
	signal.Notify(interruptSignal, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-interruptSignal

	// disconnect the event store
	_ = eventStore.Disconnect(ctx)
	// stop ego first, then the actor system (the caller owns its lifecycle)
	_ = engine.Stop(ctx)
	_ = sys.Stop(ctx)
	os.Exit(0)
}

// AccountBehavior implements EventSourcedBehavior
type AccountBehavior struct {
	id string
}

// make sure that AccountBehavior is a true persistence behavior
var _ ego.EventSourcedBehavior = &AccountBehavior{}

// NewAccountBehavior creates an instance of AccountBehavior
func NewAccountBehavior(id string) *AccountBehavior {
	return &AccountBehavior{id: id}
}

// ID returns the id
func (x *AccountBehavior) ID() string {
	return x.id
}

// InitialState returns the initial state
func (x *AccountBehavior) InitialState() ego.State {
	return ego.State(new(samplepb.Account))
}

// HandleCommand handles every command that is sent to the persistent behavior
func (x *AccountBehavior) HandleCommand(_ context.Context, command ego.Command, _ ego.State) (events []ego.Event, err error) {
	switch cmd := command.(type) {
	case *samplepb.CreateAccount:
		// TODO in production grid app validate the command using the prior state
		return []ego.Event{
			&samplepb.AccountCreated{
				AccountId:      cmd.GetAccountId(),
				AccountBalance: cmd.GetAccountBalance(),
			},
		}, nil

	case *samplepb.CreditAccount:
		// TODO in production grid app validate the command using the prior state
		return []ego.Event{
			&samplepb.AccountCredited{
				AccountId:      cmd.GetAccountId(),
				AccountBalance: cmd.GetBalance(),
			},
		}, nil

	default:
		return nil, errors.New("unhandled command")
	}
}

// HandleEvent handles every event emitted
func (x *AccountBehavior) HandleEvent(_ context.Context, event ego.Event, priorState ego.State) (state ego.State, err error) {
	switch evt := event.(type) {
	case *samplepb.AccountCreated:
		return &samplepb.Account{
			AccountId:      evt.GetAccountId(),
			AccountBalance: evt.GetAccountBalance(),
		}, nil

	case *samplepb.AccountCredited:
		account := priorState.(*samplepb.Account)
		bal := account.GetAccountBalance() + evt.GetAccountBalance()
		return &samplepb.Account{
			AccountId:      evt.GetAccountId(),
			AccountBalance: bal,
		}, nil

	default:
		return nil, errors.New("unhandled event")
	}
}

func (x *AccountBehavior) MarshalBinary() (data []byte, err error) {
	serializable := struct {
		ID string `json:"id"`
	}{
		ID: x.id,
	}
	return json.Marshal(serializable)
}

func (x *AccountBehavior) UnmarshalBinary(data []byte) error {
	serializable := struct {
		ID string `json:"id"`
	}{}

	if err := json.Unmarshal(data, &serializable); err != nil {
		return err
	}

	x.id = serializable.ID
	return nil
}
