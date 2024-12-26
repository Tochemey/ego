/*
 * MIT License
 *
 * Copyright (c) 2022-2024 Tochemey
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

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
	memory "github.com/tochemey/ego-contrib/durablestore/memory"
	"google.golang.org/protobuf/proto"

	"github.com/tochemey/ego/v3"
	samplepb "github.com/tochemey/ego/v3/example/pbs/sample/pb/v1"
)

func main() {
	// create the go context
	ctx := context.Background()
	// create the event store
	durableStore := memory.NewStateStore()
	// connect the event store
	_ = durableStore.Connect(ctx)
	// create the ego engine
	engine := ego.NewEngine("Sample", nil, ego.WithStateStore(durableStore))
	// start ego engine
	_ = engine.Start(ctx)
	// create a persistence id
	entityID := uuid.NewString()
	// create an entity behavior with a given id
	behavior := NewAccountBehavior(entityID)
	// create an entity
	_ = engine.DurableStateEntity(ctx, behavior)

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
	_ = durableStore.Disconnect(ctx)
	// stop the actor system
	_ = engine.Stop(ctx)
	os.Exit(0)
}

// AccountBehavior implements EventSourcedBehavior
type AccountBehavior struct {
	id string
}

// make sure that AccountBehavior is a true persistence behavior
var _ ego.DurableStateBehavior = &AccountBehavior{}

// NewAccountBehavior creates an instance of AccountBehavior
func NewAccountBehavior(id string) *AccountBehavior {
	return &AccountBehavior{id: id}
}

// ID returns the id
func (a *AccountBehavior) ID() string {
	return a.id
}

// InitialState returns the initial state
func (a *AccountBehavior) InitialState() ego.State {
	return ego.State(new(samplepb.Account))
}

// HandleCommand handles every command that is sent to the persistent behavior
func (a *AccountBehavior) HandleCommand(_ context.Context, command ego.Command, priorVersion uint64, priorState ego.State) (event ego.State, newVersion uint64, err error) {
	switch cmd := command.(type) {
	case *samplepb.CreateAccount:
		// TODO in production grid app validate the command using the prior state
		return &samplepb.Account{
			AccountId:      a.id,
			AccountBalance: cmd.GetAccountBalance(),
		}, priorVersion + 1, nil

	case *samplepb.CreditAccount:
		if cmd.GetAccountId() == a.id {
			// TODO in production grid app validate the command using the prior state
			account := priorState.(*samplepb.Account)
			bal := account.GetAccountBalance() + cmd.GetBalance()

			return &samplepb.Account{
				AccountId:      a.id,
				AccountBalance: bal,
			}, priorVersion + 1, nil
		}
		return nil, 0, errors.New("command sent to the wrong entity")

	default:
		return nil, 0, errors.New("unhandled command")
	}
}
