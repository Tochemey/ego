package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/tochemey/ego/entity"
	"github.com/tochemey/ego/eventstore/memory"

	"github.com/google/uuid"
	"github.com/tochemey/ego"
	"github.com/tochemey/ego/egopb"
	samplepb "github.com/tochemey/ego/example/pbs/sample/pb/v1"
	"google.golang.org/protobuf/proto"
)

func main() {
	// create the go context
	ctx := context.Background()
	// create the event store
	eventStore := memory.NewEventsStore()
	// create the ego engine
	e := ego.New("Sample", eventStore)
	// start ego engine
	_ = e.Start(ctx)
	// create a persistence id
	entityID := uuid.NewString()
	// create an entity behavior with a given id
	behavior := NewAccountBehavior(entityID)
	// create an entity
	ego.NewEntity[*samplepb.Account](ctx, e, behavior)

	// send some commands to the pid
	var command proto.Message
	// create an account
	command = &samplepb.CreateAccount{
		AccountId:      entityID,
		AccountBalance: 500.00,
	}
	// send the command to the actor. Please don't ignore the error in production grid code
	reply, _ := e.SendCommand(ctx, command, entityID)
	state := reply.GetReply().(*egopb.CommandReply_StateReply)
	log.Printf("resulting sequence number: %d", state.StateReply.GetSequenceNumber())

	account := new(samplepb.Account)
	_ = state.StateReply.GetState().UnmarshalTo(account)

	log.Printf("current balance: %v", account.GetAccountBalance())

	// send another command to credit the balance
	command = &samplepb.CreditAccount{
		AccountId: entityID,
		Balance:   250,
	}
	reply, _ = e.SendCommand(ctx, command, entityID)
	state = reply.GetReply().(*egopb.CommandReply_StateReply)
	log.Printf("resulting sequence number: %d", state.StateReply.GetSequenceNumber())

	account = new(samplepb.Account)
	_ = state.StateReply.GetState().UnmarshalTo(account)

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
var _ entity.Behavior[*samplepb.Account] = &AccountBehavior{}

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
func (a *AccountBehavior) HandleCommand(_ context.Context, command entity.Command, _ *samplepb.Account) (event entity.Event, err error) {
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
func (a *AccountBehavior) HandleEvent(_ context.Context, event entity.Event, priorState *samplepb.Account) (state *samplepb.Account, err error) {
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
