package ego

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tochemey/ego/aggregate"
	"github.com/tochemey/ego/egopb"
	"github.com/tochemey/ego/eventstore/memory"
	samplepb "github.com/tochemey/ego/example/pbs/sample/pb/v1"
	"google.golang.org/protobuf/proto"
)

func TestEgo(t *testing.T) {
	ctx := context.TODO()
	// create the event store
	eventStore := memory.NewEventsStore()
	// create the ego engine
	e := New("Sample", eventStore)
	// start ego engine
	err := e.Start(ctx)
	require.NoError(t, err)
	// create a persistence id
	entityID := uuid.NewString()
	// create an aggregate behavior with a given id
	behavior := NewAccountBehavior(entityID)
	// create an aggregate
	CreateAggregate[*samplepb.Account](ctx, e, behavior)
	// send some commands to the pid
	var command proto.Message
	// create an account
	command = &samplepb.CreateAccount{
		AccountId:      entityID,
		AccountBalance: 500.00,
	}
	// send the command to the actor. Please don't ignore the error in production grid code
	reply, err := e.SendCommand(ctx, command, entityID)
	require.NoError(t, err)
	require.IsType(t, new(egopb.CommandReply_StateReply), reply.GetReply())
	assert.EqualValues(t, 1, reply.GetStateReply().GetSequenceNumber())

	resultingState := new(samplepb.Account)
	assert.NoError(t, reply.GetStateReply().GetState().UnmarshalTo(resultingState))
	assert.EqualValues(t, 500.00, resultingState.GetAccountBalance())
	assert.Equal(t, entityID, resultingState.GetAccountId())

	// send another command to credit the balance
	command = &samplepb.CreditAccount{
		AccountId: entityID,
		Balance:   250,
	}
	reply, err = e.SendCommand(ctx, command, entityID)
	require.NoError(t, err)
	require.IsType(t, new(egopb.CommandReply_StateReply), reply.GetReply())
	assert.EqualValues(t, 2, reply.GetStateReply().GetSequenceNumber())

	newState := new(samplepb.Account)
	assert.NoError(t, reply.GetStateReply().GetState().UnmarshalTo(newState))
	assert.EqualValues(t, 750.00, newState.GetAccountBalance())
	assert.Equal(t, entityID, newState.GetAccountId())

	// free resources
	assert.NoError(t, e.Stop(ctx))
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
