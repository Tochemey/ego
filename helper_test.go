package ego

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/pkg/errors"
	testpb "github.com/tochemey/ego/test/data/pb/v1"
)

// AccountActor implement Behavior
type AccountActor struct {
	id string
}

var _ Behavior = &AccountActor{}

// NewAccountActor creates an instance of AccountActor
func NewAccountActor(id string) *AccountActor {
	return &AccountActor{id: id}
}
func (t *AccountActor) ID() string {
	return t.id
}

func (t *AccountActor) InitialState() proto.Message {
	return new(testpb.Account)
}

func (t *AccountActor) HandleCommand(_ context.Context, command proto.Message, _ proto.Message) (event proto.Message, err error) {
	switch cmd := command.(type) {
	case *testpb.CreateAccount:
		// TODO in production grid app validate the command using the prior state
		return &testpb.AccountCreated{
			AccountId:      t.id,
			AccountBalance: cmd.GetAccountBalance(),
		}, nil

	case *testpb.CreditAccount:
		if cmd.GetAccountId() == t.id {
			return &testpb.AccountCredited{
				AccountId:      cmd.GetAccountId(),
				AccountBalance: cmd.GetBalance(),
			}, nil
		}

		return nil, errors.New("command sent to the wrong entity")

	default:
		return nil, errors.New("unhandled command")
	}
}

func (t *AccountActor) HandleEvent(_ context.Context, event proto.Message, priorState proto.Message) (state proto.Message, err error) {
	switch evt := event.(type) {
	case *testpb.AccountCreated:
		return &testpb.Account{
			AccountId:      evt.GetAccountId(),
			AccountBalance: evt.GetAccountBalance(),
		}, nil

	case *testpb.AccountCredited:
		// cast the prior state to account
		account := priorState.(*testpb.Account)
		bal := account.GetAccountBalance() + evt.GetAccountBalance()
		return &testpb.Account{
			AccountId:      evt.GetAccountId(),
			AccountBalance: bal,
		}, nil

	default:
		return nil, errors.New("unhandled event")
	}
}
