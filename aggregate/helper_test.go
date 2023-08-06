package aggregate

import (
	"context"

	"github.com/pkg/errors"
	testpb "github.com/tochemey/ego/test/data/pb/v1"
)

// AccountEntityBehavior implement Behavior
type AccountEntityBehavior struct {
	id string
}

// make sure that testAccountBehavior is a true persistence behavior
var _ Behavior[*testpb.Account] = &AccountEntityBehavior{}

// NewAccountEntityBehavior creates an instance of AccountEntityBehavior
func NewAccountEntityBehavior(id string) *AccountEntityBehavior {
	return &AccountEntityBehavior{id: id}
}
func (t *AccountEntityBehavior) ID() string {
	return t.id
}

func (t *AccountEntityBehavior) InitialState() *testpb.Account {
	return new(testpb.Account)
}

// nolint
func (t *AccountEntityBehavior) HandleCommand(ctx context.Context, command Command, priorState *testpb.Account) (event Event, err error) {
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

// nolint
func (t *AccountEntityBehavior) HandleEvent(ctx context.Context, event Event, priorState *testpb.Account) (state *testpb.Account, err error) {
	switch evt := event.(type) {
	case *testpb.AccountCreated:
		return &testpb.Account{
			AccountId:      evt.GetAccountId(),
			AccountBalance: evt.GetAccountBalance(),
		}, nil

	case *testpb.AccountCredited:
		bal := priorState.GetAccountBalance() + evt.GetAccountBalance()
		return &testpb.Account{
			AccountId:      evt.GetAccountId(),
			AccountBalance: bal,
		}, nil

	default:
		return nil, errors.New("unhandled event")
	}
}
