/*
 * MIT License
 *
 * Copyright (c) 2022-2025 Arsene Tochemey Gandote
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

package ego

import (
	"context"
	"errors"

	"google.golang.org/protobuf/types/known/emptypb"

	testpb "github.com/tochemey/ego/v3/test/data/pb/v3"
)

// AccountEventSourcedBehavior implements EventSourcedBehavior
type AccountEventSourcedBehavior struct {
	id string
}

// enforces compilation error
var _ EventSourcedBehavior = (*AccountEventSourcedBehavior)(nil)

func NewAccountEventSourcedBehavior(id string) *AccountEventSourcedBehavior {
	return &AccountEventSourcedBehavior{id: id}
}
func (t *AccountEventSourcedBehavior) ID() string {
	return t.id
}

func (t *AccountEventSourcedBehavior) InitialState() State {
	return new(testpb.Account)
}

func (t *AccountEventSourcedBehavior) HandleCommand(_ context.Context, command Command, _ State) (events []Event, err error) {
	switch cmd := command.(type) {
	case *testpb.CreateAccount:
		return []Event{
			&testpb.AccountCreated{
				AccountId:      t.id,
				AccountBalance: cmd.GetAccountBalance(),
			},
		}, nil

	case *testpb.CreditAccount:
		if cmd.GetAccountId() == t.id {
			return []Event{
				&testpb.AccountCredited{
					AccountId:      cmd.GetAccountId(),
					AccountBalance: cmd.GetBalance(),
				},
			}, nil
		}

		return nil, errors.New("command sent to the wrong entity")

	case *testpb.TestNoEvent:
		return nil, nil

	case *emptypb.Empty:
		return []Event{new(emptypb.Empty)}, nil

	default:
		return nil, errors.New("unhandled command")
	}
}

func (t *AccountEventSourcedBehavior) HandleEvent(_ context.Context, event Event, priorState State) (state State, err error) {
	switch evt := event.(type) {
	case *testpb.AccountCreated:
		return &testpb.Account{
			AccountId:      evt.GetAccountId(),
			AccountBalance: evt.GetAccountBalance(),
		}, nil

	case *testpb.AccountCredited:
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

type AccountDurableStateBehavior struct {
	id string
}

// enforces compilation error
var _ DurableStateBehavior = (*AccountDurableStateBehavior)(nil)

func NewAccountDurableStateBehavior(id string) *AccountDurableStateBehavior {
	return &AccountDurableStateBehavior{id: id}
}

func (x *AccountDurableStateBehavior) ID() string {
	return x.id
}

func (x *AccountDurableStateBehavior) InitialState() State {
	return new(testpb.Account)
}

// nolint
func (x *AccountDurableStateBehavior) HandleCommand(ctx context.Context, command Command, priorVersion uint64, priorState State) (newState State, newVersion uint64, err error) {
	switch cmd := command.(type) {
	case *testpb.CreateAccount:
		return &testpb.Account{
			AccountId:      x.id,
			AccountBalance: cmd.GetAccountBalance(),
		}, priorVersion + 1, nil

	case *testpb.CreditAccount:
		if cmd.GetAccountId() == x.id {
			account := priorState.(*testpb.Account)
			bal := account.GetAccountBalance() + cmd.GetBalance()

			return &testpb.Account{
				AccountId:      cmd.GetAccountId(),
				AccountBalance: bal,
			}, priorVersion + 1, nil
		}

		return nil, 0, errors.New("command sent to the wrong entity")

	default:
		return nil, 0, errors.New("unhandled command")
	}
}
