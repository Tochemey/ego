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

package testkit

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"

	testpb "github.com/tochemey/ego/v4/test/data/testpb"
)

// ---------------------------------------------------------------------------
// Test behaviors (defined locally to avoid import cycles with the root package)
// ---------------------------------------------------------------------------

// accountEventSourcedBehavior implements EventSourcedBehavior for testing.
type accountEventSourcedBehavior struct {
	id string
}

var _ EventSourcedBehavior = (*accountEventSourcedBehavior)(nil)

func (b *accountEventSourcedBehavior) InitialState() proto.Message {
	return new(testpb.Account)
}

func (b *accountEventSourcedBehavior) HandleCommand(_ context.Context, command proto.Message, _ proto.Message) ([]proto.Message, error) {
	switch cmd := command.(type) {
	case *testpb.CreateAccount:
		return []proto.Message{
			&testpb.AccountCreated{
				AccountId:      b.id,
				AccountBalance: cmd.GetAccountBalance(),
			},
		}, nil

	case *testpb.CreditAccount:
		if cmd.GetAccountId() == b.id {
			return []proto.Message{
				&testpb.AccountCredited{
					AccountId:      cmd.GetAccountId(),
					AccountBalance: cmd.GetBalance(),
				},
			}, nil
		}
		return nil, errors.New("command sent to the wrong entity")

	case *testpb.TestNoEvent:
		return nil, nil

	default:
		return nil, errors.New("unhandled command")
	}
}

func (b *accountEventSourcedBehavior) HandleEvent(_ context.Context, event proto.Message, priorState proto.Message) (proto.Message, error) {
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

// accountDurableStateBehavior implements DurableStateBehavior for testing.
type accountDurableStateBehavior struct {
	id string
}

var _ DurableStateBehavior = (*accountDurableStateBehavior)(nil)

func (b *accountDurableStateBehavior) InitialState() proto.Message {
	return new(testpb.Account)
}

func (b *accountDurableStateBehavior) HandleCommand(_ context.Context, command proto.Message, priorVersion uint64, priorState proto.Message) (proto.Message, uint64, error) {
	switch cmd := command.(type) {
	case *testpb.CreateAccount:
		return &testpb.Account{
			AccountId:      b.id,
			AccountBalance: cmd.GetAccountBalance(),
		}, priorVersion + 1, nil

	case *testpb.CreditAccount:
		if cmd.GetAccountId() == b.id {
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

// ---------------------------------------------------------------------------
// EventSourcedScenario tests
// ---------------------------------------------------------------------------

func TestEventSourcedScenario_GivenEventsWhenCommandThenEventsAndState(t *testing.T) {
	behavior := &accountEventSourcedBehavior{id: "acc-1"}

	ForEventSourcedBehavior(behavior).
		Given(
			&testpb.AccountCreated{
				AccountId:      "acc-1",
				AccountBalance: 100.00,
			},
		).
		When(&testpb.CreditAccount{
			AccountId: "acc-1",
			Balance:   50.00,
		}).
		ThenEvents(t,
			&testpb.AccountCredited{
				AccountId:      "acc-1",
				AccountBalance: 50.00,
			},
		).
		ThenState(t, &testpb.Account{
			AccountId:      "acc-1",
			AccountBalance: 150.00,
		})
}

func TestEventSourcedScenario_WhenCommandFromInitialState(t *testing.T) {
	behavior := &accountEventSourcedBehavior{id: "acc-2"}

	ForEventSourcedBehavior(behavior).
		When(&testpb.CreateAccount{
			AccountBalance: 200.00,
		}).
		ThenEvents(t,
			&testpb.AccountCreated{
				AccountId:      "acc-2",
				AccountBalance: 200.00,
			},
		).
		ThenState(t, &testpb.Account{
			AccountId:      "acc-2",
			AccountBalance: 200.00,
		})
}

func TestEventSourcedScenario_WhenCommandReturnsError(t *testing.T) {
	behavior := &accountEventSourcedBehavior{id: "acc-3"}

	ForEventSourcedBehavior(behavior).
		When(&testpb.CreditAccount{
			AccountId: "wrong-id",
			Balance:   10.00,
		}).
		ThenError(t, "command sent to the wrong entity")
}

func TestEventSourcedScenario_WhenCommandProducesNoEvents(t *testing.T) {
	behavior := &accountEventSourcedBehavior{id: "acc-4"}

	ForEventSourcedBehavior(behavior).
		When(&testpb.TestNoEvent{}).
		ThenNoEvents(t)
}

func TestEventSourcedScenario_GivenEventsFailHandleEvent(t *testing.T) {
	behavior := &accountEventSourcedBehavior{id: "acc-5"}

	// Supply an event that HandleEvent does not recognize, which causes
	// an error during the Given phase (prior-event replay).
	ForEventSourcedBehavior(behavior).
		Given(
			&testpb.TestNoEvent{}, // not handled by HandleEvent
		).
		When(&testpb.CreateAccount{
			AccountBalance: 10.00,
		}).
		ThenError(t, "unhandled event")
}

func TestEventSourcedScenario_MultipleGivenEvents(t *testing.T) {
	behavior := &accountEventSourcedBehavior{id: "acc-6"}

	ForEventSourcedBehavior(behavior).
		Given(
			&testpb.AccountCreated{
				AccountId:      "acc-6",
				AccountBalance: 100.00,
			},
			&testpb.AccountCredited{
				AccountId:      "acc-6",
				AccountBalance: 25.00,
			},
		).
		When(&testpb.CreditAccount{
			AccountId: "acc-6",
			Balance:   75.00,
		}).
		ThenEvents(t,
			&testpb.AccountCredited{
				AccountId:      "acc-6",
				AccountBalance: 75.00,
			},
		).
		ThenState(t, &testpb.Account{
			AccountId:      "acc-6",
			AccountBalance: 200.00,
		})
}

func TestEventSourcedScenario_UnhandledCommandReturnsError(t *testing.T) {
	behavior := &accountEventSourcedBehavior{id: "acc-7"}

	// TestSend is not handled by our test behavior
	ForEventSourcedBehavior(behavior).
		When(&testpb.TestSend{}).
		ThenError(t, "unhandled command")
}

// ---------------------------------------------------------------------------
// DurableStateScenario tests
// ---------------------------------------------------------------------------

func TestDurableStateScenario_GivenStateWhenCommandThenStateAndVersion(t *testing.T) {
	behavior := &accountDurableStateBehavior{id: "ds-1"}

	ForDurableStateBehavior(behavior).
		Given(
			&testpb.Account{
				AccountId:      "ds-1",
				AccountBalance: 100.00,
			},
			1, // prior version
		).
		When(&testpb.CreditAccount{
			AccountId: "ds-1",
			Balance:   50.00,
		}).
		ThenState(t, &testpb.Account{
			AccountId:      "ds-1",
			AccountBalance: 150.00,
		}).
		ThenVersion(t, 2)
}

func TestDurableStateScenario_WhenCommandFromInitialState(t *testing.T) {
	behavior := &accountDurableStateBehavior{id: "ds-2"}

	ForDurableStateBehavior(behavior).
		When(&testpb.CreateAccount{
			AccountBalance: 300.00,
		}).
		ThenState(t, &testpb.Account{
			AccountId:      "ds-2",
			AccountBalance: 300.00,
		}).
		ThenVersion(t, 1)
}

func TestDurableStateScenario_WhenCommandReturnsError(t *testing.T) {
	behavior := &accountDurableStateBehavior{id: "ds-3"}

	ForDurableStateBehavior(behavior).
		When(&testpb.CreditAccount{
			AccountId: "wrong-id",
			Balance:   10.00,
		}).
		ThenError(t, "command sent to the wrong entity")
}

func TestDurableStateScenario_UnhandledCommandReturnsError(t *testing.T) {
	behavior := &accountDurableStateBehavior{id: "ds-4"}

	// TestSend is not handled by our test behavior
	ForDurableStateBehavior(behavior).
		When(&testpb.TestSend{}).
		ThenError(t, "unhandled command")
}
