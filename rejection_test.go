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

package ego

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tochemey/ego/v4/egopb"
	testpb "github.com/tochemey/ego/v4/test/data/testpb"
	"github.com/tochemey/ego/v4/testkit"
)

// errInvalidAmount is the rejection the rejecting test behaviors return for a
// credit that is not positive.
var errInvalidAmount = NewRejection("invalid_amount", "amount must be positive")

// rejectingEventSourcedBehavior rejects a credit that is not positive and
// handles every other command like AccountEventSourcedBehavior.
type rejectingEventSourcedBehavior struct {
	*AccountEventSourcedBehavior
}

// enforce compilation error
var _ EventSourcedBehavior = (*rejectingEventSourcedBehavior)(nil)

// HandleCommand rejects a credit that is not positive with a wrapped errInvalidAmount.
func (x *rejectingEventSourcedBehavior) HandleCommand(ctx context.Context, command Command, priorState State) ([]Event, error) {
	if credit, ok := command.(*testpb.CreditAccount); ok && credit.GetBalance() <= 0 {
		return nil, fmt.Errorf("account %s: %w", x.ID(), errInvalidAmount)
	}

	return x.AccountEventSourcedBehavior.HandleCommand(ctx, command, priorState)
}

// rejectingDurableStateBehavior rejects a credit that is not positive, answers
// TestNoEvent with a version that skips one, and handles every other command
// like AccountDurableStateBehavior.
type rejectingDurableStateBehavior struct {
	*AccountDurableStateBehavior
}

// enforce compilation error
var _ DurableStateBehavior = (*rejectingDurableStateBehavior)(nil)

// HandleCommand rejects a credit that is not positive with a wrapped errInvalidAmount
// and returns a skipped version for TestNoEvent so the entity refuses the state.
func (x *rejectingDurableStateBehavior) HandleCommand(ctx context.Context, command Command, priorVersion uint64, priorState State) (State, uint64, error) {
	switch credit := command.(type) {
	case *testpb.CreditAccount:
		if credit.GetBalance() <= 0 {
			return nil, 0, fmt.Errorf("account %s: %w", x.ID(), errInvalidAmount)
		}
	case *testpb.TestNoEvent:
		return priorState, priorVersion + 2, nil
	}

	return x.AccountDurableStateBehavior.HandleCommand(ctx, command, priorVersion, priorState)
}

func TestRejection(t *testing.T) {
	t.Run("exposes its code and message", func(t *testing.T) {
		rejection := NewRejection("invalid_amount", "amount must be positive")
		assert.Equal(t, "invalid_amount", rejection.Code())
		assert.EqualError(t, rejection, "amount must be positive")
	})

	t.Run("matches a rejection with the same code", func(t *testing.T) {
		received := NewRejection("invalid_amount", "account 42: amount must be positive")
		assert.ErrorIs(t, received, errInvalidAmount)
		assert.ErrorIs(t, fmt.Errorf("transfer failed: %w", received), errInvalidAmount)
	})

	t.Run("does not match a rejection with another code", func(t *testing.T) {
		received := NewRejection("account_closed", "amount must be positive")
		assert.NotErrorIs(t, received, errInvalidAmount)
	})

	t.Run("an empty code matches nothing", func(t *testing.T) {
		assert.NotErrorIs(t, NewRejection("", "amount must be positive"), NewRejection("", "amount must be positive"))
	})

	t.Run("a nil rejection matches nothing", func(t *testing.T) {
		assert.NotErrorIs(t, (*Rejection)(nil), errInvalidAmount)
		assert.NotErrorIs(t, errInvalidAmount, (*Rejection)(nil))
	})

	t.Run("does not match a plain error with the same text", func(t *testing.T) {
		assert.NotErrorIs(t, errors.New("amount must be positive"), errInvalidAmount)
		assert.NotErrorIs(t, errInvalidAmount, errors.New("amount must be positive"))
	})

	t.Run("code is found in a wrapped chain", func(t *testing.T) {
		assert.Equal(t, "invalid_amount", rejectionCode(fmt.Errorf("account 42: %w", errInvalidAmount)))
		assert.Empty(t, rejectionCode(errors.New("store unavailable")))
	})
}

func TestParseCommandReplyRejection(t *testing.T) {
	t.Run("error reply with a code is a rejection", func(t *testing.T) {
		reply := &egopb.CommandReply{
			Reply: &egopb.CommandReply_ErrorReply{
				ErrorReply: &egopb.ErrorReply{
					Message: "account 42: amount must be positive",
					Code:    "invalid_amount",
				},
			},
		}

		_, _, err := parseCommandReply(reply)
		require.ErrorIs(t, err, errInvalidAmount)
		assert.EqualError(t, err, "account 42: amount must be positive")

		var rejection *Rejection
		require.ErrorAs(t, err, &rejection)
		assert.Equal(t, "invalid_amount", rejection.Code())
	})

	t.Run("error reply without a code is a plain error", func(t *testing.T) {
		reply := &egopb.CommandReply{
			Reply: &egopb.CommandReply_ErrorReply{
				ErrorReply: &egopb.ErrorReply{Message: "store unavailable"},
			},
		}

		_, _, err := parseCommandReply(reply)
		require.EqualError(t, err, "store unavailable")

		var rejection *Rejection
		assert.False(t, errors.As(err, &rejection))
	})
}

// TestSendCommandRejection covers a rejection travelling from a command
// handler to the caller of SendCommand for both kinds of entities.
func TestSendCommandRejection(t *testing.T) {
	ctx := context.Background()

	eventSourcedCases := []struct {
		name string
		opts []SpawnOption
	}{
		{name: "event-sourced entity"},
		{name: "event-sourced entity with batched writes", opts: []SpawnOption{WithBatchThreshold(10)}},
	}

	for _, testCase := range eventSourcedCases {
		t.Run(testCase.name, func(t *testing.T) {
			store := testkit.NewEventsStore()
			require.NoError(t, store.Connect(ctx))
			t.Cleanup(func() { _ = store.Disconnect(ctx) })

			engine := newTestEngine(t, "Sample", store, WithLogger(DiscardLogger))
			require.NoError(t, engine.Start(ctx))

			entityID := uuid.NewString()
			behavior := &rejectingEventSourcedBehavior{AccountEventSourcedBehavior: NewAccountEventSourcedBehavior(entityID)}
			require.NoError(t, engine.Entity(ctx, behavior, testCase.opts...))

			_, revision, err := engine.SendCommand(ctx, entityID, &testpb.CreateAccount{AccountBalance: 500}, time.Minute)
			require.NoError(t, err)
			require.EqualValues(t, 1, revision)

			assertRejectedCredit(ctx, t, engine, entityID)

			// the rejection persisted nothing, so the next credit is revision 2
			_, revision, err = engine.SendCommand(ctx, entityID, &testpb.CreditAccount{AccountId: entityID, Balance: 250}, time.Minute)
			require.NoError(t, err)
			assert.EqualValues(t, 2, revision)
		})
	}

	t.Run("durable-state entity", func(t *testing.T) {
		stateStore := testkit.NewDurableStore()
		require.NoError(t, stateStore.Connect(ctx))
		t.Cleanup(func() { _ = stateStore.Disconnect(ctx) })

		engine := newTestEngine(t, "Sample", nil, WithLogger(DiscardLogger), WithStateStore(stateStore))
		require.NoError(t, engine.Start(ctx))

		entityID := uuid.NewString()
		behavior := &rejectingDurableStateBehavior{AccountDurableStateBehavior: NewAccountDurableStateBehavior(entityID)}
		require.NoError(t, engine.DurableStateEntity(ctx, behavior))

		_, revision, err := engine.SendCommand(ctx, entityID, &testpb.CreateAccount{AccountBalance: 500}, time.Minute)
		require.NoError(t, err)
		require.EqualValues(t, 1, revision)

		assertRejectedCredit(ctx, t, engine, entityID)

		// a version the entity refuses is not a rejection either
		_, _, err = engine.SendCommand(ctx, entityID, new(testpb.TestNoEvent), time.Minute)
		require.ErrorContains(t, err, "received version")

		var rejection *Rejection
		assert.False(t, errors.As(err, &rejection))

		// the rejection persisted nothing, so the next credit is version 2
		_, revision, err = engine.SendCommand(ctx, entityID, &testpb.CreditAccount{AccountId: entityID, Balance: 250}, time.Minute)
		require.NoError(t, err)
		assert.EqualValues(t, 2, revision)
	})
}

// assertRejectedCredit sends a credit the rejecting behaviors refuse and an
// unhandled command, and asserts that only the first comes back as a rejection.
func assertRejectedCredit(ctx context.Context, t *testing.T, engine *Engine, entityID string) {
	t.Helper()

	_, _, err := engine.SendCommand(ctx, entityID, &testpb.CreditAccount{AccountId: entityID, Balance: -10}, time.Minute)
	require.ErrorIs(t, err, errInvalidAmount)
	assert.EqualError(t, err, fmt.Sprintf("account %s: amount must be positive", entityID))

	_, _, err = engine.SendCommand(ctx, entityID, new(testpb.TestSend), time.Minute)
	require.EqualError(t, err, "unhandled command")
	assert.NotErrorIs(t, err, errInvalidAmount)

	var rejection *Rejection
	assert.False(t, errors.As(err, &rejection))
}
