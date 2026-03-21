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

// Package main demonstrates a fund transfer saga using eGo's saga process manager.
//
// This example implements a classic distributed transaction pattern: transferring
// funds between two bank accounts. The saga coordinates the following steps:
//
//  1. Debit the source account
//  2. Credit the destination account
//
// If the credit step fails (e.g., destination account rejects the transfer),
// the saga automatically compensates by refunding (crediting) the source account.
//
// Architecture:
//
//	                      +-----------------+
//	                      |  Fund Transfer  |
//	                      |      Saga       |
//	                      +--------+--------+
//	                               |
//	           +-------------------+-------------------+
//	           |                                       |
//	Step 1: DebitAccount                     Step 2: CreditAccount
//	           |                                       |
//	+----------v----------+             +--------------v-----------+
//	|   Source Account    |             |   Destination Account    |
//	|   (EventSourced)    |             |   (EventSourced)         |
//	+---------------------+             +--------------------------+
//
// Compensation (on failure):
//
//	Saga sends CreditAccount to Source Account to refund the debited amount.
//
// How to run:
//
//	go run ./example/saga
//
// Or using the Makefile:
//
//	make run-saga
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	"github.com/tochemey/ego/v4"
	samplepb "github.com/tochemey/ego/v4/example/examplepb"
	"github.com/tochemey/ego/v4/testkit"
)

func main() {
	ctx := context.Background()

	// Create the event store (in-memory for this example).
	eventStore := testkit.NewEventsStore()
	if err := eventStore.Connect(ctx); err != nil {
		slog.Error("failed to connect event store", "err", err)
		os.Exit(1)
	}

	// Create and start the eGo engine.
	engine := ego.NewEngine("FundTransferExample", eventStore)
	if err := engine.Start(ctx); err != nil {
		slog.Error("failed to start engine", "err", err)
		os.Exit(1)
	}

	// Create two bank accounts as event-sourced entities.
	sourceAccountID := uuid.NewString()
	destAccountID := uuid.NewString()

	sourceBehavior := NewAccountBehavior(sourceAccountID)
	destBehavior := NewAccountBehavior(destAccountID)

	if err := engine.Entity(ctx, sourceBehavior); err != nil {
		slog.Error("failed to create source account entity", "err", err)
		os.Exit(1)
	}
	if err := engine.Entity(ctx, destBehavior); err != nil {
		slog.Error("failed to create destination account entity", "err", err)
		os.Exit(1)
	}

	// Fund both accounts with initial balances.
	slog.Info("--- Setting up accounts ---")

	reply, _, err := engine.SendCommand(ctx, sourceAccountID, &samplepb.CreateAccount{
		AccountId:      sourceAccountID,
		AccountBalance: 1000.00,
	}, time.Minute)
	if err != nil {
		slog.Error("failed to create source account", "err", err)
		os.Exit(1)
	}
	slog.Info("Source account created", "balance", reply.(*samplepb.Account).GetAccountBalance())

	reply, _, err = engine.SendCommand(ctx, destAccountID, &samplepb.CreateAccount{
		AccountId:      destAccountID,
		AccountBalance: 500.00,
	}, time.Minute)
	if err != nil {
		slog.Error("failed to create destination account", "err", err)
		os.Exit(1)
	}
	slog.Info("Destination account created", "balance", reply.(*samplepb.Account).GetAccountBalance())

	// Start the fund transfer saga.
	// The saga will:
	//   1. Listen for a TransferStarted event (which it persists for itself)
	//   2. Send a DebitAccount command to the source account
	//   3. On success, send a CreditAccount command to the destination account
	//   4. On success, mark the saga as complete
	//   5. On failure at any step, trigger compensation (refund the source)
	slog.Info("--- Starting fund transfer saga ---")

	transferID := uuid.NewString()
	sagaBehavior := NewFundTransferSaga(transferID, sourceAccountID, destAccountID, 250.00)

	// Start the saga with a 30-second timeout.
	// If the saga does not complete within this duration, compensation is triggered automatically.
	if err := engine.Saga(ctx, sagaBehavior, 30*time.Second); err != nil {
		slog.Error("failed to start saga", "err", err)
		os.Exit(1)
	}
	slog.Info("Fund transfer saga started", "amount", 250.00)

	// The saga reacts to events on the event stream. To kick it off, we send
	// a command to the source account that the saga will pick up and orchestrate.
	// In this example, the saga handles the initial DebitAccount command itself
	// once it sees the TransferStarted event it persisted during HandleEvent.
	//
	// We trigger the saga by sending a DebitAccount command to the source account directly.
	// The AccountDebited event published to the event stream will be picked up by the saga.
	reply, _, err = engine.SendCommand(ctx, sourceAccountID, &samplepb.DebitAccount{
		AccountId: sourceAccountID,
		Balance:   250.00,
	}, time.Minute)
	if err != nil {
		slog.Error("failed to debit source account", "err", err)
		os.Exit(1)
	}
	sourceAccount := reply.(*samplepb.Account)
	slog.Info("Source account debited", "balance", sourceAccount.GetAccountBalance())

	// Give the saga time to process the event and send the credit command.
	time.Sleep(2 * time.Second)

	// Check the final balances.
	slog.Info("--- Final account balances ---")

	reply, _, err = engine.SendCommand(ctx, sourceAccountID, &samplepb.CreditAccount{
		AccountId: sourceAccountID,
		Balance:   0, // no-op credit just to read state
	}, time.Minute)
	if err != nil {
		slog.Error("failed to query source account", "err", err)
		os.Exit(1)
	}
	slog.Info("Source account balance", "balance", reply.(*samplepb.Account).GetAccountBalance())

	reply, _, err = engine.SendCommand(ctx, destAccountID, &samplepb.CreditAccount{
		AccountId: destAccountID,
		Balance:   0, // no-op credit just to read state
	}, time.Minute)
	if err != nil {
		slog.Error("failed to query destination account", "err", err)
		os.Exit(1)
	}
	slog.Info("Destination account balance", "balance", reply.(*samplepb.Account).GetAccountBalance())

	// Wait for interrupt signal to gracefully shut down.
	interruptSignal := make(chan os.Signal, 1)
	signal.Notify(interruptSignal, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-interruptSignal

	_ = eventStore.Disconnect(ctx)
	_ = engine.Stop(ctx)
	os.Exit(0)
}

// ---------------------------------------------------------------------------
// AccountBehavior — Event-sourced entity representing a bank account.
// Handles CreateAccount, CreditAccount, and DebitAccount commands.
// ---------------------------------------------------------------------------

// AccountBehavior implements ego.EventSourcedBehavior for a bank account.
type AccountBehavior struct {
	id string
}

var _ ego.EventSourcedBehavior = (*AccountBehavior)(nil)

// NewAccountBehavior creates a new AccountBehavior with the given persistence ID.
func NewAccountBehavior(id string) *AccountBehavior {
	return &AccountBehavior{id: id}
}

func (a *AccountBehavior) ID() string {
	return a.id
}

func (a *AccountBehavior) InitialState() ego.State {
	return new(samplepb.Account)
}

func (a *AccountBehavior) HandleCommand(_ context.Context, command ego.Command, priorState ego.State) ([]ego.Event, error) {
	switch cmd := command.(type) {
	case *samplepb.CreateAccount:
		return []ego.Event{
			&samplepb.AccountCreated{
				AccountId:      cmd.GetAccountId(),
				AccountBalance: cmd.GetAccountBalance(),
			},
		}, nil

	case *samplepb.CreditAccount:
		return []ego.Event{
			&samplepb.AccountCredited{
				AccountId:      cmd.GetAccountId(),
				AccountBalance: cmd.GetBalance(),
			},
		}, nil

	case *samplepb.DebitAccount:
		account := priorState.(*samplepb.Account)
		if account.GetAccountBalance() < cmd.GetBalance() {
			return nil, fmt.Errorf("insufficient funds: have %.2f, need %.2f",
				account.GetAccountBalance(), cmd.GetBalance())
		}
		return []ego.Event{
			&samplepb.AccountDebited{
				AccountId:      cmd.GetAccountId(),
				AccountBalance: cmd.GetBalance(),
			},
		}, nil

	default:
		return nil, errors.New("unhandled command")
	}
}

func (a *AccountBehavior) HandleEvent(_ context.Context, event ego.Event, priorState ego.State) (ego.State, error) {
	switch evt := event.(type) {
	case *samplepb.AccountCreated:
		return &samplepb.Account{
			AccountId:      evt.GetAccountId(),
			AccountBalance: evt.GetAccountBalance(),
		}, nil

	case *samplepb.AccountCredited:
		account := priorState.(*samplepb.Account)
		return &samplepb.Account{
			AccountId:      evt.GetAccountId(),
			AccountBalance: account.GetAccountBalance() + evt.GetAccountBalance(),
		}, nil

	case *samplepb.AccountDebited:
		account := priorState.(*samplepb.Account)
		return &samplepb.Account{
			AccountId:      evt.GetAccountId(),
			AccountBalance: account.GetAccountBalance() - evt.GetAccountBalance(),
		}, nil

	default:
		return nil, errors.New("unhandled event")
	}
}

func (a *AccountBehavior) MarshalBinary() ([]byte, error) {
	data := struct {
		ID string `json:"id"`
	}{ID: a.id}
	return json.Marshal(data)
}

func (a *AccountBehavior) UnmarshalBinary(data []byte) error {
	aux := struct {
		ID string `json:"id"`
	}{}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	a.id = aux.ID
	return nil
}

// ---------------------------------------------------------------------------
// FundTransferSaga — Saga that orchestrates a fund transfer between accounts.
//
// Flow:
//  1. HandleEvent picks up an AccountDebited event from the source account.
//  2. It persists a TransferStarted saga event and sends a CreditAccount
//     command to the destination account.
//  3. HandleResult receives the credit confirmation, persists a
//     DestinationCredited saga event, and marks the saga as complete.
//  4. HandleError triggers compensation if the credit fails.
//  5. Compensate sends a CreditAccount command to the source account
//     to refund the debited amount.
// ---------------------------------------------------------------------------

// FundTransferSaga implements ego.SagaBehavior for a fund transfer.
type FundTransferSaga struct {
	transferID    string
	sourceID      string
	destinationID string
	amount        float64
}

var _ ego.SagaBehavior = (*FundTransferSaga)(nil)

// NewFundTransferSaga creates a new saga instance.
func NewFundTransferSaga(transferID, sourceID, destinationID string, amount float64) *FundTransferSaga {
	return &FundTransferSaga{
		transferID:    transferID,
		sourceID:      sourceID,
		destinationID: destinationID,
		amount:        amount,
	}
}

func (s *FundTransferSaga) ID() string {
	return s.transferID
}

// InitialState returns the saga's initial state — an empty TransferState.
func (s *FundTransferSaga) InitialState() ego.State {
	return &samplepb.TransferState{
		TransferId:           s.transferID,
		SourceAccountId:      s.sourceID,
		DestinationAccountId: s.destinationID,
		Amount:               s.amount,
	}
}

// HandleEvent is called when an event from the event stream is received.
// The saga listens for AccountDebited events from the source account to
// kick off the credit step.
func (s *FundTransferSaga) HandleEvent(_ context.Context, event ego.Event, state ego.State) (*ego.SagaAction, error) {
	transfer := state.(*samplepb.TransferState)

	switch evt := event.(type) {
	case *samplepb.AccountDebited:
		// Only react to debits from our source account for the expected amount.
		if evt.GetAccountId() != s.sourceID || evt.GetAccountBalance() != s.amount {
			return nil, nil
		}

		// Already processed the debit step — skip.
		if transfer.GetSourceDebited() {
			return nil, nil
		}

		// Record that the source was debited and send a credit to the destination.
		return &ego.SagaAction{
			Events: []ego.Event{
				&samplepb.SourceDebited{
					SourceAccountId: s.sourceID,
					Amount:          s.amount,
				},
			},
			Commands: []ego.SagaCommand{
				{
					EntityID: s.destinationID,
					Command: &samplepb.CreditAccount{
						AccountId: s.destinationID,
						Balance:   s.amount,
					},
					Timeout: 5 * time.Second,
				},
			},
		}, nil

	default:
		// Ignore events we don't care about.
		return nil, nil
	}
}

// HandleResult is called when a command sent to an entity returns successfully.
func (s *FundTransferSaga) HandleResult(_ context.Context, entityID string, _ ego.State, _ ego.State) (*ego.SagaAction, error) {
	if entityID == s.destinationID {
		// Destination was credited — record and complete the saga.
		slog.Info("Saga: destination account credited successfully, completing transfer")
		return &ego.SagaAction{
			Events: []ego.Event{
				&samplepb.DestinationCredited{
					DestinationAccountId: s.destinationID,
					Amount:               s.amount,
				},
			},
			Complete: true,
		}, nil
	}
	return nil, nil
}

// HandleError is called when a command sent to an entity fails.
// The saga triggers compensation to undo completed steps.
func (s *FundTransferSaga) HandleError(_ context.Context, entityID string, err error, _ ego.State) (*ego.SagaAction, error) {
	slog.Error("Saga: command failed, triggering compensation", "entityID", entityID, "err", err)
	return &ego.SagaAction{
		Compensate: true,
	}, nil
}

// ApplyEvent applies a saga event to update the saga's internal state.
// This is a pure function used during event persistence and recovery.
func (s *FundTransferSaga) ApplyEvent(_ context.Context, event ego.Event, state ego.State) (ego.State, error) {
	transfer := proto.Clone(state.(*samplepb.TransferState)).(*samplepb.TransferState)

	switch event.(type) {
	case *samplepb.SourceDebited:
		transfer.SourceDebited = true
	case *samplepb.DestinationCredited:
		transfer.DestinationCredited = true
	}

	return transfer, nil
}

// Compensate returns the commands needed to undo completed steps.
// In this case, if the source was debited, we refund by crediting it back.
func (s *FundTransferSaga) Compensate(_ context.Context, state ego.State) ([]ego.SagaCommand, error) {
	transfer := state.(*samplepb.TransferState)

	var commands []ego.SagaCommand

	// If the source was debited, refund it.
	if transfer.GetSourceDebited() {
		slog.Info("Saga: compensating, refunding source account", "amount", s.amount)
		commands = append(commands, ego.SagaCommand{
			EntityID: s.sourceID,
			Command: &samplepb.CreditAccount{
				AccountId: s.sourceID,
				Balance:   s.amount,
			},
			Timeout: 5 * time.Second,
		})
	}

	return commands, nil
}

func (s *FundTransferSaga) MarshalBinary() ([]byte, error) {
	data := struct {
		TransferID    string  `json:"transfer_id"`
		SourceID      string  `json:"source_id"`
		DestinationID string  `json:"destination_id"`
		Amount        float64 `json:"amount"`
	}{
		TransferID:    s.transferID,
		SourceID:      s.sourceID,
		DestinationID: s.destinationID,
		Amount:        s.amount,
	}
	return json.Marshal(data)
}

func (s *FundTransferSaga) UnmarshalBinary(data []byte) error {
	aux := struct {
		TransferID    string  `json:"transfer_id"`
		SourceID      string  `json:"source_id"`
		DestinationID string  `json:"destination_id"`
		Amount        float64 `json:"amount"`
	}{}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	s.transferID = aux.TransferID
	s.sourceID = aux.SourceID
	s.destinationID = aux.DestinationID
	s.amount = aux.Amount
	return nil
}
