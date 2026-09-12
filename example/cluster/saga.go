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
	"log/slog"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/tochemey/ego/v4"
	samplepb "github.com/tochemey/ego/v4/example/examplepb"
)

// participantTimeout bounds each command the saga sends to an account.
const participantTimeout = 5 * time.Second

// FundTransferSaga implements ego.SagaBehavior for one fund transfer.
//
// The saga waits for the TransferStarted event journaled by the transfer
// entity, debits the source account, credits the destination account, and
// completes. A rejected step triggers compensation: the source is refunded
// when it had been debited, and the reason is kept in the saga state.
//
// The saga reads the journal of every cluster node, so the transfer entity
// and the accounts can live on any pod. Delivery is at-least-once, so every
// handler checks the saga state before acting.
type FundTransferSaga struct {
	transferID    string
	sourceID      string
	destinationID string
	amount        float64
}

var _ ego.SagaBehavior = (*FundTransferSaga)(nil)

// NewFundTransferSaga returns the saga coordinating the given transfer.
func NewFundTransferSaga(transferID, sourceID, destinationID string, amount float64) *FundTransferSaga {
	return &FundTransferSaga{
		transferID:    transferID,
		sourceID:      sourceID,
		destinationID: destinationID,
		amount:        amount,
	}
}

// ID returns the transfer ID, which is also the saga's persistence ID.
func (s *FundTransferSaga) ID() string {
	return s.transferID
}

// InitialState returns the transfer with no step completed yet.
func (s *FundTransferSaga) InitialState() ego.State {
	return &samplepb.TransferState{
		TransferId:           s.transferID,
		SourceAccountId:      s.sourceID,
		DestinationAccountId: s.destinationID,
		Amount:               s.amount,
	}
}

// HandleEvent starts the transfer when the transfer entity journals
// TransferStarted for this saga's transfer ID. Every other event, including
// the account events the saga's own commands produce, is ignored.
func (s *FundTransferSaga) HandleEvent(_ context.Context, event ego.Event, state ego.State) (*ego.SagaAction, error) {
	started, ok := event.(*samplepb.TransferStarted)
	if !ok || started.GetTransferId() != s.transferID {
		return nil, nil
	}

	// The event is handed over again when the saga restarts before its
	// progress was recorded; a transfer past the debit step is not debited twice.
	if state.(*samplepb.TransferState).GetSourceDebited() {
		return nil, nil
	}

	return &ego.SagaAction{
		Commands: []ego.SagaCommand{s.debitSource()},
	}, nil
}

// HandleResult advances the transfer after an account accepted a command:
// the source debit is recorded and the destination credited, then the
// destination credit is recorded and the saga completes.
func (s *FundTransferSaga) HandleResult(_ context.Context, entityID string, _ ego.State, sagaState ego.State) (*ego.SagaAction, error) {
	transfer := sagaState.(*samplepb.TransferState)

	switch entityID {
	case s.sourceID:
		if transfer.GetSourceDebited() {
			return nil, nil
		}

		return &ego.SagaAction{
			Events: []ego.Event{
				&samplepb.SourceDebited{SourceAccountId: s.sourceID, Amount: s.amount},
			},
			Commands: []ego.SagaCommand{s.creditDestination()},
		}, nil

	case s.destinationID:
		if transfer.GetDestinationCredited() {
			return nil, nil
		}

		return &ego.SagaAction{
			Events: []ego.Event{
				&samplepb.DestinationCredited{DestinationAccountId: s.destinationID, Amount: s.amount},
			},
			Complete: true,
		}, nil

	default:
		return nil, nil
	}
}

// HandleError records why a step was rejected and starts compensation.
// The reason is journaled before any refund goes out, so it survives a
// restart and is reported by the transfer API.
func (s *FundTransferSaga) HandleError(_ context.Context, entityID string, err error, _ ego.State) (*ego.SagaAction, error) {
	slog.Warn("transfer step rejected, compensating", "transferID", s.transferID, "entityID", entityID, "err", err)

	return &ego.SagaAction{
		Events: []ego.Event{
			&samplepb.TransferFailed{TransferId: s.transferID, Reason: err.Error()},
		},
		Compensate: true,
	}, nil
}

// ApplyEvent folds a saga event into the transfer state. It is pure, so
// replaying the saga's journal on recovery rebuilds the same state.
func (s *FundTransferSaga) ApplyEvent(_ context.Context, event ego.Event, state ego.State) (ego.State, error) {
	transfer := proto.Clone(state.(*samplepb.TransferState)).(*samplepb.TransferState)

	switch evt := event.(type) {
	case *samplepb.SourceDebited:
		transfer.SourceDebited = true
	case *samplepb.DestinationCredited:
		transfer.DestinationCredited = true
	case *samplepb.TransferFailed:
		transfer.FailureReason = evt.GetReason()
	}

	return transfer, nil
}

// Compensate refunds the source account when it was debited. A transfer whose
// debit was rejected has nothing to undo, and the saga then completes at once.
func (s *FundTransferSaga) Compensate(_ context.Context, state ego.State) ([]ego.SagaCommand, error) {
	transfer := state.(*samplepb.TransferState)
	if !transfer.GetSourceDebited() {
		return nil, nil
	}

	slog.Info("refunding source account", "transferID", s.transferID, "amount", s.amount)
	return []ego.SagaCommand{s.refundSource()}, nil
}

// MarshalBinary serializes the saga so it can be relocated to a peer.
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

// UnmarshalBinary restores the saga serialized by MarshalBinary.
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

// debitSource is the first step: take the amount from the source account.
func (s *FundTransferSaga) debitSource() ego.SagaCommand {
	return ego.SagaCommand{
		EntityID: s.sourceID,
		Command:  &samplepb.DebitAccount{AccountId: s.sourceID, Balance: s.amount},
		Timeout:  participantTimeout,
	}
}

// creditDestination is the second step: hand the amount to the destination account.
func (s *FundTransferSaga) creditDestination() ego.SagaCommand {
	return ego.SagaCommand{
		EntityID: s.destinationID,
		Command:  &samplepb.CreditAccount{AccountId: s.destinationID, Balance: s.amount},
		Timeout:  participantTimeout,
	}
}

// refundSource undoes the debit when a later step was rejected.
func (s *FundTransferSaga) refundSource() ego.SagaCommand {
	return ego.SagaCommand{
		EntityID: s.sourceID,
		Command:  &samplepb.CreditAccount{AccountId: s.sourceID, Balance: s.amount},
		Timeout:  participantTimeout,
	}
}
