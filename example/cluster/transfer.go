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
	"errors"
	"fmt"

	"github.com/tochemey/ego/v4"
	samplepb "github.com/tochemey/ego/v4/example/examplepb"
)

// transferEntityPrefix keeps the transfer entity apart from the saga of the
// same transfer: both journal under their ID and both are actors, so the two
// cannot share one.
const transferEntityPrefix = "transfer-"

// TransferBehavior implements ego.EventSourcedBehavior for a fund transfer
// request. Its only command is StartTransfer, and the TransferStarted event
// it journals is what the fund transfer saga reacts to.
type TransferBehavior struct {
	id string
}

var _ ego.EventSourcedBehavior = (*TransferBehavior)(nil)

// NewTransferBehavior returns the transfer entity identified by the transfer ID.
func NewTransferBehavior(id string) *TransferBehavior {
	return &TransferBehavior{id: id}
}

// ID returns the entity's persistence ID: the transfer ID under the
// transfer entity prefix.
func (t *TransferBehavior) ID() string {
	return transferEntityPrefix + t.id
}

// InitialState returns an empty transfer: nothing has been requested yet.
func (t *TransferBehavior) InitialState() ego.State {
	return new(samplepb.TransferState)
}

// HandleCommand validates a StartTransfer request and journals TransferStarted.
// A transfer that has already started rejects a second request, so a retried
// HTTP call never starts the same transfer twice.
func (t *TransferBehavior) HandleCommand(_ context.Context, command ego.Command, priorState ego.State) ([]ego.Event, error) {
	switch cmd := command.(type) {
	case *samplepb.StartTransfer:
		transfer := priorState.(*samplepb.TransferState)
		if transfer.GetTransferId() != "" {
			return nil, errors.New("transfer already started")
		}

		if err := validateTransfer(cmd); err != nil {
			return nil, err
		}

		return []ego.Event{
			&samplepb.TransferStarted{
				TransferId:           cmd.GetTransferId(),
				SourceAccountId:      cmd.GetSourceAccountId(),
				DestinationAccountId: cmd.GetDestinationAccountId(),
				Amount:               cmd.GetAmount(),
			},
		}, nil

	default:
		return nil, errors.New("unhandled command")
	}
}

// HandleEvent records the transfer details carried by TransferStarted.
func (t *TransferBehavior) HandleEvent(_ context.Context, event ego.Event, _ ego.State) (ego.State, error) {
	switch evt := event.(type) {
	case *samplepb.TransferStarted:
		return &samplepb.TransferState{
			TransferId:           evt.GetTransferId(),
			SourceAccountId:      evt.GetSourceAccountId(),
			DestinationAccountId: evt.GetDestinationAccountId(),
			Amount:               evt.GetAmount(),
		}, nil

	default:
		return nil, errors.New("unhandled event")
	}
}

// MarshalBinary serializes the behavior so its spawn can be placed on a peer.
func (t *TransferBehavior) MarshalBinary() ([]byte, error) {
	data := struct {
		ID string `json:"id"`
	}{ID: t.id}
	return json.Marshal(data)
}

// UnmarshalBinary restores the behavior serialized by MarshalBinary.
func (t *TransferBehavior) UnmarshalBinary(data []byte) error {
	aux := struct {
		ID string `json:"id"`
	}{}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	t.id = aux.ID
	return nil
}

// validateTransfer rejects a transfer request that could never be carried out.
func validateTransfer(cmd *samplepb.StartTransfer) error {
	switch {
	case cmd.GetSourceAccountId() == "" || cmd.GetDestinationAccountId() == "":
		return errors.New("source and destination accounts are required")
	case cmd.GetSourceAccountId() == cmd.GetDestinationAccountId():
		return errors.New("source and destination accounts must differ")
	case cmd.GetAmount() <= 0:
		return fmt.Errorf("amount must be positive, got %.2f", cmd.GetAmount())
	}

	return nil
}
