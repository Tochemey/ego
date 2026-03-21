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

package projection

import (
	"context"

	"google.golang.org/protobuf/types/known/anypb"
)

// DeadLetterHandler receives events that a projection handler failed
// to process after exhausting its recovery policy. Implementations
// can log, persist to a dead-letter table, publish to a queue, etc.
type DeadLetterHandler interface {
	// Handle is called with the event that could not be processed,
	// the cause error, and context about the projection and entity.
	//
	// Parameters:
	//   - ctx: The context for managing cancellation and timeouts.
	//   - projectionName: The name of the projection that failed to process the event.
	//   - persistenceID: The persistence ID of the entity that produced the event.
	//   - event: The event that could not be processed.
	//   - revision: The sequence number of the event.
	//   - cause: The error that caused the failure.
	//
	// Returns:
	//   - error: If the dead letter handler itself fails.
	Handle(ctx context.Context, projectionName string, persistenceID string,
		event *anypb.Any, revision uint64, cause error) error
}

// DiscardDeadLetterHandler is a no-op implementation of DeadLetterHandler
// that silently discards dead-letter events.
type DiscardDeadLetterHandler struct{}

// enforce compliance with the DeadLetterHandler interface
var _ DeadLetterHandler = (*DiscardDeadLetterHandler)(nil)

// NewDiscardDeadLetterHandler creates an instance of DiscardDeadLetterHandler
func NewDiscardDeadLetterHandler() *DiscardDeadLetterHandler {
	return &DiscardDeadLetterHandler{}
}

// Handle discards the dead-letter event
// nolint
func (x *DiscardDeadLetterHandler) Handle(_ context.Context, _ string, _ string,
	_ *anypb.Any, _ uint64, _ error) error {
	return nil
}
