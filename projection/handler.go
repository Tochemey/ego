/*
 * MIT License
 *
 * Copyright (c) 2022-2024 Tochemey
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

package projection

import (
	"context"

	"go.uber.org/atomic"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/tochemey/goakt/v2/log"
)

// Handler is used to handle event and state consumed from the event store
type Handler interface {
	// Handle handles the event that is consumed by the projection
	Handle(ctx context.Context, persistenceID string, event *anypb.Any, state *anypb.Any, revision uint64) error
}

// DiscardHandler implements the projection Handler interface
// This handler really does nothing with the consumed event
// Note: this will be useful when writing unit tests
type DiscardHandler struct {
	eventsCounter *atomic.Int64
	logger        log.Logger
}

// enforce the complete implementation of the Handler interface
var _ Handler = (*DiscardHandler)(nil)

// NewDiscardHandler creates an instance of DiscardHandler
func NewDiscardHandler(logger log.Logger) *DiscardHandler {
	return &DiscardHandler{
		eventsCounter: atomic.NewInt64(0),
		logger:        logger,
	}
}

// Handle handles the events consumed
func (x *DiscardHandler) Handle(_ context.Context, persistenceID string, event *anypb.Any, state *anypb.Any, revision uint64) error {
	x.logger.Debugf("handling event=(%s) revision=(%d) with resulting state=(%s) of persistenceId=(%s)",
		event.GetTypeUrl(), revision, state.GetTypeUrl(), persistenceID)
	x.eventsCounter.Inc()
	return nil
}

// EventsCount returns the number of events processed
func (x *DiscardHandler) EventsCount() int {
	return int(x.eventsCounter.Load())
}
