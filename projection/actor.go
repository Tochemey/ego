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

	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/tochemey/goakt/v2/actors"
	"github.com/tochemey/goakt/v2/goaktpb"

	"github.com/tochemey/ego/eventstore"
	"github.com/tochemey/ego/offsetstore"
)

// Start is used to start the projection
var Start = new(emptypb.Empty)

// Projection defines the projection actor
// Only a single instance of this will run throughout the cluster
type Projection struct {
	runner *runner
}

// implements the Actor contract
var _ actors.Actor = (*Projection)(nil)

// New creates an instance of Projection
func New(name string,
	handler Handler,
	eventsStore eventstore.EventsStore,
	offsetStore offsetstore.OffsetStore,
	opts ...Option) *Projection {
	// create the instance of the runner
	runner := newRunner(name, handler, eventsStore, offsetStore, opts...)
	return &Projection{runner: runner}
}

// PreStart prepares the projection
func (x *Projection) PreStart(ctx context.Context) error {
	return x.runner.Start(ctx)
}

// Receive handle the message sent to the projection actor
func (x *Projection) Receive(ctx actors.ReceiveContext) {
	switch ctx.Message().(type) {
	case *goaktpb.PostStart:
		x.runner.Run(ctx.Context())
	default:
		ctx.Unhandled()
	}
}

// PostStop prepares the actor to gracefully shutdown
func (x *Projection) PostStop(ctx context.Context) error {
	return x.runner.Stop(ctx)
}
