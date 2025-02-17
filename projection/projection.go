/*
 * MIT License
 *
 * Copyright (c) 2023-2025 Tochemey
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

	goakt "github.com/tochemey/goakt/v3/actor"
	"github.com/tochemey/goakt/v3/goaktpb"

	"github.com/tochemey/ego/v3/offsetstore"
	"github.com/tochemey/ego/v3/persistence"
)

// Projection defines the projection actor
// Only a single instance of this will run throughout the cluster
type Projection struct {
	runner *runner
}

// implements the Actor contract
var _ goakt.Actor = (*Projection)(nil)

// New creates an instance of Projection
func New(name string,
	handler Handler,
	eventsStore persistence.EventsStore,
	offsetStore offsetstore.OffsetStore,
	opts ...Option) *Projection {
	// create the instance of the runner
	runner := newRunner(name, handler, eventsStore, offsetStore, opts...)
	return &Projection{runner: runner}
}

// PreStart prepares the projection
func (proj *Projection) PreStart(ctx context.Context) error {
	return proj.runner.Start(ctx)
}

// Receive handle the message sent to the projection actor
func (proj *Projection) Receive(ctx *goakt.ReceiveContext) {
	switch ctx.Message().(type) {
	case *goaktpb.PostStart:
		proj.runner.Run(ctx.Context())
	default:
		ctx.Unhandled()
	}
}

// PostStop prepares the actor to gracefully shutdown
func (proj *Projection) PostStop(context.Context) error {
	return proj.runner.Stop()
}
