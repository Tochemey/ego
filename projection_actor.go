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
	goakt "github.com/tochemey/goakt/v3/actor"
	"github.com/tochemey/goakt/v3/goaktpb"

	"github.com/tochemey/ego/v3/internal/extensions"
)

// ProjectionActor defines the projection actor
// Only a single instance of this will run throughout the cluster
type ProjectionActor struct {
	runner *projectionRunner
}

// implements the Actor contract
var _ goakt.Actor = (*ProjectionActor)(nil)

// NewProjectionActor creates an instance of ProjectionActor
func NewProjectionActor() *ProjectionActor {
	return &ProjectionActor{}
}

// PreStart prepares the projection
func (x *ProjectionActor) PreStart(ctx *goakt.Context) error {
	offsetStore := ctx.Extension(extensions.OffsetStoreExtensionID).(*extensions.OffsetStore).Underlying()
	eventsStore := ctx.Extension(extensions.EventsStoreExtensionID).(*extensions.EventsStore).Underlying()
	projection := ctx.Extension(extensions.ProjectionExtensionID).(*extensions.ProjectionExtension)

	x.runner = newProjectionRunner(ctx.ActorName(), projection.Handler(), eventsStore, offsetStore,
		withLogger(ctx.ActorSystem().Logger()),
		withRecoveryStrategy(projection.Recovery()),
		withStartOffset(projection.StartOffset()),
		withResetOffset(projection.ResetOffset()),
		withMaxBufferSize(projection.BufferSize()),
		withPullInterval(projection.PullInterval()),
	)

	return x.runner.Start(ctx.Context())
}

// Receive handle the message sent to the projection actor
func (x *ProjectionActor) Receive(ctx *goakt.ReceiveContext) {
	switch ctx.Message().(type) {
	case *goaktpb.PostStart:
		x.runner.Run(ctx.Context())
	default:
		ctx.Unhandled()
	}
}

// PostStop prepares the actor to gracefully shutdown
func (x *ProjectionActor) PostStop(*goakt.Context) error {
	return x.runner.Stop()
}
