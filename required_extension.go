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
	"fmt"

	goakt "github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/extension"

	"github.com/tochemey/ego/v4/eventstream"
	"github.com/tochemey/ego/v4/internal/extensions"
	"github.com/tochemey/ego/v4/offsetstore"
	"github.com/tochemey/ego/v4/persistence"
)

// requiredExtension returns the extension registered under id as T.
//
// A missing extension is reported as an error wrapping
// ErrMissingRequiredExtensions instead of a panic. Go-Akt releases the
// extensions while the actor system shuts down and does not recover a panic in
// PreStart, so an actor starting at that moment would otherwise crash the
// process.
func requiredExtension[T extension.Extension](ctx *goakt.Context, id string) (T, error) {
	ext, ok := ctx.Extension(id).(T)
	if !ok {
		var missing T
		return missing, fmt.Errorf("%w: %s", ErrMissingRequiredExtensions, id)
	}

	return ext, nil
}

// requiredEventsStore returns the events store registered on the actor system.
func requiredEventsStore(ctx *goakt.Context) (persistence.EventsStore, error) {
	ext, err := requiredExtension[*extensions.EventsStore](ctx, extensions.EventsStoreExtensionID)
	if err != nil {
		return nil, err
	}

	return ext.Underlying(), nil
}

// requiredEventsStream returns the in-process event stream registered on the
// actor system.
func requiredEventsStream(ctx *goakt.Context) (eventstream.Stream, error) {
	ext, err := requiredExtension[*extensions.EventsStream](ctx, extensions.EventsStreamExtensionID)
	if err != nil {
		return nil, err
	}

	return ext.Underlying(), nil
}

// requiredOffsetStore returns the offset store registered on the actor system.
func requiredOffsetStore(ctx *goakt.Context) (offsetstore.OffsetStore, error) {
	ext, err := requiredExtension[*extensions.OffsetStore](ctx, extensions.OffsetStoreExtensionID)
	if err != nil {
		return nil, err
	}

	return ext.Underlying(), nil
}

// requiredStateStore returns the durable state store registered on the actor
// system.
func requiredStateStore(ctx *goakt.Context) (persistence.StateStore, error) {
	ext, err := requiredExtension[*extensions.DurableStateStore](ctx, extensions.DurableStateStoreExtensionID)
	if err != nil {
		return nil, err
	}

	return ext.Underlying(), nil
}
