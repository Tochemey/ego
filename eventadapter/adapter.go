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

package eventadapter

import (
	"google.golang.org/protobuf/types/known/anypb"
)

// EventAdapter transforms persisted events from older schema versions
// into the current expected shape. This is applied transparently during
// event replay (entity recovery) and projection consumption.
//
// Adapters are applied in registration order. Each adapter inspects the event
// and decides whether to transform it. If multiple adapters match, they are
// chained: the output of one becomes the input of the next.
type EventAdapter interface {
	// Adapt inspects the given event and optionally transforms it.
	// If the adapter does not apply to this event, it returns the
	// event unchanged with no error.
	//
	// The revision parameter is the event's sequence number, which
	// can be used to distinguish between different schema eras for
	// the same type URL.
	Adapt(event *anypb.Any, revision uint64) (*anypb.Any, error)
}

// Chain applies a sequence of EventAdapter instances to an event.
// Adapters are applied in order; each receives the output of the previous.
// If any adapter returns an error, the chain stops and returns that error.
func Chain(adapters []EventAdapter, event *anypb.Any, revision uint64) (*anypb.Any, error) {
	current := event
	for _, adapter := range adapters {
		adapted, err := adapter.Adapt(current, revision)
		if err != nil {
			return nil, err
		}
		current = adapted
	}
	return current, nil
}
