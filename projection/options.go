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

import "time"

// Options holds the configuration required to run a projection handler.
type Options struct {
	// Handler defines how events are processed by the projection.
	Handler Handler
	// BufferSize controls how many events to buffer in memory before invoking the handler.
	BufferSize int
	// StartOffset is the point in time from which to begin processing events.
	StartOffset time.Time
	// ResetOffset is the fallback offset used when the projection needs to recover or replay events.
	ResetOffset time.Time
	// PullInterval defines how frequently the projection polls the event store for new events.
	PullInterval time.Duration
	// Recovery configures how the projection behaves when the handler fails to process an event.
	// When nil, a default recovery strategy is applied.
	Recovery *Recovery
}
