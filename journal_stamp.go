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
	"time"

	"github.com/tochemey/ego/v5/egopb"
)

// stampEvents stamps the events of one write of a persistence ID, just before
// they are written, and returns the timestamp of the last one.
//
// previous is the timestamp of the persistence ID's latest event already
// written, or zero for its first write; it is returned unchanged when there is
// no event to stamp. The events get timestamps a nanosecond
// apart, starting at the current time or right after previous, whichever is
// later. A persistence ID's timestamps therefore always increase, even when the
// clock steps back or the entity moves to a node whose clock is behind:
// projections read a shard in timestamp order, and an event stamped before one
// of its entity's earlier events could fall behind a position a projection has
// already committed.
func stampEvents(envelopes []*egopb.Event, previous int64) int64 {
	if len(envelopes) == 0 {
		return previous
	}

	stamp := max(time.Now().UnixNano(), previous+1)
	for index, envelope := range envelopes {
		envelope.Timestamp = stamp + int64(index)
	}

	return stamp + int64(len(envelopes)-1)
}
