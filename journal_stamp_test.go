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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/tochemey/ego/v5/egopb"
)

func TestStampEvents(t *testing.T) {
	t.Run("stamps the events a nanosecond apart from the current time", func(t *testing.T) {
		before := time.Now().UnixNano()
		envelopes := []*egopb.Event{{}, {}, {}}

		last := stampEvents(envelopes, 0)

		assert.GreaterOrEqual(t, envelopes[0].GetTimestamp(), before)
		assert.Equal(t, envelopes[0].GetTimestamp()+1, envelopes[1].GetTimestamp())
		assert.Equal(t, envelopes[1].GetTimestamp()+1, envelopes[2].GetTimestamp())
		assert.Equal(t, envelopes[2].GetTimestamp(), last)
	})

	t.Run("never stamps at or before the previous event when the clock is behind it", func(t *testing.T) {
		// the previous event was stamped by a clock an hour ahead of this one
		previous := time.Now().Add(time.Hour).UnixNano()
		envelopes := []*egopb.Event{{}, {}}

		last := stampEvents(envelopes, previous)

		assert.Equal(t, previous+1, envelopes[0].GetTimestamp())
		assert.Equal(t, previous+2, envelopes[1].GetTimestamp())
		assert.Equal(t, previous+2, last)
	})
}

func TestStampEventsWithoutEvents(t *testing.T) {
	assert.EqualValues(t, 42, stampEvents(nil, 42), "nothing written leaves the last timestamp unchanged")
}
