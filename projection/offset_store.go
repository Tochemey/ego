/*
 * MIT License
 *
 * Copyright (c) 2002-2023 Tochemey
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

	"github.com/tochemey/ego/egopb"
)

// OffsetStore defines the contract needed to persist offsets
type OffsetStore interface {
	// Connect connects to the offset store
	Connect(ctx context.Context) error
	// Disconnect disconnects the offset store
	Disconnect(ctx context.Context) error
	// WriteOffset writes the current offset of the event consumed for a given projection ID
	// Note: persistence id and the projection name make a record in the journal store unique. Failure to ensure that
	// can lead to some un-wanted behaviors and data inconsistency
	WriteOffset(ctx context.Context, offset *egopb.Offset) error
	// GetCurrentOffset returns the current offset of a given projection ID
	GetCurrentOffset(ctx context.Context, projectionID *ID) (currentOffset *egopb.Offset, err error)
	// Ping verifies a connection to the database is still alive, establishing a connection if necessary.
	Ping(ctx context.Context) error
}

type name interface {
}
