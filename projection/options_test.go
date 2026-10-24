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

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/tochemey/ego/v5/egopb"
)

// noopTransactionalHandler is a TransactionalHandler that does nothing.
type noopTransactionalHandler struct{}

// Handle does nothing.
func (noopTransactionalHandler) Handle(context.Context, string, *anypb.Any, uint64, *egopb.EntityOffset) error {
	return nil
}

func TestOptionsValidate(t *testing.T) {
	t.Run("accepts a handler", func(t *testing.T) {
		assert.NoError(t, (&Options{Handler: NewDiscardHandler()}).Validate())
	})

	t.Run("accepts a transactional handler", func(t *testing.T) {
		assert.NoError(t, (&Options{TransactionalHandler: noopTransactionalHandler{}}).Validate())
	})

	t.Run("requires a handler", func(t *testing.T) {
		assert.ErrorIs(t, (&Options{}).Validate(), ErrHandlerRequired)
	})

	t.Run("rejects both handlers", func(t *testing.T) {
		options := &Options{Handler: NewDiscardHandler(), TransactionalHandler: noopTransactionalHandler{}}
		assert.ErrorIs(t, options.Validate(), ErrConflictingHandlers)
	})
}
