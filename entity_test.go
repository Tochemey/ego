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

package ego

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tochemey/ego/v2/eventstore/memory"
	samplepb "github.com/tochemey/ego/v2/example/pbs/sample/pb/v1"
)

func TestNewEntity(t *testing.T) {
	t.Run("With engine not defined", func(t *testing.T) {
		ctx := context.TODO()
		behavior := NewAccountBehavior(uuid.NewString())
		// create an entity
		entity, err := NewEntity[*samplepb.Account](ctx, behavior, nil)
		require.Error(t, err)
		require.Nil(t, entity)
		assert.EqualError(t, err, ErrEngineRequired.Error())
	})
	t.Run("With engine not started", func(t *testing.T) {
		ctx := context.TODO()
		// create the event store
		eventStore := memory.NewEventsStore()
		// create the ego engine
		engine := NewEngine("Sample", eventStore)
		behavior := NewAccountBehavior(uuid.NewString())
		// create an entity
		entity, err := NewEntity[*samplepb.Account](ctx, behavior, engine)
		require.Error(t, err)
		require.Nil(t, entity)
		assert.EqualError(t, err, ErrEngineNotStarted.Error())
	})
}

func TestSendCommand(t *testing.T) {
	t.Run("With entity not defined", func(t *testing.T) {
		ctx := context.TODO()
		entity := &Entity[*samplepb.Account]{}
		resultingState, revision, err := entity.SendCommand(ctx, new(samplepb.CreateAccount))
		require.Nil(t, resultingState)
		require.Zero(t, revision)
		require.Error(t, err)
		assert.EqualError(t, err, ErrUndefinedEntity.Error())
	})
}
