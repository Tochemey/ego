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

package projection

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/tochemey/goakt/v2/actors"
	"github.com/tochemey/goakt/v2/log"

	"github.com/tochemey/ego/v3/egopb"
	"github.com/tochemey/ego/v3/eventstore/memory"
	memoffsetstore "github.com/tochemey/ego/v3/offsetstore/memory"
	testpb "github.com/tochemey/ego/v3/test/data/pb/v3"
)

func TestActor(t *testing.T) {
	t.Run("With happy path", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		ctx := context.TODO()
		// create an actor system
		actorSystem, err := actors.NewActorSystem("TestActorSystem",
			actors.WithPassivationDisabled(),
			actors.WithLogger(log.DiscardLogger),
			actors.WithActorInitMaxRetries(3))
		require.NoError(t, err)
		assert.NotNil(t, actorSystem)

		// start the actor system
		err = actorSystem.Start(ctx)
		require.NoError(t, err)

		time.Sleep(time.Second)

		projectionName := "db-writer"
		persistenceID := uuid.NewString()
		shardNumber := uint64(9)
		logger := log.DefaultLogger

		// set up the event store
		journalStore := memory.NewEventsStore()
		assert.NotNil(t, journalStore)
		require.NoError(t, journalStore.Connect(ctx))

		// set up the offset store
		offsetStore := memoffsetstore.NewOffsetStore()
		assert.NotNil(t, offsetStore)
		require.NoError(t, offsetStore.Disconnect(ctx))

		handler := NewDiscardHandler(logger)

		// create the actor
		actor := New(projectionName, handler, journalStore, offsetStore, WithRefreshInterval(time.Millisecond))
		// spawn the actor
		pid, err := actorSystem.Spawn(ctx, persistenceID, actor)
		require.NoError(t, err)
		require.NotNil(t, pid)

		time.Sleep(time.Second)

		// start the projection
		require.NoError(t, actors.Tell(ctx, pid, Start))

		// persist some events
		state, err := anypb.New(new(testpb.Account))
		assert.NoError(t, err)
		event, err := anypb.New(&testpb.AccountCredited{})
		assert.NoError(t, err)

		count := 10
		timestamp := timestamppb.Now()
		journals := make([]*egopb.Event, count)
		for i := 0; i < count; i++ {
			seqNr := i + 1
			journals[i] = &egopb.Event{
				PersistenceId:  persistenceID,
				SequenceNumber: uint64(seqNr),
				IsDeleted:      false,
				Event:          event,
				ResultingState: state,
				Timestamp:      timestamp.AsTime().Unix(),
				Shard:          shardNumber,
			}
		}

		require.NoError(t, journalStore.WriteEvents(ctx, journals))

		// wait for the data to be persisted by the database since this an eventual consistency case
		time.Sleep(time.Second)

		// create the projection id
		projectionID := &egopb.ProjectionId{
			ProjectionName: projectionName,
			ShardNumber:    shardNumber,
		}

		// let us grab the current offset
		actual, err := offsetStore.GetCurrentOffset(ctx, projectionID)
		assert.NoError(t, err)
		assert.NotNil(t, actual)
		assert.EqualValues(t, journals[9].GetTimestamp(), actual.GetValue())

		assert.EqualValues(t, 10, handler.EventsCount())

		// free resources
		assert.NoError(t, journalStore.Disconnect(ctx))
		assert.NoError(t, offsetStore.Disconnect(ctx))
		assert.NoError(t, actorSystem.Stop(ctx))
	})
}
