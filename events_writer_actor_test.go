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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	goakt "github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/log"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/tochemey/ego/v4/egopb"
	"github.com/tochemey/ego/v4/eventstream"
	"github.com/tochemey/ego/v4/internal/extensions"
	"github.com/tochemey/ego/v4/internal/pause"
	mocks "github.com/tochemey/ego/v4/mocks/persistence"
	"github.com/tochemey/ego/v4/testkit"
)

func TestEventsWriterActor(t *testing.T) {
	t.Run("persists events and publishes to stream on success", func(t *testing.T) {
		ctx := context.TODO()

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		eventStream := eventstream.New()
		sub := eventStream.AddSubscriber()
		eventStream.Subscribe(sub, "topic.events.0")

		actorSystem, err := goakt.NewActorSystem("TestWriterSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))

		pause.For(time.Second)

		pid, err := actorSystem.Spawn(ctx, "event-writer-test", newEventsWriterActor())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		eventAny, _ := anypb.New(&egopb.NoReply{})
		envelopes := []*egopb.Event{
			{
				PersistenceId:  "entity-1",
				SequenceNumber: 1,
				Event:          eventAny,
				Timestamp:      time.Now().Unix(),
			},
			{
				PersistenceId:  "entity-1",
				SequenceNumber: 2,
				Event:          eventAny,
				Timestamp:      time.Now().Unix(),
			},
		}

		reply, err := goakt.Ask(ctx, pid, &persistEventsRequest{
			envelopes: envelopes,
			topic:     "topic.events.0",
		}, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)

		resp, ok := reply.(*persistEventsResponse)
		require.True(t, ok)
		assert.Nil(t, resp.Err)

		// verify events were written to the store
		latest, err := eventStore.GetLatestEvent(ctx, "entity-1")
		require.NoError(t, err)
		require.NotNil(t, latest)
		assert.EqualValues(t, 2, latest.GetSequenceNumber())

		// verify events were published to the stream
		var published int
		for i := 0; i < len(envelopes); i++ {
			select {
			case <-sub.Iterator():
				published++
			case <-time.After(2 * time.Second):
				t.Fatal("timed out waiting for published event")
			}
		}
		assert.Equal(t, 2, published)

		require.NoError(t, eventStore.Disconnect(ctx))
		eventStream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("returns error in response when store write fails", func(t *testing.T) {
		ctx := context.TODO()

		eventStore := new(mocks.EventsStore)
		eventStore.EXPECT().Ping(mock.Anything).Return(nil).Maybe()
		eventStore.EXPECT().WriteEvents(mock.Anything, mock.Anything).Return(assert.AnError)

		eventStream := eventstream.New()
		sub := eventStream.AddSubscriber()
		eventStream.Subscribe(sub, "topic.events.0")

		actorSystem, err := goakt.NewActorSystem("TestWriterSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))

		pause.For(time.Second)

		pid, err := actorSystem.Spawn(ctx, "event-writer-test", newEventsWriterActor())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		eventAny, _ := anypb.New(&egopb.NoReply{})
		envelopes := []*egopb.Event{
			{
				PersistenceId:  "entity-1",
				SequenceNumber: 1,
				Event:          eventAny,
				Timestamp:      time.Now().Unix(),
			},
		}

		reply, err := goakt.Ask(ctx, pid, &persistEventsRequest{
			envelopes: envelopes,
			topic:     "topic.events.0",
		}, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)

		resp, ok := reply.(*persistEventsResponse)
		require.True(t, ok)
		assert.Error(t, resp.Err)
		assert.ErrorIs(t, resp.Err, assert.AnError)

		// verify no events were published to the stream by draining
		// any messages within a short window
		timer := time.NewTimer(500 * time.Millisecond)
		defer timer.Stop()
		select {
		case msg := <-sub.Iterator():
			if msg != nil {
				t.Fatal("event should not have been published on store failure")
			}
		case <-timer.C:
			// expected: no event published
		}

		eventStream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("handles empty envelopes list", func(t *testing.T) {
		ctx := context.TODO()

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		eventStream := eventstream.New()

		actorSystem, err := goakt.NewActorSystem("TestWriterSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))

		pause.For(time.Second)

		pid, err := actorSystem.Spawn(ctx, "event-writer-test", newEventsWriterActor())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		reply, err := goakt.Ask(ctx, pid, &persistEventsRequest{
			envelopes: nil,
			topic:     "topic.events.0",
		}, 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, reply)

		resp, ok := reply.(*persistEventsResponse)
		require.True(t, ok)
		assert.Nil(t, resp.Err)

		require.NoError(t, eventStore.Disconnect(ctx))
		eventStream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("marks unhandled messages as unhandled", func(t *testing.T) {
		ctx := context.TODO()

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		eventStream := eventstream.New()

		actorSystem, err := goakt.NewActorSystem("TestWriterSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))

		pause.For(time.Second)

		pid, err := actorSystem.Spawn(ctx, "event-writer-test", newEventsWriterActor())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		// send an unexpected message type
		reply, err := goakt.Ask(ctx, pid, &egopb.NoReply{}, 5*time.Second)
		require.Error(t, err)
		assert.Nil(t, reply)

		require.NoError(t, eventStore.Disconnect(ctx))
		eventStream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})
}
