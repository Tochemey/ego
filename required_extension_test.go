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
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	goakt "github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/log"

	"github.com/tochemey/ego/v4/egopb"
	"github.com/tochemey/ego/v4/eventstream"
	"github.com/tochemey/ego/v4/internal/pause"
	"github.com/tochemey/ego/v4/offsetstore"
	"github.com/tochemey/ego/v4/persistence"
	"github.com/tochemey/ego/v4/testkit"
)

// TestRequiredExtensions asserts that the required-extension lookups return the
// stores the actor system was built with, and a start-up error rather than a
// panic when the actor system does not carry them.
func TestRequiredExtensions(t *testing.T) {
	ctx := context.TODO()

	t.Run("present", func(t *testing.T) {
		eventsStore := testkit.NewEventsStore()
		stateStore := testkit.NewDurableStore()
		offsetStore := testkit.NewOffsetStore()

		cfg := NewConfig(eventsStore,
			WithLogger(DiscardLogger),
			WithStateStore(stateStore),
			WithOffsetStore(offsetStore))

		actorSystem, err := goakt.NewActorSystem("TestSystem", cfg.GoaktOptions()...)
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		probe := new(extensionProbe)
		_, err = actorSystem.Spawn(ctx, uuid.NewString(), probe)
		require.NoError(t, err)

		probe.mutex.Lock()
		defer probe.mutex.Unlock()

		for _, lookupErr := range probe.errs {
			require.NoError(t, lookupErr)
		}

		// The events store is served through the node-wide shard-offsets
		// cache, which hands every other call to the registered store.
		_, servesSharedOffsets := probe.eventsStore.(shardOffsetsInvalidator)
		assert.True(t, servesSharedOffsets)

		written := &egopb.Event{PersistenceId: uuid.NewString(), SequenceNumber: 1, Timestamp: time.Now().UnixNano()}
		require.NoError(t, eventsStore.WriteEvents(ctx, []*egopb.Event{written}))
		latest, err := probe.eventsStore.GetLatestEvent(ctx, written.GetPersistenceId())
		require.NoError(t, err)
		assert.Equal(t, written.GetSequenceNumber(), latest.GetSequenceNumber())

		assert.Same(t, stateStore, probe.stateStore)
		assert.Same(t, offsetStore, probe.offsetStore)
		assert.Same(t, cfg.eventStream, probe.eventsStream)

		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("missing", func(t *testing.T) {
		// a bare actor system: none of eGo's extensions are registered
		actorSystem, err := goakt.NewActorSystem("TestSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))
		pause.For(time.Second)

		probe := new(extensionProbe)
		_, err = actorSystem.Spawn(ctx, uuid.NewString(), probe)
		require.NoError(t, err)

		probe.mutex.Lock()
		defer probe.mutex.Unlock()

		require.Len(t, probe.errs, 4)

		for _, lookupErr := range probe.errs {
			require.ErrorIs(t, lookupErr, ErrMissingRequiredExtensions)
		}

		require.NoError(t, actorSystem.Stop(ctx))
	})
}

// TestActorsFailToStartWithoutRequiredExtensions asserts that every eGo actor
// reports a missing required extension as a start-up error instead of
// panicking. The extensions are released while the actor system shuts down, so
// an actor starting at that moment must fail cleanly rather than crash the
// process.
func TestActorsFailToStartWithoutRequiredExtensions(t *testing.T) {
	ctx := context.TODO()

	// a bare actor system: none of eGo's extensions are registered
	actorSystem, err := goakt.NewActorSystem("TestSystem",
		goakt.WithLogger(log.DiscardLogger),
		goakt.WithActorInitMaxRetries(1))
	require.NoError(t, err)
	require.NoError(t, actorSystem.Start(ctx))
	pause.For(time.Second)

	actors := map[string]goakt.Actor{
		"event-sourced":  newEventSourcedActor(),
		"durable-state":  newDurableStateActor(),
		"saga":           newSagaActor(),
		"projection":     NewProjectionActor(),
		"events-writer":  newEventsWriterActor(),
		"events-janitor": newEventsJanitorActor(),
	}

	for kind, actor := range actors {
		t.Run(kind, func(t *testing.T) {
			pid, spawnErr := actorSystem.Spawn(ctx, uuid.NewString(), actor)
			require.ErrorIs(t, spawnErr, ErrMissingRequiredExtensions)
			require.Nil(t, pid)
		})
	}

	require.NoError(t, actorSystem.Stop(ctx))
}

// extensionProbe is a test actor that resolves every required extension in
// PreStart and records what each lookup returned, so the lookups can be
// asserted from the test goroutine once the actor has started.
type extensionProbe struct {
	mutex        sync.Mutex
	eventsStore  persistence.EventsStore
	eventsStream eventstream.Stream
	offsetStore  offsetstore.OffsetStore
	stateStore   persistence.StateStore
	errs         []error
}

var _ goakt.Actor = (*extensionProbe)(nil)

// PreStart runs the four lookups and records their results. It never fails so
// the test can inspect every result, including the errors.
func (p *extensionProbe) PreStart(ctx *goakt.Context) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	var err error

	p.eventsStore, err = requiredEventsStore(ctx)
	p.errs = append(p.errs, err)

	p.eventsStream, err = requiredEventsStream(ctx)
	p.errs = append(p.errs, err)

	p.offsetStore, err = requiredOffsetStore(ctx)
	p.errs = append(p.errs, err)

	p.stateStore, err = requiredStateStore(ctx)
	p.errs = append(p.errs, err)

	return nil
}

// Receive ignores every message.
func (p *extensionProbe) Receive(ctx *goakt.ReceiveContext) { ctx.Unhandled() }

// PostStop is a no-op.
func (p *extensionProbe) PostStop(_ *goakt.Context) error { return nil }
