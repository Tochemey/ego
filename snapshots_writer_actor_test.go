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
	"github.com/tochemey/ego/v4/encryption"
	"github.com/tochemey/ego/v4/eventstream"
	"github.com/tochemey/ego/v4/internal/extensions"
	"github.com/tochemey/ego/v4/internal/pause"
	mockencryption "github.com/tochemey/ego/v4/mocks/encryption"
	mocks "github.com/tochemey/ego/v4/mocks/persistence"
	"github.com/tochemey/ego/v4/testkit"
)

func TestSnapshotsWriterActor(t *testing.T) {
	t.Run("persists snapshot to store on success", func(t *testing.T) {
		ctx := context.TODO()

		snapshotStore := testkit.NewSnapshotStore()
		require.NoError(t, snapshotStore.Connect(ctx))

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		eventStream := eventstream.New()

		actorSystem, err := goakt.NewActorSystem("TestSnapshotSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
				extensions.NewSnapshotStore(snapshotStore),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))

		pause.For(time.Second)

		pid, err := actorSystem.Spawn(ctx, "snapshot-writer-test", newSnapshotsWriterActor())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		stateAny, _ := anypb.New(&egopb.NoReply{})
		snapshot := &egopb.Snapshot{
			PersistenceId:  "entity-1",
			SequenceNumber: 10,
			State:          stateAny,
			Timestamp:      time.Now().Unix(),
		}

		err = goakt.Tell(ctx, pid, &persistSnapshotRequest{snapshot: snapshot})
		require.NoError(t, err)

		pause.For(time.Second)

		latest, err := snapshotStore.GetLatestSnapshot(ctx, "entity-1")
		require.NoError(t, err)
		require.NotNil(t, latest)
		assert.EqualValues(t, 10, latest.GetSequenceNumber())

		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, snapshotStore.Disconnect(ctx))
		eventStream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("encrypts snapshot state before writing when encryptor is configured", func(t *testing.T) {
		ctx := context.TODO()

		snapshotStore := testkit.NewSnapshotStore()
		require.NoError(t, snapshotStore.Connect(ctx))

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		eventStream := eventstream.New()

		keyStore := testkit.NewKeyStore()
		encryptor := encryption.NewAESEncryptor(keyStore)

		actorSystem, err := goakt.NewActorSystem("TestSnapshotSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
				extensions.NewSnapshotStore(snapshotStore),
				extensions.NewEncryptor(encryptor),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))

		pause.For(time.Second)

		pid, err := actorSystem.Spawn(ctx, "snapshot-writer-test", newSnapshotsWriterActor())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		stateAny, _ := anypb.New(&egopb.NoReply{})
		snapshot := &egopb.Snapshot{
			PersistenceId:  "entity-1",
			SequenceNumber: 5,
			State:          stateAny,
			Timestamp:      time.Now().Unix(),
		}

		err = goakt.Tell(ctx, pid, &persistSnapshotRequest{snapshot: snapshot})
		require.NoError(t, err)

		pause.For(time.Second)

		latest, err := snapshotStore.GetLatestSnapshot(ctx, "entity-1")
		require.NoError(t, err)
		require.NotNil(t, latest)
		assert.EqualValues(t, 5, latest.GetSequenceNumber())
		assert.True(t, latest.GetIsEncrypted())
		assert.NotEmpty(t, latest.GetEncryptionKeyId())

		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, snapshotStore.Disconnect(ctx))
		eventStream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("forwards retention request to janitor after successful write", func(t *testing.T) {
		ctx := context.TODO()

		snapshotStore := testkit.NewSnapshotStore()
		require.NoError(t, snapshotStore.Connect(ctx))

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		eventStream := eventstream.New()

		eventsStoreMock := new(mocks.EventsStore)
		eventsStoreMock.EXPECT().Ping(mock.Anything).Return(nil).Maybe()
		eventsStoreMock.EXPECT().DeleteEvents(mock.Anything, "entity-1", uint64(10)).Return(nil)

		actorSystem, err := goakt.NewActorSystem("TestSnapshotSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventsStoreMock),
				extensions.NewEventsStream(eventStream),
				extensions.NewSnapshotStore(snapshotStore),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))

		pause.For(time.Second)

		snapshotPID, err := actorSystem.Spawn(ctx, "snapshot-writer-test", newSnapshotsWriterActor())
		require.NoError(t, err)
		require.NotNil(t, snapshotPID)

		janitorPID, err := actorSystem.Spawn(ctx, "janitor-test", newEventsJanitorActor())
		require.NoError(t, err)
		require.NotNil(t, janitorPID)

		pause.For(time.Second)

		stateAny, _ := anypb.New(&egopb.NoReply{})
		snapshot := &egopb.Snapshot{
			PersistenceId:  "entity-1",
			SequenceNumber: 10,
			State:          stateAny,
			Timestamp:      time.Now().Unix(),
		}

		err = goakt.Tell(ctx, snapshotPID, &persistSnapshotRequest{
			snapshot: snapshot,
			retentionReq: &applyRetentionRequest{
				persistenceID:          "entity-1",
				eventsCounter:          10,
				snapshotInterval:       5,
				deleteEventsOnSnapshot: true,
			},
			janitor: janitorPID,
		})
		require.NoError(t, err)

		pause.For(2 * time.Second)

		eventsStoreMock.AssertExpectations(t)

		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, snapshotStore.Disconnect(ctx))
		eventStream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("does not forward retention when snapshot write fails", func(t *testing.T) {
		ctx := context.TODO()

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		eventStream := eventstream.New()

		snapshotStore := new(mocks.SnapshotStore)
		snapshotStore.EXPECT().Ping(mock.Anything).Return(nil).Maybe()
		snapshotStore.EXPECT().WriteSnapshot(mock.Anything, mock.Anything).Return(assert.AnError)

		eventsStoreMock := new(mocks.EventsStore)
		eventsStoreMock.EXPECT().Ping(mock.Anything).Return(nil).Maybe()

		actorSystem, err := goakt.NewActorSystem("TestSnapshotSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventsStoreMock),
				extensions.NewEventsStream(eventStream),
				extensions.NewSnapshotStore(snapshotStore),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))

		pause.For(time.Second)

		snapshotPID, err := actorSystem.Spawn(ctx, "snapshot-writer-test", newSnapshotsWriterActor())
		require.NoError(t, err)
		require.NotNil(t, snapshotPID)

		janitorPID, err := actorSystem.Spawn(ctx, "janitor-test", newEventsJanitorActor())
		require.NoError(t, err)
		require.NotNil(t, janitorPID)

		pause.For(time.Second)

		stateAny, _ := anypb.New(&egopb.NoReply{})
		snapshot := &egopb.Snapshot{
			PersistenceId:  "entity-1",
			SequenceNumber: 10,
			State:          stateAny,
			Timestamp:      time.Now().Unix(),
		}

		err = goakt.Tell(ctx, snapshotPID, &persistSnapshotRequest{
			snapshot: snapshot,
			retentionReq: &applyRetentionRequest{
				persistenceID:          "entity-1",
				eventsCounter:          10,
				snapshotInterval:       5,
				deleteEventsOnSnapshot: true,
			},
			janitor: janitorPID,
		})
		require.NoError(t, err)

		pause.For(2 * time.Second)

		assert.True(t, snapshotPID.IsRunning())
		eventsStoreMock.AssertNotCalled(t, "DeleteEvents", mock.Anything, mock.Anything, mock.Anything)

		require.NoError(t, eventStore.Disconnect(ctx))
		eventStream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("logs error and continues when store write fails", func(t *testing.T) {
		ctx := context.TODO()

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		eventStream := eventstream.New()

		snapshotStore := new(mocks.SnapshotStore)
		snapshotStore.EXPECT().Ping(mock.Anything).Return(nil).Maybe()
		snapshotStore.EXPECT().WriteSnapshot(mock.Anything, mock.Anything).Return(assert.AnError)

		actorSystem, err := goakt.NewActorSystem("TestSnapshotSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
				extensions.NewSnapshotStore(snapshotStore),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))

		pause.For(time.Second)

		pid, err := actorSystem.Spawn(ctx, "snapshot-writer-test", newSnapshotsWriterActor())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		stateAny, _ := anypb.New(&egopb.NoReply{})
		snapshot := &egopb.Snapshot{
			PersistenceId:  "entity-1",
			SequenceNumber: 10,
			State:          stateAny,
			Timestamp:      time.Now().Unix(),
		}

		err = goakt.Tell(ctx, pid, &persistSnapshotRequest{snapshot: snapshot})
		require.NoError(t, err)

		pause.For(2 * time.Second)

		assert.True(t, pid.IsRunning())

		require.NoError(t, eventStore.Disconnect(ctx))
		eventStream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("logs error and continues when encryption fails", func(t *testing.T) {
		ctx := context.TODO()

		snapshotStore := testkit.NewSnapshotStore()
		require.NoError(t, snapshotStore.Connect(ctx))

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		eventStream := eventstream.New()

		encryptor := new(mockencryption.Encryptor)
		encryptor.EXPECT().Encrypt(mock.Anything, "entity-1", mock.Anything).Return(nil, "", assert.AnError)

		actorSystem, err := goakt.NewActorSystem("TestSnapshotSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
				extensions.NewSnapshotStore(snapshotStore),
				extensions.NewEncryptor(encryptor),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))

		pause.For(time.Second)

		pid, err := actorSystem.Spawn(ctx, "snapshot-writer-test", newSnapshotsWriterActor())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		stateAny, _ := anypb.New(&egopb.NoReply{})
		snapshot := &egopb.Snapshot{
			PersistenceId:  "entity-1",
			SequenceNumber: 10,
			State:          stateAny,
			Timestamp:      time.Now().Unix(),
		}

		err = goakt.Tell(ctx, pid, &persistSnapshotRequest{snapshot: snapshot})
		require.NoError(t, err)

		pause.For(time.Second)

		assert.True(t, pid.IsRunning())

		// verify no snapshot was written since encryption failed
		latest, err := snapshotStore.GetLatestSnapshot(ctx, "entity-1")
		require.NoError(t, err)
		assert.Nil(t, latest)

		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, snapshotStore.Disconnect(ctx))
		eventStream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("handles nil snapshot store gracefully", func(t *testing.T) {
		ctx := context.TODO()

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		eventStream := eventstream.New()

		actorSystem, err := goakt.NewActorSystem("TestSnapshotSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))

		pause.For(time.Second)

		pid, err := actorSystem.Spawn(ctx, "snapshot-writer-test", newSnapshotsWriterActor())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		stateAny, _ := anypb.New(&egopb.NoReply{})
		snapshot := &egopb.Snapshot{
			PersistenceId:  "entity-1",
			SequenceNumber: 10,
			State:          stateAny,
			Timestamp:      time.Now().Unix(),
		}

		err = goakt.Tell(ctx, pid, &persistSnapshotRequest{snapshot: snapshot})
		require.NoError(t, err)

		pause.For(time.Second)

		assert.True(t, pid.IsRunning())

		require.NoError(t, eventStore.Disconnect(ctx))
		eventStream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})

	t.Run("marks unhandled messages as unhandled", func(t *testing.T) {
		ctx := context.TODO()

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		eventStream := eventstream.New()

		actorSystem, err := goakt.NewActorSystem("TestSnapshotSystem",
			goakt.WithLogger(log.DiscardLogger),
			goakt.WithExtensions(
				extensions.NewEventsStore(eventStore),
				extensions.NewEventsStream(eventStream),
			),
			goakt.WithActorInitMaxRetries(1))
		require.NoError(t, err)
		require.NoError(t, actorSystem.Start(ctx))

		pause.For(time.Second)

		pid, err := actorSystem.Spawn(ctx, "snapshot-writer-test", newSnapshotsWriterActor())
		require.NoError(t, err)
		require.NotNil(t, pid)

		pause.For(time.Second)

		reply, err := goakt.Ask(ctx, pid, &egopb.NoReply{}, 5*time.Second)
		require.Error(t, err)
		assert.Nil(t, reply)

		require.NoError(t, eventStore.Disconnect(ctx))
		eventStream.Close()
		require.NoError(t, actorSystem.Stop(ctx))
	})
}
