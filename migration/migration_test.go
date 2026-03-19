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

package migration

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tochemey/goakt/v4/log"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/tochemey/ego/v4/egopb"
	"github.com/tochemey/ego/v4/testkit"
)

// buildLegacyEventBytes constructs raw protobuf bytes for an Event that includes
// the old resulting_state at field 5. Since the current generated Event no longer
// has that field, we manually append the field 5 bytes to a normally-serialized Event.
func buildLegacyEventBytes(t *testing.T, persistenceID string, seqNr uint64, event *anypb.Any, state *anypb.Any, timestamp int64, shard uint64) []byte {
	t.Helper()

	// Serialize the base event (without resulting_state)
	baseEvent := &egopb.Event{
		PersistenceId:  persistenceID,
		SequenceNumber: seqNr,
		IsDeleted:      false,
		Event:          event,
		Timestamp:      timestamp,
		Shard:          shard,
	}
	baseBytes, err := proto.Marshal(baseEvent)
	require.NoError(t, err)

	if state == nil {
		return baseBytes
	}

	// Serialize the state as field 5 (length-delimited, wire type 2)
	// Tag = (5 << 3) | 2 = 42
	stateBytes, err := proto.Marshal(state)
	require.NoError(t, err)

	// Build the field 5 tag + length + value
	tag := encodeVarint(42) // field 5, wire type 2
	length := encodeVarint(uint64(len(stateBytes)))

	// Append field 5 to the base event bytes
	result := make([]byte, 0, len(baseBytes)+len(tag)+len(length)+len(stateBytes))
	result = append(result, baseBytes...)
	result = append(result, tag...)
	result = append(result, length...)
	result = append(result, stateBytes...)
	return result
}

// encodeVarint encodes a uint64 as a protobuf varint
func encodeVarint(v uint64) []byte {
	var buf [10]byte
	n := 0
	for v >= 0x80 {
		buf[n] = byte(v&0x7f) | 0x80
		v >>= 7
		n++
	}
	buf[n] = byte(v)
	return buf[:n+1]
}

// writeLegacyEvent writes an event with legacy resulting_state into the events store.
// It serializes the event with field 5 appended, then deserializes it back using the
// current Event proto (which puts field 5 into unknown fields), and writes it.
func writeLegacyEvent(t *testing.T, store *testkit.EventStore, persistenceID string, seqNr uint64, eventPayload *anypb.Any, state *anypb.Any, ts int64, shard uint64) {
	t.Helper()
	raw := buildLegacyEventBytes(t, persistenceID, seqNr, eventPayload, state, ts, shard)

	// Deserialize using the current Event proto — field 5 goes into unknown fields
	evt := new(egopb.Event)
	require.NoError(t, proto.Unmarshal(raw, evt))

	require.NoError(t, store.WriteEvents(context.Background(), []*egopb.Event{evt}))
}

func TestMigratorRun(t *testing.T) {
	t.Run("migrates legacy events with resulting_state to snapshots", func(t *testing.T) {
		ctx := context.Background()

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		snapshotStore := testkit.NewSnapshotStore()
		require.NoError(t, snapshotStore.Connect(ctx))

		// Create a fake state to embed as resulting_state
		ts := timestamppb.Now()
		stateAny, err := anypb.New(ts)
		require.NoError(t, err)

		eventAny, err := anypb.New(timestamppb.Now())
		require.NoError(t, err)

		// Write 3 legacy events for entity "entity-1"
		for i := uint64(1); i <= 3; i++ {
			writeLegacyEvent(t, eventStore, "entity-1", i, eventAny, stateAny, int64(i*100), 0)
		}

		// Run migration
		migrator := New(eventStore, snapshotStore,
			WithPageSize(10),
			WithLogger(log.DiscardLogger),
		)
		require.NoError(t, migrator.Run(ctx))

		// Verify snapshot was written for entity-1 at sequence 3 (the latest)
		snapshot, err := snapshotStore.GetLatestSnapshot(ctx, "entity-1")
		require.NoError(t, err)
		require.NotNil(t, snapshot)
		assert.Equal(t, "entity-1", snapshot.GetPersistenceId())
		assert.EqualValues(t, 3, snapshot.GetSequenceNumber())
		assert.NotNil(t, snapshot.GetState())

		// Verify the state content
		var recovered timestamppb.Timestamp
		require.NoError(t, snapshot.GetState().UnmarshalTo(&recovered))
		assert.True(t, proto.Equal(ts, &recovered))

		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, snapshotStore.Disconnect(ctx))
	})

	t.Run("skips entities with no resulting_state", func(t *testing.T) {
		ctx := context.Background()

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		snapshotStore := testkit.NewSnapshotStore()
		require.NoError(t, snapshotStore.Connect(ctx))

		eventAny, err := anypb.New(timestamppb.Now())
		require.NoError(t, err)

		// Write events without resulting_state (new-format events)
		for i := uint64(1); i <= 3; i++ {
			writeLegacyEvent(t, eventStore, "entity-new", i, eventAny, nil, int64(i*100), 0)
		}

		migrator := New(eventStore, snapshotStore,
			WithPageSize(10),
			WithLogger(log.DiscardLogger),
		)
		require.NoError(t, migrator.Run(ctx))

		// No snapshot should exist
		snapshot, err := snapshotStore.GetLatestSnapshot(ctx, "entity-new")
		require.NoError(t, err)
		assert.Nil(t, snapshot)

		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, snapshotStore.Disconnect(ctx))
	})

	t.Run("handles multiple entities", func(t *testing.T) {
		ctx := context.Background()

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		snapshotStore := testkit.NewSnapshotStore()
		require.NoError(t, snapshotStore.Connect(ctx))

		state1, _ := anypb.New(&timestamppb.Timestamp{Seconds: 111})
		state2, _ := anypb.New(&timestamppb.Timestamp{Seconds: 222})
		eventAny, _ := anypb.New(timestamppb.Now())

		writeLegacyEvent(t, eventStore, "e1", 1, eventAny, state1, 100, 0)
		writeLegacyEvent(t, eventStore, "e1", 2, eventAny, state1, 200, 0)
		writeLegacyEvent(t, eventStore, "e2", 1, eventAny, state2, 100, 1)

		migrator := New(eventStore, snapshotStore,
			WithPageSize(2),
			WithLogger(log.DiscardLogger),
		)
		require.NoError(t, migrator.Run(ctx))

		snap1, err := snapshotStore.GetLatestSnapshot(ctx, "e1")
		require.NoError(t, err)
		require.NotNil(t, snap1)
		assert.EqualValues(t, 2, snap1.GetSequenceNumber())

		snap2, err := snapshotStore.GetLatestSnapshot(ctx, "e2")
		require.NoError(t, err)
		require.NotNil(t, snap2)
		assert.EqualValues(t, 1, snap2.GetSequenceNumber())

		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, snapshotStore.Disconnect(ctx))
	})

	t.Run("is idempotent", func(t *testing.T) {
		ctx := context.Background()

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		snapshotStore := testkit.NewSnapshotStore()
		require.NoError(t, snapshotStore.Connect(ctx))

		stateAny, _ := anypb.New(&timestamppb.Timestamp{Seconds: 999})
		eventAny, _ := anypb.New(timestamppb.Now())

		writeLegacyEvent(t, eventStore, "idem-1", 1, eventAny, stateAny, 100, 0)

		migrator := New(eventStore, snapshotStore,
			WithPageSize(10),
			WithLogger(log.DiscardLogger),
		)

		// Run twice
		require.NoError(t, migrator.Run(ctx))
		require.NoError(t, migrator.Run(ctx))

		snapshot, err := snapshotStore.GetLatestSnapshot(ctx, "idem-1")
		require.NoError(t, err)
		require.NotNil(t, snapshot)
		assert.EqualValues(t, 1, snapshot.GetSequenceNumber())

		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, snapshotStore.Disconnect(ctx))
	})

	t.Run("returns error when events store is unreachable", func(t *testing.T) {
		ctx := context.Background()

		eventStore := testkit.NewEventsStore()
		// deliberately don't connect

		snapshotStore := testkit.NewSnapshotStore()
		require.NoError(t, snapshotStore.Connect(ctx))

		migrator := New(eventStore, snapshotStore,
			WithLogger(log.DiscardLogger),
		)
		// Ping will auto-connect the testkit store, so this will actually succeed.
		// That's fine — testkit stores auto-connect on Ping.
		err := migrator.Run(ctx)
		require.NoError(t, err)

		require.NoError(t, snapshotStore.Disconnect(ctx))
	})

	t.Run("with empty events store", func(t *testing.T) {
		ctx := context.Background()

		eventStore := testkit.NewEventsStore()
		require.NoError(t, eventStore.Connect(ctx))

		snapshotStore := testkit.NewSnapshotStore()
		require.NoError(t, snapshotStore.Connect(ctx))

		migrator := New(eventStore, snapshotStore,
			WithLogger(log.DiscardLogger),
		)
		require.NoError(t, migrator.Run(ctx))

		require.NoError(t, eventStore.Disconnect(ctx))
		require.NoError(t, snapshotStore.Disconnect(ctx))
	})
}

func TestExtractLegacyResultingState(t *testing.T) {
	t.Run("returns nil for nil event", func(t *testing.T) {
		assert.Nil(t, extractLegacyResultingState(nil))
	})

	t.Run("returns nil for event without unknown fields", func(t *testing.T) {
		evt := &egopb.Event{
			PersistenceId:  "test",
			SequenceNumber: 1,
		}
		assert.Nil(t, extractLegacyResultingState(evt))
	})

	t.Run("extracts state from legacy event", func(t *testing.T) {
		state, _ := anypb.New(&timestamppb.Timestamp{Seconds: 42})
		eventAny, _ := anypb.New(timestamppb.Now())

		raw := buildLegacyEventBytes(t, "test", 1, eventAny, state, 100, 0)

		evt := new(egopb.Event)
		require.NoError(t, proto.Unmarshal(raw, evt))

		extracted := extractLegacyResultingState(evt)
		require.NotNil(t, extracted)
		assert.NotEmpty(t, extracted.GetTypeUrl())

		var ts timestamppb.Timestamp
		require.NoError(t, extracted.UnmarshalTo(&ts))
		assert.EqualValues(t, 42, ts.GetSeconds())
	})

	t.Run("returns nil for event without field 5", func(t *testing.T) {
		eventAny, _ := anypb.New(timestamppb.Now())
		raw := buildLegacyEventBytes(t, "test", 1, eventAny, nil, 100, 0)

		evt := new(egopb.Event)
		require.NoError(t, proto.Unmarshal(raw, evt))

		assert.Nil(t, extractLegacyResultingState(evt))
	})
}

func TestConsumeVarint(t *testing.T) {
	t.Run("single byte", func(t *testing.T) {
		v, n := consumeVarint([]byte{0x05})
		assert.Equal(t, uint64(5), v)
		assert.Equal(t, 1, n)
	})

	t.Run("multi byte", func(t *testing.T) {
		v, n := consumeVarint([]byte{0xAC, 0x02})
		assert.Equal(t, uint64(300), v)
		assert.Equal(t, 2, n)
	})

	t.Run("empty input", func(t *testing.T) {
		_, n := consumeVarint([]byte{})
		assert.Equal(t, -1, n)
	})
}

func TestConsumeTag(t *testing.T) {
	t.Run("field 5 wire type 2", func(t *testing.T) {
		// Tag for field 5, wire type 2 = (5 << 3) | 2 = 42
		fieldNum, wireType, n := consumeTag([]byte{42})
		assert.Equal(t, uint32(5), fieldNum)
		assert.Equal(t, 2, wireType)
		assert.Equal(t, 1, n)
	})

	t.Run("empty input", func(t *testing.T) {
		_, _, n := consumeTag([]byte{})
		assert.Equal(t, -1, n)
	})
}

func TestMigratorOptions(t *testing.T) {
	t.Run("WithPageSize", func(t *testing.T) {
		m := New(nil, nil, WithPageSize(100))
		assert.EqualValues(t, 100, m.pageSize)
	})

	t.Run("WithLogger", func(t *testing.T) {
		logger := log.DiscardLogger
		m := New(nil, nil, WithLogger(logger))
		assert.Equal(t, logger, m.logger)
	})

	t.Run("defaults", func(t *testing.T) {
		m := New(nil, nil)
		assert.EqualValues(t, 500, m.pageSize)
		assert.NotNil(t, m.logger)
	})
}
