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

package migrate

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/tochemey/ego/v5/egopb"
	"github.com/tochemey/ego/v5/persistence"
	testpb "github.com/tochemey/ego/v5/test/data/testpb"
	"github.com/tochemey/ego/v5/testkit"
)

const (
	// legacyShard is the shard every legacy test event is on.
	legacyShard = uint64(9)
	// legacyResultingStateField is the field number legacy events stored their
	// resulting state in.
	legacyResultingStateField = 5
	// testSource is the source location the tests pass on the command line.
	testSource = "postgres://legacy"
	// testTarget is the target location the tests pass on the command line.
	testTarget = "postgres://current"
)

// staticOffsets is a migration.LegacyOffsetReader over fixed offsets.
type staticOffsets struct {
	// offsets holds the legacy offsets of every projection.
	offsets map[string][]int64
	// err is returned instead of the offsets when set.
	err error
}

// LegacyOffsets returns the fixed offsets, or the configured error.
func (x staticOffsets) LegacyOffsets(context.Context) (map[string][]int64, error) {
	return x.offsets, x.err
}

// fixture holds the stores a test opener hands to the command and what the
// opener saw.
type fixture struct {
	// stores is what the opener returns.
	stores *Stores
	// config is the configuration the opener received.
	config Config
	// released reports whether the command released the stores.
	released bool
	// releaseCtx is the context the command released the stores with.
	releaseCtx context.Context
	// openErr is returned by the opener when set.
	openErr error
	// releaseErr is returned by the release when set.
	releaseErr error
}

// newFixture builds a fixture with connected in-memory stores. The source
// journal holds two events of account-1, and the legacy snapshot store a
// snapshot of it.
func newFixture(t *testing.T) *fixture {
	t.Helper()
	ctx := context.Background()

	sourceJournal := testkit.NewEventsStore()
	targetJournal := testkit.NewEventsStore()
	sourceSnapshots := testkit.NewSnapshotStore()
	targetSnapshots := testkit.NewSnapshotStore()
	targetOffsets := testkit.NewOffsetStore()
	for _, store := range []interface{ Connect(context.Context) error }{sourceJournal, targetJournal, sourceSnapshots, targetSnapshots, targetOffsets} {
		require.NoError(t, store.Connect(ctx))
	}

	event, err := anypb.New(&testpb.AccountCredited{AccountId: "account-1"})
	require.NoError(t, err)
	require.NoError(t, sourceJournal.WriteEvents(ctx, []*egopb.Event{
		{PersistenceId: "account-1", SequenceNumber: 1, Event: event, Timestamp: 100, Shard: legacyShard},
		{PersistenceId: "account-1", SequenceNumber: 2, Event: event, Timestamp: 200, Shard: legacyShard},
	}))

	state, err := anypb.New(&testpb.Account{AccountId: "account-1", AccountBalance: 2})
	require.NoError(t, err)
	require.NoError(t, sourceSnapshots.WriteSnapshot(ctx, &egopb.Snapshot{PersistenceId: "account-1", SequenceNumber: 2, State: state}))

	return &fixture{
		stores: &Stores{
			SourceJournal:   sourceJournal,
			TargetJournal:   targetJournal,
			SourceSnapshots: sourceSnapshots,
			TargetSnapshots: targetSnapshots,
			LegacyOffsets:   staticOffsets{offsets: map[string][]int64{"balances": {300, 100}, "audit": {50}}},
			TargetOffsets:   targetOffsets,
		},
	}
}

// open is the Opener of the fixture.
func (x *fixture) open(_ context.Context, config Config) (*Stores, Release, error) {
	x.config = config
	if x.openErr != nil {
		return nil, nil, x.openErr
	}

	return x.stores, func(ctx context.Context) error {
		x.released = true
		x.releaseCtx = ctx
		return x.releaseErr
	}, nil
}

// run executes the command with the given arguments and returns its standard output.
func (x *fixture) run(t *testing.T, args ...string) (string, error) {
	t.Helper()
	return x.runContext(t, context.Background(), args...)
}

// runContext executes the command with the given context and arguments and
// returns its standard output.
func (x *fixture) runContext(t *testing.T, ctx context.Context, args ...string) (string, error) {
	t.Helper()

	var stdout, stderr bytes.Buffer
	cmd := NewCommand(x.open)
	cmd.SetArgs(args)
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)

	err := cmd.ExecuteContext(ctx)
	return stdout.String(), err
}

// targetEvents returns the events of account-1 in the target journal.
func (x *fixture) targetEvents(t *testing.T) []*egopb.Event {
	t.Helper()

	events, err := x.stores.TargetJournal.ReplayEvents(context.Background(), "account-1", 1, 10, 10)
	require.NoError(t, err)
	return events
}

func TestJournalCommand(t *testing.T) {
	ctx := context.Background()

	t.Run("copies the journal and the snapshots", func(t *testing.T) {
		fixture := newFixture(t)

		out, err := fixture.run(t, "journal", "--source", testSource, "--target", testTarget, "--parallelism", "2", "--page-size", "1")
		require.NoError(t, err)

		assert.Contains(t, out, "journal and snapshots migrated")
		assert.Equal(t, Config{Source: testSource, Target: testTarget}, fixture.config)
		assert.True(t, fixture.released)

		events := fixture.targetEvents(t)
		require.Len(t, events, 2)
		assert.Equal(t, persistence.ShardFor("account-1"), events[0].GetShard())

		snapshot, err := fixture.stores.TargetSnapshots.GetLatestSnapshot(ctx, "account-1")
		require.NoError(t, err)
		require.NotNil(t, snapshot)
		assert.EqualValues(t, 2, snapshot.GetSequenceNumber())
	})

	t.Run("skips the snapshots without snapshot stores", func(t *testing.T) {
		fixture := newFixture(t)
		fixture.stores.SourceSnapshots = nil

		out, err := fixture.run(t, "journal")
		require.NoError(t, err)
		assert.Contains(t, out, "snapshots skipped")
		assert.Len(t, fixture.targetEvents(t), 2)
	})

	t.Run("requires the source journal", func(t *testing.T) {
		fixture := newFixture(t)
		fixture.stores.SourceJournal = nil

		_, err := fixture.run(t, "journal")
		require.ErrorIs(t, err, ErrSourceJournalRequired)
		assert.True(t, fixture.released, "the stores are released when the migration fails")
	})

	t.Run("requires the target journal", func(t *testing.T) {
		fixture := newFixture(t)
		fixture.stores.TargetJournal = nil

		_, err := fixture.run(t, "journal")
		require.ErrorIs(t, err, ErrTargetJournalRequired)
	})

	t.Run("reports a failed journal migration", func(t *testing.T) {
		fixture := newFixture(t)
		fixture.stores.SourceJournal = failingJournal{}

		_, err := fixture.run(t, "journal")
		require.ErrorIs(t, err, errStoreDown)
	})

	t.Run("reports a failed snapshot migration", func(t *testing.T) {
		fixture := newFixture(t)
		fixture.stores.SourceSnapshots = failingSnapshots{}

		_, err := fixture.run(t, "journal")
		require.ErrorIs(t, err, errStoreDown)
	})
}

func TestInterruptedCommand(t *testing.T) {
	t.Run("reports the interruption and releases the stores with a live context", func(t *testing.T) {
		fixture := newFixture(t)
		interrupted, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := fixture.runContext(t, interrupted, "journal")
		require.ErrorIs(t, err, context.Canceled)
		require.True(t, fixture.released)
		assert.NoError(t, fixture.releaseCtx.Err(), "the stores are released with a context that is not cancelled")
	})
}

func TestOffsetsCommand(t *testing.T) {
	ctx := context.Background()

	t.Run("resumes every projection from its oldest legacy offset", func(t *testing.T) {
		fixture := newFixture(t)

		out, err := fixture.run(t, "offsets")
		require.NoError(t, err)
		assert.Contains(t, out, "2 projections resumed")

		start, err := fixture.stores.TargetOffsets.StartOffset(ctx, "balances")
		require.NoError(t, err)
		assert.EqualValues(t, 100, start)
	})

	t.Run("requires the legacy offsets", func(t *testing.T) {
		fixture := newFixture(t)
		fixture.stores.LegacyOffsets = nil

		_, err := fixture.run(t, "offsets")
		require.ErrorIs(t, err, ErrLegacyOffsetsRequired)
	})

	t.Run("requires the target offset store", func(t *testing.T) {
		fixture := newFixture(t)
		fixture.stores.TargetOffsets = nil

		_, err := fixture.run(t, "offsets")
		require.ErrorIs(t, err, ErrTargetOffsetsRequired)
	})

	t.Run("reports legacy offsets that cannot be read", func(t *testing.T) {
		fixture := newFixture(t)
		fixture.stores.LegacyOffsets = staticOffsets{err: errStoreDown}

		_, err := fixture.run(t, "offsets")
		require.ErrorIs(t, err, errStoreDown)
	})
}

func TestLegacySnapshotsCommand(t *testing.T) {
	ctx := context.Background()

	t.Run("writes a snapshot from a legacy event's resulting state", func(t *testing.T) {
		fixture := newFixture(t)
		writeLegacyEvent(t, fixture.stores.SourceJournal.(*testkit.EventStore), "account-2", &testpb.Account{AccountId: "account-2", AccountBalance: 7})

		out, err := fixture.run(t, "legacy-snapshots")
		require.NoError(t, err)
		assert.Contains(t, out, "legacy snapshots migrated")

		snapshot, err := fixture.stores.TargetSnapshots.GetLatestSnapshot(ctx, "account-2")
		require.NoError(t, err)
		require.NotNil(t, snapshot)
		assert.EqualValues(t, 1, snapshot.GetSequenceNumber())
	})

	t.Run("requires the source journal", func(t *testing.T) {
		fixture := newFixture(t)
		fixture.stores.SourceJournal = nil

		_, err := fixture.run(t, "legacy-snapshots")
		require.ErrorIs(t, err, ErrSourceJournalRequired)
	})

	t.Run("requires the target snapshot store", func(t *testing.T) {
		fixture := newFixture(t)
		fixture.stores.TargetSnapshots = nil

		_, err := fixture.run(t, "legacy-snapshots")
		require.ErrorIs(t, err, ErrTargetSnapshotsRequired)
	})

	t.Run("reports a failed migration", func(t *testing.T) {
		fixture := newFixture(t)
		fixture.stores.SourceJournal = failingJournal{}

		_, err := fixture.run(t, "legacy-snapshots")
		require.ErrorIs(t, err, errStoreDown)
	})
}

func TestAllCommand(t *testing.T) {
	ctx := context.Background()

	t.Run("migrates the journal, then the offsets", func(t *testing.T) {
		fixture := newFixture(t)

		out, err := fixture.run(t, "all")
		require.NoError(t, err)
		assert.Contains(t, out, "journal and snapshots migrated")
		assert.Contains(t, out, "2 projections resumed")
		assert.Len(t, fixture.targetEvents(t), 2)

		start, err := fixture.stores.TargetOffsets.StartOffset(ctx, "audit")
		require.NoError(t, err)
		assert.EqualValues(t, 50, start)
	})

	t.Run("stops before the offsets when the journal fails", func(t *testing.T) {
		fixture := newFixture(t)
		fixture.stores.TargetJournal = nil

		out, err := fixture.run(t, "all")
		require.ErrorIs(t, err, ErrTargetJournalRequired)
		assert.NotContains(t, out, "projections resumed")
	})
}

func TestCommandFlagsAndStores(t *testing.T) {
	t.Run("rejects a parallelism below one", func(t *testing.T) {
		fixture := newFixture(t)

		_, err := fixture.run(t, "journal", "--parallelism", "0")
		require.ErrorContains(t, err, "--parallelism must be at least 1")
		assert.False(t, fixture.released, "no store is opened")
	})

	t.Run("rejects a page size below one", func(t *testing.T) {
		fixture := newFixture(t)

		_, err := fixture.run(t, "journal", "--page-size", "0")
		require.ErrorContains(t, err, "--page-size must be at least 1")
	})

	t.Run("rejects arguments", func(t *testing.T) {
		fixture := newFixture(t)

		_, err := fixture.run(t, "journal", "unexpected")
		require.Error(t, err)
	})

	t.Run("reports stores that cannot be opened", func(t *testing.T) {
		fixture := newFixture(t)
		fixture.openErr = errStoreDown

		_, err := fixture.run(t, "journal")
		require.ErrorIs(t, err, errStoreDown)
		assert.ErrorContains(t, err, "failed to open the stores")
	})

	t.Run("reports stores that cannot be released", func(t *testing.T) {
		fixture := newFixture(t)
		fixture.releaseErr = errStoreDown

		_, err := fixture.run(t, "journal")
		require.ErrorIs(t, err, errStoreDown)
		assert.ErrorContains(t, err, "failed to release the stores")
	})
}

// errStoreDown is the error the failing test stores return.
var errStoreDown = errors.New("store down")

// failingJournal is a migration.JournalReader whose every call fails.
type failingJournal struct{}

// Ping fails.
func (failingJournal) Ping(context.Context) error { return errStoreDown }

// PersistenceIDs fails.
func (failingJournal) PersistenceIDs(context.Context, uint64, string) ([]string, string, error) {
	return nil, "", errStoreDown
}

// GetLatestEvent fails.
func (failingJournal) GetLatestEvent(context.Context, string) (*egopb.Event, error) {
	return nil, errStoreDown
}

// ReplayEvents fails.
func (failingJournal) ReplayEvents(context.Context, string, uint64, uint64, uint64) ([]*egopb.Event, error) {
	return nil, errStoreDown
}

// failingSnapshots is a migration.SnapshotReader whose every call fails.
type failingSnapshots struct{}

// Ping fails.
func (failingSnapshots) Ping(context.Context) error { return errStoreDown }

// PersistenceIDs fails.
func (failingSnapshots) PersistenceIDs(context.Context, uint64, string) ([]string, string, error) {
	return nil, "", errStoreDown
}

// GetLatestSnapshot fails.
func (failingSnapshots) GetLatestSnapshot(context.Context, string) (*egopb.Snapshot, error) {
	return nil, errStoreDown
}

// writeLegacyEvent writes the first event of the persistence ID the way
// releases that stored the resulting state in every event did.
func writeLegacyEvent(t *testing.T, store *testkit.EventStore, persistenceID string, state proto.Message) {
	t.Helper()

	event, err := anypb.New(&testpb.AccountCreated{AccountId: persistenceID})
	require.NoError(t, err)
	encoded, err := proto.Marshal(&egopb.Event{PersistenceId: persistenceID, SequenceNumber: 1, Event: event, Timestamp: 100, Shard: legacyShard})
	require.NoError(t, err)

	stateAny, err := anypb.New(state)
	require.NoError(t, err)
	stateBytes, err := proto.Marshal(stateAny)
	require.NoError(t, err)

	encoded = protowire.AppendTag(encoded, legacyResultingStateField, protowire.BytesType)
	encoded = protowire.AppendBytes(encoded, stateBytes)

	legacy := new(egopb.Event)
	require.NoError(t, proto.Unmarshal(encoded, legacy))
	require.NoError(t, store.WriteEvents(context.Background(), []*egopb.Event{legacy}))
}
