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

package cassandra

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tccassandra "github.com/testcontainers/testcontainers-go/modules/cassandra"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/tochemey/ego/v5/egopb"
	"github.com/tochemey/ego/v5/persistence"
)

const (
	// cassandraImage is the Cassandra image the tests run against.
	cassandraImage = "cassandra:5.0.6"
	// testKeyspace is the keyspace that holds the states_store table in the tests.
	testKeyspace = "ego_test"
	// schemaFile is the schema the tests apply, the one the README tells users to apply.
	schemaFile = "resources/durablestore_cassandra.cql"
	// sessionTimeout bounds every query of the test sessions; a fresh container answers slowly.
	sessionTimeout = 30 * time.Second
)

// testHost is the address of the Cassandra container shared by the tests.
var testHost string

// TestMain starts one Cassandra container for the package, creates the
// keyspace and the schema, runs the tests and removes the container.
func TestMain(m *testing.M) {
	ctx := context.Background()

	container, err := tccassandra.Run(ctx, cassandraImage)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start cassandra: %v\n", err)
		os.Exit(1)
	}

	testHost, err = container.ConnectionHost(ctx)
	if err == nil {
		err = createSchema()
	}

	code := 1
	if err == nil {
		code = m.Run()
	} else {
		fmt.Fprintf(os.Stderr, "failed to prepare cassandra: %v\n", err)
	}

	_ = container.Terminate(ctx)
	os.Exit(code)
}

func TestDurableStore(t *testing.T) {
	ctx := context.Background()
	session := newSession(t, testKeyspace)
	store := NewDurableStore(session)

	require.NoError(t, store.Connect(ctx))
	require.NoError(t, store.Ping(ctx))

	t.Run("returns nil when no state is stored", func(t *testing.T) {
		state, err := store.GetLatestState(ctx, "missing")
		require.NoError(t, err)
		assert.Nil(t, state)
	})

	t.Run("writes and reads the latest state", func(t *testing.T) {
		persistenceID := "account-write"
		first := newDurableState(t, persistenceID, 1, "opened")
		second := newDurableState(t, persistenceID, 2, "credited")

		require.NoError(t, store.WriteState(ctx, first))
		require.NoError(t, store.WriteState(ctx, second))

		state, err := store.GetLatestState(ctx, persistenceID)
		require.NoError(t, err)
		assert.True(t, proto.Equal(second, state), "got %v", state)
	})

	t.Run("keeps a tombstone when deleting with a version", func(t *testing.T) {
		persistenceID := "account-tombstone"
		require.NoError(t, store.WriteState(ctx, newDurableState(t, persistenceID, 3, "credited")))

		require.NoError(t, store.DeleteState(ctx, persistenceID, 4))

		state, err := store.GetLatestState(ctx, persistenceID)
		require.NoError(t, err)
		require.NotNil(t, state)
		assert.Equal(t, uint64(4), state.GetVersionNumber())
		assert.Nil(t, state.GetResultingState())
		assert.Equal(t, persistence.ShardFor(persistenceID), state.GetShard())
		assert.Positive(t, state.GetTimestamp())
	})

	t.Run("removes the row when deleting with version zero", func(t *testing.T) {
		persistenceID := "account-delete"
		require.NoError(t, store.WriteState(ctx, newDurableState(t, persistenceID, 1, "opened")))

		require.NoError(t, store.DeleteState(ctx, persistenceID, 0))

		state, err := store.GetLatestState(ctx, persistenceID)
		require.NoError(t, err)
		assert.Nil(t, state)
	})

	t.Run("deleting a missing state is not an error", func(t *testing.T) {
		require.NoError(t, store.DeleteState(ctx, "never-written", 0))
	})

	t.Run("serves the application's own queries on the same session", func(t *testing.T) {
		var count int
		require.NoError(t, session.Query(`SELECT COUNT(*) FROM states_store`).ScanContext(ctx, &count))
		assert.Positive(t, count)
	})

	require.NoError(t, store.Disconnect(ctx))
	require.NoError(t, store.Ping(ctx), "Disconnect must leave the application's session open")
}

func TestDurableStoreErrors(t *testing.T) {
	ctx := context.Background()
	session := newSession(t, testKeyspace)
	session.Close()
	store := NewDurableStore(session)

	assert.Error(t, store.Connect(ctx))
	assert.Error(t, store.WriteState(ctx, newDurableState(t, "account", 1, "opened")))
	assert.Error(t, store.DeleteState(ctx, "account", 1))
	assert.Error(t, store.DeleteState(ctx, "account", 0))

	state, err := store.GetLatestState(ctx, "account")
	assert.Error(t, err)
	assert.Nil(t, state)
}

// newSession opens a session on the test container, closed when the test ends.
func newSession(t *testing.T, keyspace string) *gocql.Session {
	t.Helper()

	session, err := openSession(keyspace)
	require.NoError(t, err)
	t.Cleanup(session.Close)
	return session
}

// newDurableState builds a durable state of the persistence ID whose resulting
// state is a string value.
func newDurableState(t *testing.T, persistenceID string, version uint64, value string) *egopb.DurableState {
	t.Helper()

	resultingState, err := anypb.New(wrapperspb.String(value))
	require.NoError(t, err)

	return &egopb.DurableState{
		PersistenceId:  persistenceID,
		VersionNumber:  version,
		ResultingState: resultingState,
		Timestamp:      time.Now().UnixNano(),
		Shard:          persistence.ShardFor(persistenceID),
	}
}

// openSession opens a session on the test container, in the given keyspace
// when it is not empty.
func openSession(keyspace string) (*gocql.Session, error) {
	cluster := gocql.NewCluster(testHost)
	cluster.Keyspace = keyspace
	cluster.Consistency = gocql.One
	cluster.Timeout = sessionTimeout
	cluster.ConnectTimeout = sessionTimeout
	return cluster.CreateSession()
}

// createSchema creates the test keyspace and applies the schema file to it.
func createSchema() error {
	session, err := openSession("")
	if err != nil {
		return err
	}
	defer session.Close()

	createKeyspace := fmt.Sprintf(`CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}`, testKeyspace)
	if err := session.Query(createKeyspace).Exec(); err != nil {
		return err
	}

	schema, err := os.ReadFile(schemaFile)
	if err != nil {
		return err
	}

	statement := regexp.MustCompile(`(?m)^--.*$`).ReplaceAllString(string(schema), "")
	statement = strings.Replace(statement, "states_store", testKeyspace+".states_store", 1)
	return session.Query(strings.TrimSpace(statement)).Exec()
}
