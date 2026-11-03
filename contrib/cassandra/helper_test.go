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
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/stretchr/testify/require"
	tccassandra "github.com/testcontainers/testcontainers-go/modules/cassandra"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/tochemey/ego/v5/egopb"
	"github.com/tochemey/ego/v5/persistence"
)

const (
	// cassandraImage is the Cassandra image the tests run against.
	cassandraImage = "cassandra:5.0.6"
	// testKeyspace is the keyspace that holds the tables of the module's schemas in the tests.
	testKeyspace = "ego_test"
	// schemaFiles matches the schemas the tests apply, the ones the README tells users to apply.
	schemaFiles = "resources/*.cql"
	// sessionTimeout bounds every query of the test sessions; a fresh container answers slowly.
	sessionTimeout = 30 * time.Second
)

// testHost is the address of the Cassandra container shared by the tests.
var testHost string

// commentLine matches a comment line of a schema file.
var commentLine = regexp.MustCompile(`(?m)^--.*$`)

// TestMain starts one Cassandra container for the package, creates the
// keyspace and the schemas, runs the tests and removes the container.
func TestMain(m *testing.M) {
	ctx := context.Background()

	container, err := tccassandra.Run(ctx, cassandraImage)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start cassandra: %v\n", err)
		os.Exit(1)
	}

	testHost, err = container.ConnectionHost(ctx)
	if err == nil {
		err = createSchemas()
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

// newSession opens a session on the test container, closed when the test ends.
func newSession(t *testing.T, keyspace string) *gocql.Session {
	t.Helper()

	session, err := openSession(keyspace)
	require.NoError(t, err)
	t.Cleanup(session.Close)
	return session
}

// newClosedSession opens a session on the test keyspace and closes it, so every
// statement run on it fails.
func newClosedSession(t *testing.T) *gocql.Session {
	t.Helper()

	session, err := openSession(testKeyspace)
	require.NoError(t, err)
	session.Close()
	return session
}

// truncate empties the given tables of the test keyspace.
func truncate(t *testing.T, session *gocql.Session, tables ...string) {
	t.Helper()

	for _, table := range tables {
		require.NoError(t, session.Query("TRUNCATE "+table).ExecContext(t.Context()))
	}
}

// newKeyspace creates a keyspace with the given CREATE TABLE statements only,
// and returns a session on it closed when the test ends.
func newKeyspace(t *testing.T, keyspace string, tables ...string) *gocql.Session {
	t.Helper()

	admin := newSession(t, "")
	require.NoError(t, admin.Query(createKeyspaceStatement(keyspace)).ExecContext(t.Context()))

	session := newSession(t, keyspace)
	for _, table := range tables {
		require.NoError(t, session.Query(table).ExecContext(t.Context()))
	}

	return session
}

// newPayload packs a string value into an Any.
func newPayload(t *testing.T, value string) *anypb.Any {
	t.Helper()

	payload, err := anypb.New(wrapperspb.String(value))
	require.NoError(t, err)
	return payload
}

// newDurableState builds a durable state of the persistence ID whose resulting
// state is a string value.
func newDurableState(t *testing.T, persistenceID string, version uint64, value string) *egopb.DurableState {
	t.Helper()

	return &egopb.DurableState{
		PersistenceId:  persistenceID,
		VersionNumber:  version,
		ResultingState: newPayload(t, value),
		Timestamp:      time.Now().UnixNano(),
		Shard:          persistence.ShardFor(persistenceID),
	}
}

// newOffset builds the offset of a persistence ID's event.
func newOffset(shardNumber uint64, persistenceID string, sequenceNumber uint64, timestamp, consumedAt int64) *egopb.EntityOffset {
	return &egopb.EntityOffset{
		ShardNumber:    shardNumber,
		PersistenceId:  persistenceID,
		SequenceNumber: sequenceNumber,
		Timestamp:      timestamp,
		ConsumedAt:     consumedAt,
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

// createKeyspaceStatement creates a keyspace of a single replica.
func createKeyspaceStatement(keyspace string) string {
	return fmt.Sprintf(`CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}`, keyspace)
}

// createSchemas creates the test keyspace and applies every schema file to it,
// one statement at a time.
func createSchemas() error {
	admin, err := openSession("")
	if err != nil {
		return err
	}
	defer admin.Close()

	if err := admin.Query(createKeyspaceStatement(testKeyspace)).Exec(); err != nil {
		return err
	}

	session, err := openSession(testKeyspace)
	if err != nil {
		return err
	}
	defer session.Close()

	files, err := filepath.Glob(schemaFiles)
	if err != nil {
		return err
	}

	for _, file := range files {
		schema, err := os.ReadFile(file)
		if err != nil {
			return err
		}

		for _, statement := range strings.Split(commentLine.ReplaceAllString(string(schema), ""), ";") {
			if statement = strings.TrimSpace(statement); statement == "" {
				continue
			}

			if err := session.Query(statement).Exec(); err != nil {
				return fmt.Errorf("%s: %w", file, err)
			}
		}
	}

	return nil
}
