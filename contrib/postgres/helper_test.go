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

package postgres

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/tochemey/ego/v5/egopb"
	"github.com/tochemey/ego/v5/persistence"
	"github.com/tochemey/ego/v5/test/data/testpb"
)

const (
	postgresImage    = "postgres:16-alpine"
	postgresPort     = "5432/tcp"
	testDatabase     = "ego"
	testUser         = "ego"
	testPassword     = "ego"
	testSchema       = "public"
	startupTimeout   = 2 * time.Minute
	schemasPattern   = "resources/*.sql"
	truncateTables   = "TRUNCATE events_store, snapshots_store, states_store, offsets_store, offsets_start"
	readyLogLine     = "database system is ready to accept connections"
	readyLogRepeated = 2
)

var (
	// testPool is the pool every integration test runs on.
	testPool *pgxpool.Pool
	// testDSN is the connection string of the test database.
	testDSN string
)

// TestMain starts one PostgreSQL container for the whole package, creates the
// tables of the resources folder and runs the tests on one shared pool.
func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

// runTests runs the tests and returns their exit code, releasing the container
// and the pool on the way out.
func runTests(m *testing.M) int {
	ctx := context.Background()

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        postgresImage,
			ExposedPorts: []string{postgresPort},
			Env: map[string]string{
				"POSTGRES_DB":       testDatabase,
				"POSTGRES_USER":     testUser,
				"POSTGRES_PASSWORD": testPassword,
			},
			WaitingFor: wait.ForAll(
				wait.ForListeningPort(postgresPort),
				wait.ForLog(readyLogLine).WithOccurrence(readyLogRepeated),
			).WithDeadline(startupTimeout),
		},
		Started: true,
	})
	if err != nil {
		log.Printf("failed to start postgres: %v", err)
		return 1
	}

	defer func() {
		_ = container.Terminate(ctx)
	}()

	host, err := container.Host(ctx)
	if err != nil {
		log.Printf("failed to read the container host: %v", err)
		return 1
	}

	port, err := container.MappedPort(ctx, postgresPort)
	if err != nil {
		log.Printf("failed to read the container port: %v", err)
		return 1
	}

	testDSN = fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable&search_path=%s",
		testUser, testPassword, net.JoinHostPort(host, port.Port()), testDatabase, testSchema)

	testPool, err = pgxpool.New(ctx, testDSN)
	if err != nil {
		log.Printf("failed to open the pool: %v", err)
		return 1
	}

	defer testPool.Close()

	if err := createTables(ctx, testPool); err != nil {
		log.Printf("failed to create the tables: %v", err)
		return 1
	}

	return m.Run()
}

// createTables runs every schema of the resources folder.
func createTables(ctx context.Context, pool *pgxpool.Pool) error {
	schemas, err := filepath.Glob(schemasPattern)
	if err != nil {
		return err
	}

	for _, schema := range schemas {
		statements, err := os.ReadFile(schema)
		if err != nil {
			return err
		}

		if _, err := pool.Exec(ctx, string(statements)); err != nil {
			return fmt.Errorf("%s: %w", schema, err)
		}
	}

	return nil
}

// resetTables empties every table, so each test starts from an empty database.
func resetTables(tb testing.TB) {
	tb.Helper()
	_, err := testPool.Exec(context.Background(), truncateTables)
	require.NoError(tb, err)
}

// closedPool returns a pool on the test database that is closed already, so
// every statement run on it fails.
func closedPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), testDSN)
	require.NoError(t, err)
	pool.Close()
	return pool
}

// insertRow runs a statement on the test database, to store a row the stores
// would never write.
func insertRow(t *testing.T, statement string, arguments ...any) {
	t.Helper()
	_, err := testPool.Exec(context.Background(), statement, arguments...)
	require.NoError(t, err)
}

// newEvent returns an event of the persistence ID on its shard.
func newEvent(tb testing.TB, persistenceID string, sequenceNumber uint64, timestamp int64) *egopb.Event {
	tb.Helper()
	payload, err := anypb.New(&testpb.AccountCreated{AccountId: persistenceID, AccountBalance: float64(sequenceNumber)})
	require.NoError(tb, err)
	return &egopb.Event{
		PersistenceId:  persistenceID,
		SequenceNumber: sequenceNumber,
		Event:          payload,
		Timestamp:      timestamp,
		Shard:          persistence.ShardFor(persistenceID),
	}
}

// newState returns an account state message wrapped in an Any.
func newState(tb testing.TB, accountID string, balance float64) *anypb.Any {
	tb.Helper()
	state, err := anypb.New(&testpb.Account{AccountId: accountID, AccountBalance: balance})
	require.NoError(tb, err)
	return state
}
