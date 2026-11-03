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

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	// register the MySQL driver with database/sql
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	tcmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/tochemey/ego/v5/egopb"
	"github.com/tochemey/ego/v5/test/data/testpb"
)

const (
	// mysqlImage is the MySQL image the tests run against.
	mysqlImage = "mysql:8.4"
	// driverName is the name github.com/go-sql-driver/mysql registers with database/sql.
	driverName = "mysql"
	// interpolateParams makes the driver send a statement with its arguments in one round trip.
	interpolateParams = "interpolateParams=true"
)

// schemaFiles are the scripts of the resources directory the test database is created with.
var schemaFiles = []string{
	"eventstore_mysql.sql",
	"snapshotstore_mysql.sql",
	"durablestore_mysql.sql",
	"offsetstore_mysql.sql",
}

// tables are the tables resetTables empties between tests.
var tables = []string{"events_store", "snapshots_store", "states_store", "offsets_store", "offsets_start"}

var (
	// testDSN is the connection string of the MySQL container shared by the tests.
	testDSN string
	// testDB is the handle every test runs on.
	testDB *sql.DB
)

// TestMain starts one MySQL container for the package, creates the schema, runs the tests and
// removes the container.
func TestMain(m *testing.M) {
	ctx := context.Background()

	scripts := make([]string, 0, len(schemaFiles))
	for _, file := range schemaFiles {
		scripts = append(scripts, filepath.Join("resources", file))
	}

	container, err := tcmysql.Run(ctx, mysqlImage, tcmysql.WithScripts(scripts...))
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start mysql: %v\n", err)
		os.Exit(1)
	}

	testDSN, err = container.ConnectionString(ctx, interpolateParams)
	if err == nil {
		testDB, err = sql.Open(driverName, testDSN)
	}

	if err == nil {
		err = testDB.PingContext(ctx)
	}

	code := 1
	if err == nil {
		code = m.Run()
		_ = testDB.Close()
	} else {
		fmt.Fprintf(os.Stderr, "failed to prepare mysql: %v\n", err)
	}

	_ = container.Terminate(ctx)
	os.Exit(code)
}

// resetTables empties every table, so a test starts from empty storage.
func resetTables(tb testing.TB) {
	tb.Helper()

	for _, table := range tables {
		_, err := testDB.ExecContext(context.Background(), "TRUNCATE TABLE "+table)
		require.NoError(tb, err)
	}
}

// newClosedDB returns a closed handle on the test database, on which every statement fails.
func newClosedDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open(driverName, testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	return db
}

// newPayload packs an account with the given ID into an Any.
func newPayload(t testing.TB, accountID string) *anypb.Any {
	t.Helper()

	payload, err := anypb.New(&testpb.Account{AccountId: accountID, AccountBalance: 100})
	require.NoError(t, err)
	return payload
}

// newEvent creates an event of the persistence ID with the given sequence number, shard and timestamp.
func newEvent(t testing.TB, persistenceID string, sequenceNumber, shard uint64, timestamp int64) *egopb.Event {
	t.Helper()

	return &egopb.Event{
		PersistenceId:  persistenceID,
		SequenceNumber: sequenceNumber,
		Event:          newPayload(t, persistenceID),
		Timestamp:      timestamp,
		Shard:          shard,
	}
}

// manyEvents creates count events of the persistence ID, sequence numbers 1 to count.
func manyEvents(t testing.TB, persistenceID string, count int) []*egopb.Event {
	t.Helper()

	events := make([]*egopb.Event, 0, count)
	for index := range count {
		sequenceNumber := uint64(index) + 1
		events = append(events, newEvent(t, persistenceID, sequenceNumber, 1, int64(sequenceNumber)))
	}

	return events
}

// newOffset creates the offset of the persistence ID's event with the given sequence number.
func newOffset(shard uint64, persistenceID string, sequenceNumber uint64, timestamp, consumedAt int64) *egopb.EntityOffset {
	return &egopb.EntityOffset{
		ShardNumber:    shard,
		PersistenceId:  persistenceID,
		SequenceNumber: sequenceNumber,
		Timestamp:      timestamp,
		ConsumedAt:     consumedAt,
	}
}

// explain returns the Extra column of every row of the plan MySQL chooses for query.
func explain(t *testing.T, query string, args ...any) []string {
	t.Helper()

	rows, err := testDB.QueryContext(context.Background(), "EXPLAIN "+query, args...)
	require.NoError(t, err)
	defer rows.Close()

	columns, err := rows.Columns()
	require.NoError(t, err)

	var extras []string
	for rows.Next() {
		values := make([]sql.NullString, len(columns))
		pointers := make([]any, len(columns))
		for index := range values {
			pointers[index] = &values[index]
		}

		require.NoError(t, rows.Scan(pointers...))
		for index, column := range columns {
			if column == "Extra" {
				extras = append(extras, values[index].String)
			}
		}
	}

	require.NoError(t, rows.Err())
	return extras
}
