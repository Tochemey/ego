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

package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
)

// account is a test struct
type account struct {
	AccountID   string
	AccountName string
}

// PostgresTestSuite will run the Postgres tests
type PostgresTestSuite struct {
	suite.Suite
	container *TestContainer
}

// SetupSuite starts the Postgres database engine and set the container
// host and port to use in the tests
func (s *PostgresTestSuite) SetupSuite() {
	s.container = NewTestContainer("testdb", "test", "test")
}

func (s *PostgresTestSuite) TearDownSuite() {
	s.container.Cleanup()
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestPostgresTestSuite(t *testing.T) {
	suite.Run(t, new(PostgresTestSuite))
}

func (s *PostgresTestSuite) TestConnect() {
	s.Run("with valid connection settings", func() {
		ctx := context.TODO()
		db := s.container.GetTestDB()

		err := db.Connect(ctx)
		s.Assert().NoError(err)
	})

	s.Run("with invalid database port", func() {
		ctx := context.TODO()
		db := New(&Config{
			DBUser:                "test",
			DBName:                "testdb",
			DBPassword:            "test",
			DBSchema:              s.container.Schema(),
			DBHost:                s.container.Host(),
			DBPort:                -2,
			MaxConnections:        4,
			MinConnections:        0,
			MaxConnectionLifetime: time.Hour,
			MaxConnIdleTime:       30 * time.Minute,
			HealthCheckPeriod:     time.Minute,
		})
		err := db.Connect(ctx)
		s.Assert().Error(err)
	})

	s.Run("with invalid database name", func() {
		ctx := context.TODO()
		db := New(&Config{
			DBUser:                "test",
			DBName:                "wrong-name",
			DBPassword:            "test",
			DBSchema:              s.container.Schema(),
			DBHost:                s.container.Host(),
			DBPort:                s.container.Port(),
			MaxConnections:        4,
			MinConnections:        0,
			MaxConnectionLifetime: time.Hour,
			MaxConnIdleTime:       30 * time.Minute,
			HealthCheckPeriod:     time.Minute,
		})
		err := db.Connect(ctx)
		s.Assert().Error(err)
	})

	s.Run("with invalid database user", func() {
		ctx := context.TODO()
		db := New(&Config{
			DBUser:                "test-user",
			DBName:                "testdb",
			DBPassword:            "test",
			DBSchema:              s.container.Schema(),
			DBHost:                s.container.Host(),
			DBPort:                s.container.Port(),
			MaxConnections:        4,
			MinConnections:        0,
			MaxConnectionLifetime: time.Hour,
			MaxConnIdleTime:       30 * time.Minute,
			HealthCheckPeriod:     time.Minute,
		})
		err := db.Connect(ctx)
		s.Assert().Error(err)
	})

	s.Run("with invalid database password", func() {
		ctx := context.TODO()
		db := New(&Config{
			DBUser:                "test",
			DBName:                "testdb",
			DBPassword:            "invalid-db-pass",
			DBSchema:              s.container.Schema(),
			DBHost:                s.container.Host(),
			DBPort:                s.container.Port(),
			MaxConnections:        4,
			MinConnections:        0,
			MaxConnectionLifetime: time.Hour,
			MaxConnIdleTime:       30 * time.Minute,
			HealthCheckPeriod:     time.Minute,
		})

		err := db.Connect(ctx)
		s.Assert().Error(err)
	})
}

func (s *PostgresTestSuite) TestExec() {
	ctx := context.TODO()
	db := s.container.GetTestDB()
	err := db.Connect(ctx)
	s.Assert().NoError(err)

	s.Run("with valid SQL statement", func() {
		// let us create a test table
		const schemaDDL = `
		CREATE TABLE accounts
		(
		    account_id		UUID,
			account_name 	VARCHAR(255)  NOT NULL,
		    PRIMARY KEY (account_id)
		);
	`
		_, err = db.Exec(ctx, schemaDDL)
		s.Assert().NoError(err)
	})

	s.Run("with invalid SQL statement", func() {
		const schemaDDL = `SOME-INVALID-SQL`
		_, err = db.Exec(ctx, schemaDDL)
		s.Assert().Error(err)
	})
}

func (s *PostgresTestSuite) TestSelect() {
	ctx := context.TODO()
	db := s.container.GetTestDB()
	err := db.Connect(ctx)
	s.Assert().NoError(err)

	const selectSQL = `SELECT account_id, account_name FROM accounts WHERE account_id = $1`

	s.Run("with valid record", func() {
		// first drop the table
		err = db.DropTable(ctx, "accounts")
		s.Assert().NoError(err)

		// create the database table
		err = createTable(ctx, db)
		s.Assert().NoError(err)

		// let us insert into that table
		inserted := &account{
			AccountID:   uuid.New().String(),
			AccountName: "some-account",
		}
		err = insertInto(ctx, db, inserted)
		s.Assert().NoError(err)

		// let us select the record inserted
		selected := &account{}
		err = db.Select(ctx, selected, selectSQL, inserted.AccountID)
		s.Assert().NoError(err)

		// let us compare the selected data and the record added
		s.Assert().Equal(inserted.AccountID, selected.AccountID)
		s.Assert().Equal(inserted.AccountName, selected.AccountName)
	})

	s.Run("with no records", func() {
		// first drop the table
		err = db.DropTable(ctx, "accounts")
		s.Assert().NoError(err)

		// create the database table
		err = createTable(ctx, db)
		s.Assert().NoError(err)

		var selected *account
		err = db.Select(ctx, selected, selectSQL, uuid.New().String())
		s.Assert().NoError(err)
		s.Assert().Nil(selected)
	})

	s.Run("with invalid SQL statement", func() {
		var selected *account
		err = db.Select(ctx, selected, "weird-sql", uuid.New().String())
		s.Assert().Error(err)
		s.Assert().Nil(selected)
	})
}

func (s *PostgresTestSuite) TestSelectAll() {
	ctx := context.TODO()
	db := s.container.GetTestDB()
	err := db.Connect(ctx)
	s.Assert().NoError(err)

	const selectSQL = `SELECT account_id, account_name FROM accounts;`

	s.Run("with valid records", func() {
		// first drop the table
		err = db.DropTable(ctx, "accounts")
		s.Assert().NoError(err)

		// create the database table
		err = createTable(ctx, db)
		s.Assert().NoError(err)

		// let us insert into that table
		inserted := &account{
			AccountID:   uuid.New().String(),
			AccountName: "some-account",
		}
		err = insertInto(ctx, db, inserted)
		s.Assert().NoError(err)

		// let us select the record inserted
		var selected []*account
		err = db.SelectAll(ctx, &selected, selectSQL)
		s.Assert().NoError(err)
		s.Assert().Equal(1, len(selected))
	})

	s.Run("with no records", func() {
		// first drop the table
		err = db.DropTable(ctx, "accounts")
		s.Assert().NoError(err)

		// create the database table
		err = createTable(ctx, db)
		s.Assert().NoError(err)

		var selected []*account
		err = db.SelectAll(ctx, &selected, selectSQL)
		s.Assert().NoError(err)
		s.Assert().Nil(selected)
	})

	s.Run("with invalid SQL statement", func() {
		var selected []*account
		err = db.SelectAll(ctx, selected, "weird-sql", uuid.New().String())
		s.Assert().Error(err)
		s.Assert().Nil(selected)
	})
}

func (s *PostgresTestSuite) TestClose() {
	ctx := context.TODO()
	db := s.container.GetTestDB()
	err := db.Connect(ctx)
	s.Assert().NoError(err)

	// close the db connection
	err = db.Disconnect(ctx)
	s.Assert().NoError(err)

	// let us execute a query against a closed connection
	err = db.TableExists(ctx, "accounts")
	s.Assert().Error(err)
	s.Assert().EqualError(err, "closed pool")
}

func createTable(ctx context.Context, db Postgres) error {
	// let us create a test table
	const schemaDDL = `
		CREATE TABLE IF NOT EXISTS accounts
		(
		    account_id		UUID,
			account_name 	VARCHAR(255)  NOT NULL,
		    PRIMARY KEY (account_id)
		);	
	`
	_, err := db.Exec(ctx, schemaDDL)
	return err
}

func insertInto(ctx context.Context, db Postgres, account *account) error {
	const insertSQL = `INSERT INTO accounts(account_id, account_name) VALUES($1, $2);`
	_, err := db.Exec(ctx, insertSQL, account.AccountID, account.AccountName)
	return err
}
