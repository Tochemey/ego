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
	"fmt"

	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Postgres will be implemented by concrete RDBMS store
type Postgres interface {
	// Connect connects to the underlying database
	Connect(ctx context.Context) error
	// Disconnect closes the underlying opened underlying connection database
	Disconnect(ctx context.Context) error
	// Select fetches a single row from the database and automatically scanned it into the dst.
	// It returns an error in case of failure. When there is no record no errors is return.
	Select(ctx context.Context, dst any, query string, args ...any) error
	// SelectAll fetches a set of rows as defined by the query and scanned those record in the dst.
	// It returns nil when there is no records to fetch.
	SelectAll(ctx context.Context, dst any, query string, args ...any) error
	// Exec executes an SQL statement against the database and returns the appropriate result or an error.
	Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error)
	// BeginTx helps start an SQL transaction. The return transaction object is expected to be used in
	// the subsequent queries following the BeginTx.
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
}

// Postgres helps interact with the Postgres database
type postgres struct {
	connStr string
	pool    *pgxpool.Pool
	config  *Config
}

var _ Postgres = (*postgres)(nil)

const postgresDriver = "postgres"
const instrumentationName = "ego.events_store"

// New returns a store connecting to the given Postgres database.
func New(config *Config) Postgres {
	postgres := new(postgres)
	postgres.config = config
	postgres.connStr = createConnectionString(config.DBHost, config.DBPort, config.DBName, config.DBUser, config.DBPassword, config.DBSchema)
	return postgres
}

// Connect will connect to our Postgres database
func (pg *postgres) Connect(ctx context.Context) error {
	// create the connection config
	config, err := pgxpool.ParseConfig(pg.connStr)
	if err != nil {
		return fmt.Errorf("failed to parse connection string: %w", err)
	}

	// amend some of the configuration
	config.MaxConns = int32(pg.config.MaxConnections)
	config.MaxConnLifetime = pg.config.MaxConnectionLifetime
	config.MaxConnIdleTime = pg.config.MaxConnIdleTime
	config.MinConns = int32(pg.config.MinConnections)
	config.HealthCheckPeriod = pg.config.HealthCheckPeriod

	// connect to the pool
	dbpool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return fmt.Errorf("failed to create the connection pool: %w", err)
	}

	// let us test the connection
	if err := dbpool.Ping(ctx); err != nil {
		return fmt.Errorf("failed to ping the database connection: %w", err)
	}

	// set the db handle
	pg.pool = dbpool
	return nil
}

// createConnectionString will create the Postgres connection string from the
// supplied connection details
// TODO: enhance this with the SSL settings
func createConnectionString(host string, port int, name, user string, password string, schema string) string {
	info := fmt.Sprintf("host=%s port=%d user=%s dbname=%s sslmode=disable", host, port, user, name)
	// The Postgres driver gets confused in cases where the user has no password
	// set but a password is passed, so only set password if its non-empty
	if password != "" {
		info += fmt.Sprintf(" password=%s", password)
	}

	if schema != "" {
		info += fmt.Sprintf(" search_path=%s", schema)
	}

	return info
}

// Exec executes a sql query without returning rows against the database
func (pg *postgres) Exec(ctx context.Context, query string, args ...interface{}) (pgconn.CommandTag, error) {
	return pg.pool.Exec(ctx, query, args...)
}

// BeginTx starts a new database transaction
func (pg *postgres) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	return pg.pool.BeginTx(ctx, txOptions)
}

// SelectAll fetches rows
func (pg *postgres) SelectAll(ctx context.Context, dst interface{}, query string, args ...interface{}) error {
	err := pgxscan.Select(ctx, pg.pool, dst, query, args...)
	if err != nil {
		if pgxscan.NotFound(err) {
			return nil
		}

		return err
	}
	return nil
}

// Select fetches only one row
func (pg *postgres) Select(ctx context.Context, dst interface{}, query string, args ...interface{}) error {
	err := pgxscan.Get(ctx, pg.pool, dst, query, args...)
	if err != nil {
		if pgxscan.NotFound(err) {
			return nil
		}
		return err
	}

	return nil
}

// Disconnect the database connection.
// nolint
func (pg *postgres) Disconnect(ctx context.Context) error {
	if pg.pool == nil {
		return nil
	}
	pg.pool.Close()
	return nil
}
