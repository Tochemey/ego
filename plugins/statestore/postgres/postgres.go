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
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/golang/protobuf/proto"
	"github.com/jackc/pgx/v5"
	"go.uber.org/atomic"

	"github.com/tochemey/ego/v3/egopb"
	"github.com/tochemey/ego/v3/internal/postgres"
	"github.com/tochemey/ego/v3/persistence"
)

var (
	columns = []string{
		"persistence_id",
		"version_number",
		"state_payload",
		"state_manifest",
		"timestamp",
		"shard_number",
	}

	tableName = "states_store"
)

// DurableStore implements the DurableStore interface
// and helps persist events in a Postgres database
type DurableStore struct {
	db postgres.Postgres
	sb sq.StatementBuilderType
	// hold the connection state to avoid multiple connection of the same instance
	connected *atomic.Bool
}

// enforce interface implementation
var _ persistence.StateStore = (*DurableStore)(nil)

// NewStateStore creates a new instance of StateStore
func NewStateStore(config *Config) *DurableStore {
	// create the underlying db connection
	db := postgres.New(postgres.NewConfig(config.DBHost, config.DBPort, config.DBUser, config.DBPassword, config.DBName))
	return &DurableStore{
		db:              db,
		sb:              sq.StatementBuilder.PlaceholderFormat(sq.Dollar),
		connected:       atomic.NewBool(false),
	}
}

// Connect connects to the underlying postgres database
func (s *DurableStore) Connect(ctx context.Context) error {
	// check whether this instance of the journal is connected or not
	if s.connected.Load() {
		return nil
	}

	// connect to the underlying db
	if err := s.db.Connect(ctx); err != nil {
		return err
	}

	// set the connection status
	s.connected.Store(true)

	return nil
}

// Disconnect disconnects from the underlying postgres database
func (s *DurableStore) Disconnect(ctx context.Context) error {
	// check whether this instance of the journal is connected or not
	if !s.connected.Load() {
		return nil
	}

	// disconnect the underlying database
	if err := s.db.Disconnect(ctx); err != nil {
		return err
	}
	// set the connection status
	s.connected.Store(false)

	return nil
}

// Ping verifies a connection to the database is still alive, establishing a connection if necessary.
func (s *DurableStore) Ping(ctx context.Context) error {
	// check whether we are connected or not
	if !s.connected.Load() {
		return s.Connect(ctx)
	}

	return nil
}

// WriteState writes a durable state into the underlying postgres database
func (s *DurableStore) WriteState(ctx context.Context, state *egopb.DurableState) error {
	if !s.connected.Load() {
		return errors.New("durable store is not connected")
	}

	if state == nil || proto.Equal(state, &egopb.DurableState{}) {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("failed to obtain a database transaction: %w", err)
	}

	query, args, err := s.sb.Delete(tableName).Where(sq.Eq{"persistence_id": state.GetPersistenceId()}).ToSql()
	if err != nil {
		return fmt.Errorf("failed to build the delete sql statement: %w", err)
	}

	if _, err := s.db.Exec(ctx, query, args...); err != nil {
		return fmt.Errorf("failed to delete durable state from the database: %w", err)
	}

	bytea, _ := proto.Marshal(state.GetResultingState())
	manifest := string(state.GetResultingState().ProtoReflect().Descriptor().FullName())

	statement := s.sb.
		Insert(tableName).
		Columns(columns...).
		Values(
			state.GetPersistenceId(),
			state.GetVersionNumber(),
			bytea,
			manifest,
			state.GetTimestamp(),
			state.GetShard(),
		)

	query, args, err = statement.ToSql()
	if err != nil {
		return fmt.Errorf("unable to build sql insert statement: %w", err)
	}

	_, execErr := tx.Exec(ctx, query, args...)
	if execErr != nil {
		if err = tx.Rollback(ctx); err != nil {
			return fmt.Errorf("unable to rollback db transaction: %w", err)
		}
		return fmt.Errorf("failed to record durable state: %w", execErr)
	}

	// commit the transaction
	if commitErr := tx.Commit(ctx); commitErr != nil {
		return fmt.Errorf("failed to record durable state: %w", commitErr)
	}
	return nil
}

// GetLatestState fetches the latest durable state of a persistenceID
func (s *DurableStore) GetLatestState(ctx context.Context, persistenceID string) (*egopb.DurableState, error) {
	if !s.connected.Load() {
		return nil, errors.New("durable store is not connected")
	}

	statement := s.sb.
		Select(columns...).
		From(tableName).
		Where(sq.Eq{"persistence_id": persistenceID}).
		Limit(1)

	query, args, err := statement.ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build the select sql statement: %w", err)
	}

	row := new(row)
	err = s.db.Select(ctx, row, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch the latest event from the database: %w", err)
	}

	if row.PersistenceID == "" {
		return nil, nil
	}

	return row.ToDurableState()
}
