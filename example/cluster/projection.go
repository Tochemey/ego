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

package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/tochemey/ego/v5/egopb"
	samplepb "github.com/tochemey/ego/v5/example/examplepb"
	"github.com/tochemey/ego/v5/projection"
)

// AccountBalanceHandler keeps the account_balances read model. Credits and
// debits are added to the stored balance, so applying an event twice would
// count it twice: each event is applied and its offset recorded in one
// transaction, so the projection applies it exactly once.
type AccountBalanceHandler struct {
	// pool is the connection pool of the database holding the read model and the offsets.
	pool *pgxpool.Pool
	// offsets is the projection's offset store, in the same database.
	offsets *PostgresOffsetStore
	// projectionName is the name the offsets are recorded under.
	projectionName string
}

var _ projection.TransactionalHandler = (*AccountBalanceHandler)(nil)

// NewAccountBalanceHandler creates the handler of the named projection.
func NewAccountBalanceHandler(pool *pgxpool.Pool, offsets *PostgresOffsetStore, projectionName string) *AccountBalanceHandler {
	return &AccountBalanceHandler{pool: pool, offsets: offsets, projectionName: projectionName}
}

// Handle applies the event to the read model and records its offset in the same transaction.
func (h *AccountBalanceHandler) Handle(ctx context.Context, persistenceID string, event *anypb.Any, _ uint64, offset *egopb.EntityOffset) error {
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	if err := applyBalanceEvent(ctx, tx, persistenceID, event); err != nil {
		return err
	}

	if err := h.offsets.WriteOffsetsTx(ctx, tx, h.projectionName, []*egopb.EntityOffset{offset}); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// applyBalanceEvent applies one account event to the account_balances read
// model within the transaction. Events the read model does not track are ignored.
func applyBalanceEvent(ctx context.Context, tx pgx.Tx, persistenceID string, event *anypb.Any) error {
	msg, err := event.UnmarshalNew()
	if err != nil {
		slog.Warn("projection: failed to unmarshal event, skipping",
			"persistenceID", persistenceID, "typeUrl", event.GetTypeUrl(), "err", err)
		return nil
	}

	now := time.Now().UnixMilli()

	switch evt := msg.(type) {
	case *samplepb.AccountCreated:
		_, err = tx.Exec(ctx, `
			INSERT INTO account_balances (account_id, balance, updated_at)
			VALUES ($1, $2, $3)
			ON CONFLICT (account_id) DO UPDATE SET balance = EXCLUDED.balance, updated_at = EXCLUDED.updated_at`,
			evt.GetAccountId(), evt.GetAccountBalance(), now)

	case *samplepb.AccountCredited:
		_, err = tx.Exec(ctx, `
			INSERT INTO account_balances (account_id, balance, updated_at)
			VALUES ($1, $2, $3)
			ON CONFLICT (account_id) DO UPDATE SET
				balance = account_balances.balance + EXCLUDED.balance,
				updated_at = EXCLUDED.updated_at`,
			evt.GetAccountId(), evt.GetAccountBalance(), now)

	case *samplepb.AccountDebited:
		// Pass the delta as a single typed parameter so EXCLUDED.balance can
		// be used in both the INSERT and the ON CONFLICT branch. Using `-$2`
		// inline trips Postgres' "operator is not unique" check (SQLSTATE
		// 42725) because the prefix `-` can't pick an overload against an
		// untyped placeholder.
		delta := -evt.GetAccountBalance()
		_, err = tx.Exec(ctx, `
			INSERT INTO account_balances (account_id, balance, updated_at)
			VALUES ($1, $2, $3)
			ON CONFLICT (account_id) DO UPDATE SET
				balance = account_balances.balance + EXCLUDED.balance,
				updated_at = EXCLUDED.updated_at`,
			evt.GetAccountId(), delta, now)
	}

	return err
}
