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
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/anypb"

	samplepb "github.com/tochemey/ego/v4/example/examplepb"
	"github.com/tochemey/ego/v4/projection"
)

// AccountBalanceHandler implements projection.Handler.
// It materializes account balance events into the account_balances read table
// in PostgreSQL, providing a queryable read model for the CQRS read side.
type AccountBalanceHandler struct {
	pool *pgxpool.Pool
}

var _ projection.Handler = (*AccountBalanceHandler)(nil)

// NewAccountBalanceHandler creates a projection handler that writes to the
// account_balances table using the given connection pool.
func NewAccountBalanceHandler(pool *pgxpool.Pool) *AccountBalanceHandler {
	return &AccountBalanceHandler{pool: pool}
}

// Handle processes a single event from the projection runner.
// It unmarshals the event and upserts the account balance accordingly.
func (h *AccountBalanceHandler) Handle(ctx context.Context, persistenceID string, event *anypb.Any, _ uint64) error {
	msg, err := event.UnmarshalNew()
	if err != nil {
		slog.Warn("projection: failed to unmarshal event, skipping",
			"persistenceID", persistenceID, "typeUrl", event.GetTypeUrl(), "err", err)
		return nil
	}

	now := time.Now().UnixMilli()

	switch evt := msg.(type) {
	case *samplepb.AccountCreated:
		_, err = h.pool.Exec(ctx, `
			INSERT INTO account_balances (account_id, balance, updated_at)
			VALUES ($1, $2, $3)
			ON CONFLICT (account_id) DO UPDATE SET balance = EXCLUDED.balance, updated_at = EXCLUDED.updated_at`,
			evt.GetAccountId(), evt.GetAccountBalance(), now)

	case *samplepb.AccountCredited:
		_, err = h.pool.Exec(ctx, `
			INSERT INTO account_balances (account_id, balance, updated_at)
			VALUES ($1, $2, $3)
			ON CONFLICT (account_id) DO UPDATE SET
				balance = account_balances.balance + EXCLUDED.balance,
				updated_at = EXCLUDED.updated_at`,
			evt.GetAccountId(), evt.GetAccountBalance(), now)

	case *samplepb.AccountDebited:
		_, err = h.pool.Exec(ctx, `
			INSERT INTO account_balances (account_id, balance, updated_at)
			VALUES ($1, -$2, $3)
			ON CONFLICT (account_id) DO UPDATE SET
				balance = account_balances.balance - $2,
				updated_at = EXCLUDED.updated_at`,
			evt.GetAccountId(), evt.GetAccountBalance(), now)

	default:
		// Ignore events we don't care about (e.g., saga events).
		return nil
	}

	if err != nil {
		return err
	}
	return nil
}
