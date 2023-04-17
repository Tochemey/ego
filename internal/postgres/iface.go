package postgres

import (
	"context"
	"database/sql"
)

// IDatabase will be implemented by concrete RDBMS store
type IDatabase interface {
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
	Exec(ctx context.Context, query string, args ...any) (sql.Result, error)
	// BeginTx helps start an SQL transaction. The return transaction object is expected to be used in
	// the subsequent queries following the BeginTx.
	BeginTx(ctx context.Context, txOptions *sql.TxOptions) (*sql.Tx, error)
}
