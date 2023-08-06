package postgres

import (
	"context"

	"github.com/tochemey/gopack/postgres"
)

// SchemaUtils help create the various test tables in unit/integration tests
type SchemaUtils struct {
	db *postgres.TestDB
}

// NewSchemaUtils creates an instance of SchemaUtils
func NewSchemaUtils(db *postgres.TestDB) *SchemaUtils {
	return &SchemaUtils{db: db}
}

// CreateTable creates the event store table used for unit tests
func (d SchemaUtils) CreateTable(ctx context.Context) error {
	schemaDDL := `
	DROP TABLE IF EXISTS events_store;
	CREATE TABLE IF NOT EXISTS events_store
	(
	    persistence_id  VARCHAR(255)          NOT NULL,
	    sequence_number BIGINT                NOT NULL,
	    is_deleted      BOOLEAN DEFAULT FALSE NOT NULL,
	    event_payload   BYTEA                 NOT NULL,
	    event_manifest  VARCHAR(255)          NOT NULL,
	    state_payload   BYTEA                 NOT NULL,
	    state_manifest  VARCHAR(255)          NOT NULL,
	    timestamp       BIGINT                NOT NULL,
	    shard_number BIGINT NOT NULL ,
	
	    PRIMARY KEY (persistence_id, sequence_number)
	);
	
	--- create an index on the is_deleted column
	CREATE INDEX IF NOT EXISTS idx_event_journal_deleted ON events_store (is_deleted);
	CREATE INDEX IF NOT EXISTS idx_event_journal_shard ON events_store (shard_number);
	`
	_, err := d.db.Exec(ctx, schemaDDL)
	return err
}

// DropTable drop the table used in unit test
// This is useful for resource cleanup after a unit test
func (d SchemaUtils) DropTable(ctx context.Context) error {
	return d.db.DropTable(ctx, tableName)
}
