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
    shard_number    BIGINT                NOT NULL,

    PRIMARY KEY (persistence_id, sequence_number)
);

--- create an index on the is_deleted column
CREATE INDEX IF NOT EXISTS idx_event_journal_deleted ON events_store (is_deleted);
CREATE INDEX IF NOT EXISTS idx_event_journal_shard ON events_store (shard_number);
