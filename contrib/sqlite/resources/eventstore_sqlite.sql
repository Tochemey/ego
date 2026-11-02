-- events_store holds the journal of every event-sourced entity.
-- The primary key serves the replay of an entity; idx_events_store_shard_timestamp
-- serves the reads of a shard by projections and the newest timestamp of each shard.
CREATE TABLE IF NOT EXISTS events_store
(
    persistence_id    TEXT              NOT NULL,
    sequence_number   INTEGER           NOT NULL,
    is_deleted        INTEGER DEFAULT 0 NOT NULL,
    event_payload     BLOB              NOT NULL,
    event_manifest    TEXT              NOT NULL,
    timestamp         INTEGER           NOT NULL,
    shard_number      INTEGER           NOT NULL,
    encryption_key_id TEXT    DEFAULT '' NOT NULL,
    is_encrypted      INTEGER DEFAULT 0 NOT NULL,

    PRIMARY KEY (persistence_id, sequence_number)
);

CREATE INDEX IF NOT EXISTS idx_events_store_shard_timestamp ON events_store (shard_number, timestamp, sequence_number, persistence_id);
