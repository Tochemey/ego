-- snapshots_store holds the snapshots of event-sourced entities.
-- The primary key serves the latest snapshot of an entity and the deletion of older ones.
CREATE TABLE IF NOT EXISTS snapshots_store
(
    persistence_id    TEXT              NOT NULL,
    sequence_number   INTEGER           NOT NULL,
    state_payload     BLOB              NOT NULL,
    state_manifest    TEXT              NOT NULL,
    timestamp         INTEGER           NOT NULL,
    encryption_key_id TEXT    DEFAULT '' NOT NULL,
    is_encrypted      INTEGER DEFAULT 0 NOT NULL,

    PRIMARY KEY (persistence_id, sequence_number)
);
