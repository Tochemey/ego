-- states_store holds the latest state of every durable state entity, one row per entity.
-- A deleted entity may keep a tombstone: its version with an empty payload and manifest.
CREATE TABLE IF NOT EXISTS states_store
(
    persistence_id TEXT    NOT NULL PRIMARY KEY,
    version_number INTEGER NOT NULL,
    state_payload  BLOB    NOT NULL,
    state_manifest TEXT    NOT NULL,
    timestamp      INTEGER NOT NULL,
    shard_number   INTEGER NOT NULL
);
