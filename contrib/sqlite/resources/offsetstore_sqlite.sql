-- offsets_store holds one row per event a projection processed: the offset of an entity.
-- The primary key serves the reads of a shard, newest first, and the deletion of old offsets;
-- idx_offsets_store_entity serves the latest offset of an entity. Its rows are small, so the
-- table is stored without a rowid, clustered on the primary key.
CREATE TABLE IF NOT EXISTS offsets_store
(
    projection_name TEXT    NOT NULL,
    shard_number    INTEGER NOT NULL,
    timestamp       INTEGER NOT NULL,
    persistence_id  TEXT    NOT NULL,
    sequence_number INTEGER NOT NULL,
    consumed_at     INTEGER NOT NULL,

    PRIMARY KEY (projection_name, shard_number, timestamp, persistence_id, sequence_number)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_offsets_store_entity ON offsets_store (projection_name, shard_number, persistence_id, sequence_number);

-- offsets_start holds the timestamp a projection starts from, recorded when its offsets are reset.
CREATE TABLE IF NOT EXISTS offsets_start
(
    projection_name TEXT    NOT NULL PRIMARY KEY,
    start_offset    INTEGER NOT NULL
);
