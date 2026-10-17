--  MIT License
--
--  Copyright (c) 2022-2026 Arsene Tochemey Gandote
--
--  Permission is hereby granted, free of charge, to any person obtaining a copy
--  of this software and associated documentation files (the "Software"), to deal
--  in the Software without restriction, including without limitation the rights
--  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
--  copies of the Software, and to permit persons to whom the Software is
--  furnished to do so, subject to the following conditions:
--
--  The above copyright notice and this permission notice shall be included in all
--  copies or substantial portions of the Software.
--
--  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
--  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
--  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
--  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
--  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
--  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
--  SOFTWARE.


-- The journal of persistence.EventsStore (eGo v5).
--
-- Every event of a persistence ID is stored on the shard persistence.ShardFor
-- gives it, and an entity's timestamps strictly increase. WriteEvents must
-- insert without ON CONFLICT: the primary key then rejects an event already
-- stored, which fails the whole write.
CREATE TABLE IF NOT EXISTS events_store
(
    persistence_id    VARCHAR(255)            NOT NULL,
    sequence_number   BIGINT                  NOT NULL,
    is_deleted        BOOLEAN DEFAULT FALSE   NOT NULL,
    event_payload     BYTEA                   NOT NULL,
    event_manifest    VARCHAR(255)            NOT NULL,
    timestamp         BIGINT                  NOT NULL,
    shard_number      BIGINT                  NOT NULL,
    encryption_key_id VARCHAR(255) DEFAULT '' NOT NULL,
    is_encrypted      BOOLEAN DEFAULT FALSE   NOT NULL,

    -- WriteEvents, ReplayEvents, GetLatestEvent, DeleteEvents and PersistenceIDs
    PRIMARY KEY (persistence_id, sequence_number)
);

-- EventsByShard:
--   SELECT ... FROM events_store WHERE shard_number = $1 AND timestamp >= $2
--   ORDER BY timestamp, sequence_number, persistence_id LIMIT $3
-- and ShardOffsets:
--   SELECT shard_number, MAX(timestamp) FROM events_store GROUP BY shard_number
CREATE INDEX IF NOT EXISTS idx_events_store_shard ON events_store (shard_number, timestamp, sequence_number, persistence_id);
