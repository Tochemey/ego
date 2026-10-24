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


-- The offsets of offsetstore.OffsetStore (eGo v5): one row per event a
-- projection recorded, the last processed event of each persistence ID.
-- WriteOffsets inserts with ON CONFLICT DO NOTHING; DeleteOffsetsBefore removes
-- the old rows.
CREATE TABLE IF NOT EXISTS offsets_store
(
    projection_name VARCHAR(255) NOT NULL,
    shard_number    BIGINT       NOT NULL,
    timestamp       BIGINT       NOT NULL,
    persistence_id  VARCHAR(255) NOT NULL,
    sequence_number BIGINT       NOT NULL,
    consumed_at     BIGINT       NOT NULL,

    -- ReadOffsets (newest first), LatestOffsets and DeleteOffsetsBefore
    PRIMARY KEY (projection_name, shard_number, timestamp, persistence_id, sequence_number)
);

-- ReadEntityOffset:
--   SELECT ... FROM offsets_store
--   WHERE projection_name = $1 AND shard_number = $2 AND persistence_id = $3
--   ORDER BY sequence_number DESC LIMIT 1
CREATE INDEX IF NOT EXISTS idx_offsets_store_entity ON offsets_store (projection_name, shard_number, persistence_id, sequence_number);

-- The point each projection starts from: ResetOffset records it, StartOffset
-- reads it and DeleteOffset removes it.
CREATE TABLE IF NOT EXISTS offsets_start
(
    projection_name VARCHAR(255) PRIMARY KEY,
    start_offset    BIGINT       NOT NULL
);
