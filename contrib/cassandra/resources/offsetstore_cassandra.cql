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

-- offsets_store keeps the offsets of a projection on a shard in one partition,
-- the newest timestamp first.
CREATE TABLE IF NOT EXISTS offsets_store
(
    projection_name text,
    shard_number    bigint,
    timestamp       bigint,
    persistence_id  text,
    sequence_number bigint,
    consumed_at     bigint,
    PRIMARY KEY ((projection_name, shard_number), timestamp, persistence_id, sequence_number)
) WITH CLUSTERING ORDER BY (timestamp DESC, persistence_id DESC, sequence_number DESC);

-- offsets_by_entity holds the same offsets ordered by persistence ID, the
-- highest sequence number first, so the offset of a persistence ID is one read.
CREATE TABLE IF NOT EXISTS offsets_by_entity
(
    projection_name text,
    shard_number    bigint,
    persistence_id  text,
    sequence_number bigint,
    timestamp       bigint,
    consumed_at     bigint,
    PRIMARY KEY ((projection_name, shard_number), persistence_id, sequence_number)
) WITH CLUSTERING ORDER BY (persistence_id ASC, sequence_number DESC);

-- offsets_start keeps the timestamp a projection starts from after a reset.
CREATE TABLE IF NOT EXISTS offsets_start
(
    projection_name text PRIMARY KEY,
    start_offset    bigint
);
