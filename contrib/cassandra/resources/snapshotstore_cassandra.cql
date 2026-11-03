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

-- snapshots_store keeps the snapshots of every persistence ID in one partition,
-- the highest sequence number first, so the latest snapshot is the partition's
-- first row.
CREATE TABLE IF NOT EXISTS snapshots_store
(
    persistence_id    text,
    sequence_number   bigint,
    state_payload     blob,
    state_manifest    text,
    timestamp         bigint,
    encryption_key_id text,
    is_encrypted      boolean,
    PRIMARY KEY ((persistence_id), sequence_number)
) WITH CLUSTERING ORDER BY (sequence_number DESC);
