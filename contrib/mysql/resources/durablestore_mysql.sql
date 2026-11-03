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

-- The durable states of persistence.StateStore (eGo v5), for MySQL 8.0.19 or
-- later: one row per persistence ID, replaced on every write. A deleted state
-- is either the removed row or a tombstone row whose state_manifest is empty.
CREATE TABLE IF NOT EXISTS states_store
(
    persistence_id VARCHAR(255) NOT NULL PRIMARY KEY,
    version_number BIGINT       NOT NULL,
    state_payload  LONGBLOB     NOT NULL,
    state_manifest VARCHAR(255) NOT NULL,
    timestamp      BIGINT       NOT NULL,
    shard_number   BIGINT       NOT NULL
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_bin;
