# Migrating a v4 deployment to v5

This example migrates a large eGo v4 PostgreSQL deployment to v5 end to end, with the [migration command](../../cmd/migrate), while the v4 deployment keeps writing during the first copy. It runs in Docker Compose: two PostgreSQL databases and one tool.

## What it shows

The v4 database is seeded with the data a real v4 deployment accumulates, including every case v5 changes:

- **Events on cluster partitions.** v4 stored an event on its entity's cluster partition. Every event moves to the fixed shard `persistence.ShardFor` gives its entity.
- **Clocks that stepped back.** One entity in ten has its second half of events stamped ten seconds before its first half. v5 timestamps of an entity strictly increase, so the migration restamps those events a nanosecond after their predecessor.
- **Entities whose events retention deleted.** One entity in twenty-five keeps only a snapshot. The migration lists the snapshot store, not the journal, so these snapshots are copied too.
- **Projection offsets per legacy shard.** Two projections committed one offset per v4 partition, except one partition holding events that the audit projection never committed on. The balances projection resumes on the v5 journal from the oldest of its offsets; the audit projection resumes from the start, since v4 would have read that partition from its start.
- **Writes during the copy.** The journal is copied while a writer keeps appending events to the v4 journal, then copied again after the writer stops. The second run continues where the first one stopped.

At the end the tool checks the v5 database against the v4 one: the same events with the same payloads, every event on its fixed shard, strictly increasing timestamps, the latest snapshot of every entity, and both projections resumed.

## Running it

You need Docker with Compose v2.

```shell
./run.sh
```

The default run seeds 10,000 entities of 100 events each, about a million events, and writes for 30 seconds during the first copy. Size it with environment variables:

```shell
ENTITIES=100000 EVENTS=50 WRITE_FOR=1m ./run.sh
```

`KEEP_DATABASES=1 ./run.sh` leaves the databases running afterwards; inspect them with `docker compose exec legacy-db psql -U ego` or `docker compose exec target-db psql -U ego`, and remove them with `docker compose down --volumes`.

The script prints each step, then the checks:

```text
PASS  events           ... in v4, ... in v5
PASS  entities         ... entities in v4, ... in v5, with the same latest sequence numbers
PASS  payloads         every event payload is copied unchanged
PASS  shards           ... of ... entities were off persistence.ShardFor in v4, 0 in v5
PASS  timestamps       ... events stamped at or before their previous one in v4, 0 in v5
PASS  snapshots        latest snapshot of ... entities copied, ... of them with no event left
PASS  projections      audit from the start (v4 partitions holding events without its offset: 1); balances from its oldest v4 offset
```

## How it is built

| File | Role |
|---|---|
| [`docker-compose.yml`](docker-compose.yml) | The v4 database, created with [`schema/v4.sql`](schema/v4.sql) (the v4.5.1 reference schemas); the v5 database, created with the v5 reference schemas in [`resources`](../../resources); and the tool. |
| [`main.go`](main.go) | The tool. Its `migrate` subcommand is `migrate.NewCommand` with an opener over the two databases. |
| [`stores.go`](stores.go) | The PostgreSQL stores the opener returns. The v4 and v5 event and snapshot tables have the same columns, so the same stores read the v4 data and write the v5 data. `legacyOffsets` reads the v4 offsets table. |
| [`seed.go`](seed.go), [`write.go`](write.go), [`verify.go`](verify.go) | Seeding the v4 database, writing to it during the copy, and checking the result. |
| [`run.sh`](run.sh) | The steps, in order. |

To migrate your own deployment, write the same kind of opener over your stores, as the [migration guide](../../MIGRATING_V4_TO_V5.md) explains, and run the same two steps: `migrate journal` as often as you like while v4 still runs, then `migrate all` once after its writers stopped.
