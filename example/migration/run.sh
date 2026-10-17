#!/usr/bin/env bash
# Migrates a large eGo v4 PostgreSQL deployment to v5 end to end:
#
#   1. seed the v4 database with a large journal, snapshots and offsets;
#   2. copy the journal and snapshots while the v4 deployment still writes;
#   3. stop the v4 writers, copy the remainder and resume the projections;
#   4. verify the v5 database against the v4 one.
#
# ENTITIES, EVENTS and WRITE_FOR size the run; KEEP_DATABASES=1 leaves the
# databases running afterwards for inspection.
set -euo pipefail
cd "$(dirname "$0")"

ENTITIES="${ENTITIES:-10000}"
EVENTS="${EVENTS:-100}"
WRITE_FOR="${WRITE_FOR:-30s}"
LEGACY="postgres://ego:ego@legacy-db:5432/ego?sslmode=disable"
TARGET="postgres://ego:ego@target-db:5432/ego?sslmode=disable"

compose() {
  docker compose --profile tool "$@"
}

tool() {
  compose run --rm --no-deps tool "$@"
}

step() {
  printf '\n==> %s\n' "$*"
}

cleanup() {
  if [[ "${KEEP_DATABASES:-0}" != "1" ]]; then
    compose down --volumes --remove-orphans >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

step "Building the tool and starting the v4 and v5 databases"
compose build tool
compose up --detach --wait legacy-db target-db

step "Seeding the v4 database: ${ENTITIES} entities of ${EVENTS} events"
tool seed --legacy "$LEGACY" --entities "$ENTITIES" --events "$EVENTS"

step "Copying the journal and snapshots while the v4 deployment still writes for ${WRITE_FOR}"
tool write --legacy "$LEGACY" --entities "$ENTITIES" --for "$WRITE_FOR" &
writer=$!
time tool migrate journal --source "$LEGACY" --target "$TARGET"
wait "$writer"

step "The v4 writers stopped: copying the remainder and resuming the projections"
time tool migrate all --source "$LEGACY" --target "$TARGET"

step "Verifying the v5 database against the v4 one"
tool verify --legacy "$LEGACY" --target "$TARGET"
