# eGo Cluster Example

A production-ready example that runs a 3-node eGo cluster on Kubernetes using [Kind](https://kind.sigs.k8s.io/). It demonstrates event sourcing, CQRS with a projection read side, Kubernetes-native peer discovery, PostgreSQL persistence, and full observability with OpenTelemetry, Jaeger, Prometheus, and Grafana.

## What This Example Shows

- **Kubernetes-native peer discovery** — pods find each other via the Kubernetes API
- **PostgreSQL-backed persistence** — events and projection offsets are stored in PostgreSQL
- **CQRS with singleton projection** — commands produce events (write side), a projection singleton on the oldest node materializes account balances into a read table (read side); migrates automatically on node failure
- **Fund-transfer saga** — a saga coordinates a debit and a credit across two account entities that may live on different pods, and refunds the source when the credit is rejected
- **3-node cluster** — shows partition distribution and quorum
- **NGINX Ingress load balancing** — requests to `http://localhost` are round-robin distributed across all 3 pods (no port-forward needed)
- **Traces and metrics** — OpenTelemetry instrumentation with Jaeger for traces and Prometheus + Grafana for metrics
- **Pre-built Grafana dashboard** — visualizes command rates, latency percentiles, event throughput, projection lag, and active entity/projection counts
- **Production patterns** — environment-based config, health probes, RBAC, distroless container image, graceful shutdown

## Architecture

```text
                          ┌──────────────────────┐
                          │     HTTP Client      │
                          │  (curl / make test)  │
                          └──────────┬───────────┘
                                     │
                          ┌──────────▼───────────┐
                          │  NGINX Ingress       │
                          │  http://localhost    │
                          │  round-robin → pods  │
                          └──────────┬───────────┘
                                     │
              ┌──────────────────────┼──────────────────────┐
              │                      │                      │
     ┌────────▼────────┐   ┌────────▼────────┐   ┌────────▼────────┐
     │  ego-cluster-0  │   │  ego-cluster-1  │   │  ego-cluster-2  │
     │  (oldest node)  │   │                 │   │                 │
     │                 │   │                 │   │                 │
     │  eGo Engine     │   │  eGo Engine     │   │  eGo Engine     │
     │  HTTP API :8080 │   │  HTTP API :8080 │   │  HTTP API :8080 │
     │  ┌────────────┐ │   │                 │   │                 │
     │  │ Projection │ │   │  (no projection │   │  (no projection │
     │  │ (singleton)│ │   │   on this node) │   │   on this node) │
     │  └─────┬──────┘ │   │                 │   │                 │
     └────────┼────────┘   └────────┬────────┘   └────────┬────────┘
              │                     │                      │
              │        gossip protocol (peer discovery)    │
              ├─────────────────────┼──────────────────────┤
              │                     │                      │
              │               OTLP/gRPC                    │
              └──────────┬──────────┼──────────┬───────────┘
                         │          │          │
                ┌────────▼──────────▼──────────▼────────┐
                │            OTel Collector             │
                └──────────┬───────────────┬────────────┘
                           │               │
              ┌────────────▼──┐    ┌───────▼────────┐
              │    Jaeger     │    │   Prometheus   │
              │   (traces)    │    │   (metrics)    │
              └───────────────┘    └───────┬────────┘
                                           │
                                   ┌───────▼────────┐
                                   │    Grafana     │
                                   │  (dashboards)  │
                                   └────────────────┘

              ┌─────────────────────────────────────────┐
              │              PostgreSQL                 │
              │  events_store | offsets_store           │
              │  account_balances (projection read tbl) │
              └─────────────────────────────────────────┘

Projection singleton: In cluster mode the projection runs on exactly ONE
node — the oldest (ego-cluster-0). If that node leaves the cluster, the
projection automatically migrates to the new oldest node. This prevents
duplicate event processing across pods.

Fund transfer saga: POST /transfers/{id} starts a saga on the pod that
accepted the request. The saga is fed from the journal, so it reacts to
the TransferStarted event whichever pod journaled it, and sends its
commands to the source and destination accounts wherever they live.
```

## Prerequisites

Make sure the following tools are installed:

- [Docker](https://docs.docker.com/get-docker/)
- [Kind](https://kind.sigs.k8s.io/docs/user/quick-start/#installation)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)
- [curl](https://curl.se/)

## Quick Start

Run everything with a single command:

```bash
cd example/cluster
make all
```

This will:

1. Create a Kind cluster named `ego-cluster` with ingress port mappings
2. Build the Docker image and load it into Kind
3. Install NGINX Ingress Controller for load balancing
4. Deploy PostgreSQL, the observability stack (OTel Collector, Jaeger, Prometheus, Grafana), RBAC, and the 3-replica app with an Ingress
5. Wait for all pods and the ingress to become ready
6. Run integration tests via `http://localhost` (load-balanced across pods)

After `make all` completes you can explore the cluster further:

```bash
# Observability UIs (each blocks until you press Ctrl+C)
make grafana     # Grafana dashboard   http://localhost:3000  (admin / admin)
make jaeger      # Jaeger trace UI     http://localhost:16686
make prometheus  # Prometheus query UI http://localhost:9090
make dashboard   # Kubernetes dashboard https://localhost:8443

# Load & inspect
make load-test   # create 1000 accounts (sequential) and report throughput/pod distribution
make db          # snapshot PostgreSQL tables (events, offsets, balances) — no psql needed
make status      # show all Kubernetes resources in the ego-example namespace
make logs        # tail logs from all app pods
make reset       # truncate all tables for a clean re-run (no teardown needed)

# Cleanup
make teardown    # delete the Kind cluster and all resources
```

## Step-by-Step

If you prefer to run each step individually:

### 1. Create the Kind cluster

```bash
make kind-create
```

### 2. Build and load the Docker image

```bash
make docker-build
```

### 3. Deploy to Kubernetes

```bash
make deploy
```

This applies the manifests in order:

- NGINX Ingress Controller — installed from the official Kind-compatible manifest
- `k8s/namespace.yaml` — creates the `ego-example` namespace
- `k8s/postgres.yaml` — deploys PostgreSQL with init SQL (events, offsets, and account_balances tables)
- `k8s/rbac.yaml` — creates ServiceAccount, Role, and RoleBinding for pod discovery
- `k8s/grafana-dashboard.yaml` — pre-built Grafana dashboard for eGo metrics
- `k8s/observability.yaml` — OTel Collector, Jaeger, Prometheus, and Grafana
- `k8s/app.yaml` — 3-replica StatefulSet + headless Service (gossip) + ClusterIP Service (HTTP) + Ingress

### 4. Wait for pods to be ready

```bash
make wait
```

### 5. Run the integration tests

```bash
make test
```

The test hits `http://localhost` through the NGINX Ingress (requests are round-robin distributed across pods):

1. Health check
2. Creates an account with balance **1000**
3. Sends **30 credit requests** of 10 each (load-balanced across pods)
4. Sends **10 debit requests** of 5 each (load-balanced across pods)
5. Waits for the projection to catch up
6. Queries the projection read table and verifies the balance is **1250** (1000 + 30x10 - 10x5)
7. Sends 30 health checks and collects the `X-Served-By` response header to **verify requests hit multiple pods**
8. Creates a second account with balance **500**
9. Transfers **250** from `acct-1` to `acct-2` through a saga and waits until its outcome is `succeeded`
10. Transfers **100** from `acct-1` to an unknown account: the debit goes through, the credit is rejected, the saga refunds the source and settles on `reverted`
11. Transfers **5000** from `acct-2` to `acct-1`: the debit is rejected for insufficient funds and the saga settles on `reverted` with nothing to undo
12. Queries the projection and verifies `acct-1` is **1000** (1250 - 250 - 100 + 100 refund) and `acct-2` is **750** (500 + 250)

### 6. Run the load test (optional)

```bash
make load-test
```

Creates **1000 accounts** sequentially via NGINX Ingress, distributed round-robin across pods. Reports:

- Pass / fail counts
- Total duration and throughput (~accounts/s)
- Pod distribution via `X-Served-By` headers (shows how load was spread across pods)

### 7. Inspect the database (optional)

```bash
make db
```

Runs `psql` inside the PostgreSQL pod and prints snapshots of all three tables — no local `psql` installation needed:

| Table              | Contents                                                  |
|--------------------|-----------------------------------------------------------|
| `account_balances` | Projection read model — current balance per account       |
| `events_store`     | Raw event log — last 20 events with sequence and manifest |
| `offsets_store`    | Projection progress — current offset per shard            |

Row counts for all three tables are shown at the end. While a transfer saga runs, its progress shows up in `offsets_store` under the projection name `ego.saga.<transfer id>`; those rows are removed once the saga settles.

## Observability

### Grafana

```bash
make grafana
```

Opens Grafana at [http://localhost:3000](http://localhost:3000) (login: `admin` / `admin`).

The pre-built **eGo Cluster** dashboard is automatically provisioned and includes:

| Panel                                | Description                                   |
|--------------------------------------|-----------------------------------------------|
| Commands Processed (rate/s)          | Throughput of commands per second             |
| Command Duration (p50/p95/p99)       | Latency histogram percentiles in milliseconds |
| Events Persisted (rate/s)            | Rate of events written to the event store     |
| Projection Events Processed (rate/s) | Rate of events consumed by the projection     |
| Active Entities                      | Current number of live entity actors          |
| Active Projections                   | Current number of running projection actors   |
| Commands Total                       | Cumulative command count                      |
| Events Persisted Total               | Cumulative event count                        |
| Projection Lag (ms)                  | How far behind each projection shard is       |
| Projection Events Behind             | Approximate unprocessed event count per shard |

Direct link: [http://localhost:3000/d/ego-cluster-dashboard](http://localhost:3000/d/ego-cluster-dashboard)

### Jaeger

```bash
make jaeger
```

Opens Jaeger at [http://localhost:16686](http://localhost:16686). Select service `ego-cluster` to see traces for command processing, including:

- Span name: `ego.command`
- Attributes: `ego.persistence_id`, `ego.command_type`

### Prometheus

```bash
make prometheus
```

Opens Prometheus at [http://localhost:9090](http://localhost:9090). Available metrics:

eGo defines OpenTelemetry instruments with dotted names; the OpenTelemetry Collector exposes them to Prometheus with underscores.

| Metric                            | Type          | Description                         |
|-----------------------------------|---------------|-------------------------------------|
| `ego_commands_total`              | Counter       | Total commands processed            |
| `ego_commands_duration`           | Histogram     | Command processing duration (ms)    |
| `ego_events_persisted`            | Counter       | Total events persisted              |
| `ego_projection_events_processed` | Counter       | Total projection events processed   |
| `ego_entities_active`             | UpDownCounter | Currently active entities           |
| `ego_projections_active`          | UpDownCounter | Currently active projections        |
| `ego_projection_lag_ms`           | Gauge         | Projection lag per shard (ms)       |
| `ego_projection_latest_offset`    | Gauge         | Current projection offset per shard |
| `ego_projection_events_behind`    | Gauge         | Unprocessed events per shard        |

### Kubernetes Dashboard

```bash
make dashboard
```

Installs and opens the Kubernetes Dashboard. A token is printed to the terminal for login.

## All Make Targets

| Target              | Description                                                                     |
|---------------------|---------------------------------------------------------------------------------|
| `make all`          | Full flow: create cluster, build, deploy, wait, test                            |
| `make kind-create`  | Create the Kind cluster with ingress port mappings                              |
| `make docker-build` | Build the Docker image and load it into Kind                                    |
| `make deploy`       | Apply all Kubernetes manifests                                                  |
| `make wait`         | Wait for StatefulSet rollout and ingress readiness                              |
| `make test`         | Run integration tests via ingress (balance check + pod spread + transfer sagas) |
| `make load-test`    | Create 1000 accounts (sequential); report throughput + pod dist                 |
| `make db`           | Snapshot PostgreSQL tables (events, offsets, balances) in-cluster               |
| `make grafana`      | Port-forward Grafana to localhost:3000 (admin / admin)                          |
| `make jaeger`       | Port-forward Jaeger to localhost:16686                                          |
| `make prometheus`   | Port-forward Prometheus to localhost:9090                                       |
| `make dashboard`    | Install and open the Kubernetes dashboard at `https://localhost:8443`           |
| `make reset`        | Truncate all tables for a clean re-run (no teardown needed)                     |
| `make status`       | Show all Kubernetes resources in the ego-example namespace                      |
| `make logs`         | Tail logs from all app pods                                                     |
| `make teardown`     | Delete the Kind cluster and all resources                                       |
| `make clean`        | Alias for `teardown`                                                            |

## HTTP API

The app exposes the following endpoints on port `8080`:

| Method | Path                    | Description                                                                                                                                     |
|--------|-------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------|
| `GET`  | `/healthz`              | Health and readiness probe                                                                                                                      |
| `POST` | `/accounts/{id}`        | Create an account (body: `{"balance": 1000}`)                                                                                                   |
| `POST` | `/accounts/{id}/credit` | Credit an account (body: `{"amount": 250}`)                                                                                                     |
| `POST` | `/accounts/{id}/debit`  | Debit an account (body: `{"amount": 100}`)                                                                                                      |
| `GET`  | `/accounts/{id}`        | Query account balance from the **projection read table**                                                                                        |
| `POST` | `/transfers/{id}`       | Start a fund transfer saga (body: `{"source_account_id": "acct-1", "destination_account_id": "acct-2", "amount": 250}`); answers `202 Accepted` |
| `GET`  | `/transfers/{id}`       | Query the saga: status, outcome, completed steps, and failure reason                                                                            |

The write-side endpoints (`POST`) send commands to event-sourced entities. The read-side endpoint (`GET /accounts/{id}`) queries the `account_balances` table, which is populated by the projection handler as it consumes events. The transfer endpoints drive a saga, described next.

## Fund Transfer Saga

`POST /transfers/{id}` moves money between two accounts with a saga, eGo's process manager for workflows that span several entities. Three pieces are involved:

| Piece              | File          | Role                                                                                                                 |
|--------------------|---------------|----------------------------------------------------------------------------------------------------------------------|
| `TransferBehavior` | `transfer.go` | Event-sourced entity, journaled under `transfer-<id>`, that validates `StartTransfer` and journals `TransferStarted` |
| `FundTransferSaga` | `saga.go`     | Saga keyed by the transfer ID: debits the source, credits the destination, refunds on failure                        |
| `AccountBehavior`  | `behavior.go` | The participants: `DebitAccount` rejects insufficient funds, `CreditAccount` always succeeds                         |

The request handler starts the saga first, then creates the transfer entity and sends it `StartTransfer`. The order matters: a saga only sees events journaled after it started.

### Which ID is which

One transfer of 250 from `acct-1` to `acct-2`, requested as `POST /transfers/tx-1`, involves four IDs:

| ID | What it names | Who chooses it |
| --- | --- | --- |
| `acct-1` | Source account entity | The client, when it created the account |
| `acct-2` | Destination account entity | The client |
| `tx-1` | The saga, that is the transfer process | The client, in the request path |
| `transfer-tx-1` | The transfer entity that records the request | Derived from `tx-1` by `TransferBehavior.ID()` |

1. The handler starts the saga. This spawns an actor named `tx-1` whose journal is persistence ID `tx-1`; from now on it reads every event written to the store.

   ```go
   saga := NewFundTransferSaga("tx-1", "acct-1", "acct-2", 250)
   engine.Saga(ctx, saga, transferTimeout, ego.WithOffsetRemoval())
   ```

2. The handler creates the transfer entity and sends it the command. The entity journals `TransferStarted{transfer_id: "tx-1"}` under persistence ID `transfer-tx-1`.

   ```go
   transfer := NewTransferBehavior("tx-1") // transfer.ID() == "transfer-tx-1"
   engine.Entity(ctx, transfer)
   engine.SendCommand(ctx, transfer.ID(), &samplepb.StartTransfer{TransferId: "tx-1", ...}, timeout)
   ```

3. The saga reads that event, sees that `transfer_id` is its own ID, and sends `DebitAccount` to entity `acct-1`.
4. `acct-1` journals `AccountDebited` under `acct-1` and replies. `HandleResult` receives `entityID == "acct-1"`, journals `SourceDebited` under `tx-1`, and sends `CreditAccount` to `acct-2`.
5. `acct-2` journals `AccountCredited` under `acct-2` and replies. `HandleResult` receives `entityID == "acct-2"`, journals `DestinationCredited` under `tx-1`, and completes.
6. `GET /transfers/tx-1` runs `engine.SagaStatus(ctx, "tx-1", ...)`.

The events table then holds:

| persistence_id | events |
| --- | --- |
| `transfer-tx-1` | `TransferStarted` |
| `acct-1` | `AccountDebited` |
| `acct-2` | `AccountCredited` |
| `tx-1` | `SagaStatusChanged`, `SourceDebited`, `DestinationCredited`, `SagaStatusChanged` |

The account IDs are only ever command targets: the saga keeps them in its state and sends commands to them. The saga's own journal and actor are `tx-1`, which is why the transfer entity cannot also be `tx-1`: the store would hold two streams under one persistence ID and the actor system two actors under one name. That is the only reason for the `transfer-` prefix.

The transfer entity is not part of the saga pattern itself. A saga only reacts to events in the journal, so something has to write the event that kicks it off; here that is the transfer entity, which lets the saga own the debit step and report a rejected debit.

```text
POST /transfers/tx-1
  │
  ├─ engine.Saga(FundTransferSaga)         the saga starts reading the journal
  ├─ engine.Entity(TransferBehavior)
  └─ SendCommand(StartTransfer) ────────►  TransferStarted journaled
                                              │
                        saga HandleEvent  ◄───┘  (read from the journal, whichever pod wrote it)
                          │
                          ├─ DebitAccount ───► source account ──► HandleResult: journals SourceDebited
                          │
                          ├─ CreditAccount ──► destination ─────► HandleResult: journals DestinationCredited
                          │
                          └──────────────────────────────────────► completed
```

When a step is rejected, `HandleError` journals `TransferFailed` with the reason and asks for compensation. `Compensate` refunds the source if it was debited; otherwise there is nothing to undo and the saga settles at once.

`GET /transfers/{id}` reports the saga status together with an `outcome` derived from the status and the saga state, since a saga that refunded the source completes just like one that moved the money:

| `outcome`   | Meaning                                                                                                            |
|-------------|--------------------------------------------------------------------------------------------------------------------|
| `pending`   | The saga is running or compensating                                                                                |
| `succeeded` | Completed and the destination was credited                                                                         |
| `reverted`  | Completed without crediting the destination: the source was refunded, or never debited (`failure_reason` says why) |
| `failed`    | The refund itself was rejected; an operator has to look at the transfer                                            |

What the cluster changes:

- The saga actor lives on the pod that accepted the request, and `GET /transfers/{id}` reaches it from any pod. It is long-lived, so it is relocated when its pod leaves the cluster; that is why `FundTransferSaga` is listed in `ego.WithEntityKinds` next to the entities.
- The saga is fed from the journal, so the transfer entity and both accounts can be on any pod. An event written on the saga's own pod reaches it immediately; one written by a peer arrives on the next poll.
- Delivery to the saga is at-least-once, and so are its commands to the accounts. The saga checks its own state before acting, but the accounts in this example do not deduplicate: a saga restarted between sending a command and recording its result could debit or refund twice. A production participant keys such commands by transfer ID.
- Sagas are started with `ego.WithOffsetRemoval()`, so the offset rows they record while reading the journal are deleted once they settle and the offset store does not grow with every transfer ever made.
- A transfer ID is used once: starting a saga with an existing ID answers `409 Conflict`.

## Project Structure

```text
example/cluster/
├── main.go                  # Engine setup, HTTP API, graceful shutdown
├── behavior.go              # AccountBehavior (event-sourced entity)
├── transfer.go              # TransferBehavior (event-sourced entity that starts a transfer)
├── saga.go                  # FundTransferSaga (debit, credit, refund on failure)
├── discovery.go             # Kubernetes cluster discovery provider
├── stores.go                # PostgreSQL EventsStore and OffsetStore
├── projection.go            # Projection handler (materializes account balances)
├── telemetry.go             # OpenTelemetry setup (OTLP/gRPC exporters)
├── Dockerfile               # Multi-stage build (golang → distroless)
├── Makefile                 # All build, deploy, and test targets
├── kind-config.yaml         # Kind cluster config with ingress port mappings
├── go.mod                   # Separate module (isolates k8s/pgx/otel dependencies)
├── go.sum
├── README.md
└── k8s/
    ├── namespace.yaml       # ego-example namespace
    ├── postgres.yaml        # PostgreSQL StatefulSet + init SQL
    ├── rbac.yaml            # ServiceAccount + Role + RoleBinding
    ├── observability.yaml   # OTel Collector, Jaeger, Prometheus, Grafana
    ├── grafana-dashboard.yaml # Pre-built Grafana dashboard for eGo metrics
    └── app.yaml             # 3-replica StatefulSet + headless Service
```

## Load Balancing

All HTTP requests go through the **NGINX Ingress Controller**, which distributes them across the 3 app pods using round-robin. After `make all`, the API is accessible at `http://localhost` — no port-forwarding needed.

The Kind cluster is created with `extraPortMappings` (see `kind-config.yaml`) so that host port 80 maps into the cluster's ingress controller node. The Ingress resource in `k8s/app.yaml` routes all paths (`/`) to the `ego-cluster` ClusterIP service.

A separate headless service (`ego-cluster-headless`) is kept for gossip-based peer discovery — it is not used for HTTP traffic.

## Dependency Isolation

This example is a **separate Go module** (`github.com/tochemey/ego/v4/example/cluster`) with its own `go.mod`. Heavy dependencies like `k8s.io/client-go`, `github.com/jackc/pgx/v5`, and the OpenTelemetry SDK are confined to this module and do not affect the core eGo library.

## Cleanup

```bash
make teardown
```

This deletes the Kind cluster and all associated resources.
