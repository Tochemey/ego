# eGo Cluster Example

A production-ready example that runs a 3-node eGo cluster on Kubernetes using [Kind](https://kind.sigs.k8s.io/).
It demonstrates event sourcing, CQRS with a projection read side, Kubernetes-native peer discovery, PostgreSQL persistence, and full observability with OpenTelemetry, Jaeger, Prometheus, and Grafana.

## What This Example Shows

- **Kubernetes-native peer discovery** — pods find each other via the Kubernetes API
- **PostgreSQL-backed persistence** — events and projection offsets are stored in PostgreSQL
- **CQRS with singleton projection** — commands produce events (write side), a projection singleton on the oldest node materializes account balances into a read table (read side); migrates automatically on node failure
- **3-node cluster** — shows partition distribution and quorum
- **NGINX Ingress load balancing** — requests to `http://localhost` are round-robin distributed across all 3 pods (no port-forward needed)
- **Traces and metrics** — OpenTelemetry instrumentation with Jaeger for traces and Prometheus + Grafana for metrics
- **Pre-built Grafana dashboard** — visualizes command rates, latency percentiles, event throughput, projection lag, and active entity/projection counts
- **Production patterns** — environment-based config, health probes, RBAC, distroless container image, graceful shutdown

## Architecture

```
                          ┌──────────────────────┐
                          │     HTTP Client      │
                          │  (curl / make test)  │
                          └──────────┬───────────┘
                                     │
                          ┌──────────▼───────────┐
                          │  NGINX Ingress       │
                          │  http://localhost     │
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
- `k8s/app.yaml` — 3-replica Deployment + headless Service (gossip) + ClusterIP Service (HTTP) + Ingress

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

| Table               | Contents                                                  |
|---------------------|-----------------------------------------------------------|
| `account_balances`  | Projection read model — current balance per account       |
| `events_store`      | Raw event log — last 20 events with sequence and manifest |
| `offsets_store`     | Projection progress — current offset per shard            |

Row counts for all three tables are shown at the end.

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

| Target              | Description                                                        |
|---------------------|--------------------------------------------------------------------|
| `make all`          | Full flow: create cluster, build, deploy, wait, test               |
| `make kind-create`  | Create the Kind cluster with ingress port mappings                 |
| `make docker-build` | Build the Docker image and load it into Kind                       |
| `make deploy`       | Apply all Kubernetes manifests                                     |
| `make wait`         | Wait for StatefulSet rollout and ingress readiness                 |
| `make test`         | Run integration tests via ingress (balance check + pod spread)     |
| `make load-test`    | Create 1000 accounts (sequential); report throughput + pod dist    |
| `make db`           | Snapshot PostgreSQL tables (events, offsets, balances) in-cluster  |
| `make grafana`      | Port-forward Grafana to localhost:3000 (admin / admin)             |
| `make jaeger`       | Port-forward Jaeger to localhost:16686                             |
| `make prometheus`   | Port-forward Prometheus to localhost:9090                          |
| `make dashboard`    | Install and open the Kubernetes dashboard at https://localhost:8443 |
| `make reset`        | Truncate all tables for a clean re-run (no teardown needed)        |
| `make status`       | Show all Kubernetes resources in the ego-example namespace         |
| `make logs`         | Tail logs from all app pods                                        |
| `make teardown`     | Delete the Kind cluster and all resources                          |
| `make clean`        | Alias for `teardown`                                               |

## HTTP API

The app exposes the following endpoints on port `8080`:

| Method | Path                    | Description                                              |
|--------|-------------------------|----------------------------------------------------------|
| `GET`  | `/healthz`              | Health and readiness probe                               |
| `POST` | `/accounts/{id}`        | Create an account (body: `{"balance": 1000}`)            |
| `POST` | `/accounts/{id}/credit` | Credit an account (body: `{"amount": 250}`)              |
| `POST` | `/accounts/{id}/debit`  | Debit an account (body: `{"amount": 100}`)               |
| `GET`  | `/accounts/{id}`        | Query account balance from the **projection read table** |

The write-side endpoints (`POST`) send commands to event-sourced entities.
The read-side endpoint (`GET /accounts/{id}`) queries the `account_balances` table, which is populated by the projection handler as it consumes events.

## Project Structure

```
example/cluster/
├── main.go                  # Engine setup, HTTP API, graceful shutdown
├── behavior.go              # AccountBehavior (event-sourced entity)
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
    └── app.yaml             # 3-replica Deployment + headless Service
```

## Load Balancing

All HTTP requests go through the **NGINX Ingress Controller**, which distributes them across the 3 app pods using round-robin. After `make all`, the API is accessible at `http://localhost` — no port-forwarding needed.

The Kind cluster is created with `extraPortMappings` (see `kind-config.yaml`) so that host port 80 maps into the cluster's ingress controller node. The Ingress resource in `k8s/app.yaml` routes all paths (`/`) to the `ego-cluster` ClusterIP service.

A separate headless service (`ego-cluster-headless`) is kept for gossip-based peer discovery — it is not used for HTTP traffic.

## Dependency Isolation

This example is a **separate Go module** (`github.com/tochemey/ego/v4/example/cluster`) with its own `go.mod`.
Heavy dependencies like `k8s.io/client-go`, `github.com/jackc/pgx/v5`, and the OpenTelemetry SDK are confined to this module and do not affect the core eGo library.

## Cleanup

```bash
make teardown
```

This deletes the Kind cluster and all associated resources.
