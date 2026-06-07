# Distributed Cache in Go

A distributed in-memory key-value cache with a custom TCP protocol, consistent-hash sharding, quorum replication, and Raft-backed membership management. Built as a self-contained systems project demonstrating routing, consensus, replication, observability, and container deployment.

## Overview

Clients connect over TCP and issue text commands (`SET`, `GET`, `DEL`, and others). A single process can run as a standalone cache or join a cluster. In cluster mode, keys are partitioned across nodes via consistent hashing; optional replication copies each key to multiple ring successors with configurable read and write quorums. Raft handles cluster membership changes (`CLUSTER_JOIN`, `CLUSTER_LEAVE`) independently of cache data replication.

The project targets engineers who want a readable, end-to-end example of how sharding, replication, and consensus fit together—not a Redis drop-in replacement.

## Architecture

```
Client ──TCP──► Node (server → replication → raft → store)
                  │
                  ├── MOVED / NOT_LEADER redirects
                  ├── REPL_* to peer nodes
                  └── HTTP debug: /metrics, /discovery, pprof
```

Each node maintains a hash ring view, an in-memory store with TTL/LRU, and optionally a replication manager and Raft state machine. See [docs/architecture.md](docs/architecture.md) for sequence diagrams and quorum write paths.

## Features

| Area | Capability |
|------|------------|
| Storage | In-memory GET/SET/DEL, TTL expiration, LRU eviction, memory limits |
| Sharding | Consistent hashing, 128 virtual nodes per physical node, `MOVED` redirects |
| Replication | Primary/replica model, tunable RF, W, R, write/read consistency modes |
| Consensus | Raft leader election, log replication, safe membership changes |
| Observability | Per-command p50/p95/p99, HTTP and TCP metrics, Go pprof |
| Deployment | Docker multi-stage build, Compose 3-node stack, health checks, discovery |
| Tooling | CLI client, load generator, repeatable benchmark scripts |

## Consistent Hashing

Keys are hashed (FNV-1a) onto a ring with virtual nodes to reduce skew. `Owners(key, n)` returns the next *n* distinct nodes clockwise— the first is the **primary**, the rest are **replicas**.

When a client contacts a non-owner, the server responds:

```
MOVED <node-id> <advertised-addr>
```

Clients and the load generator follow redirects (up to five hops). From outside Docker, remap advertised hostnames with `-addr-map` or learn topology from `/discovery`.

**Tradeoff:** Adding or removing nodes remaps only adjacent key ranges, but the ring does not auto-rebalance existing data.

## Replication

Enabled when `-replication-factor > 1`. Default for a three-node cluster: RF=3, W=2, R=2.

- **Writes:** Primary writes locally, fans out `REPL_SET`/`REPL_DEL`, waits for quorum acks, rolls back on failure.
- **Reads:** Configurable—quorum (majority must agree), primary-only, or any healthy replica.
- **Health:** Periodic `PING`; unhealthy peers excluded from quorum.

**Tradeoff:** Quorum improves consistency during partial failure but reduces write availability when too many replicas are down.

## Raft Consensus

Raft manages **who is in the cluster**, not cache values. The leader serializes `CLUSTER_JOIN` and `CLUSTER_LEAVE` through a replicated log; followers apply committed entries to update the hash ring.

- Election timeout: 150–300 ms
- Heartbeat: 50 ms
- Non-leaders respond with `NOT_LEADER <leader-id> <addr>`

**Tradeoff:** Separating membership consensus from data replication simplifies each layer but requires reasoning about two failure domains.

## Observability

Enable the debug HTTP server:

```powershell
go run ./cmd/node -addr :6379 -debug-addr 127.0.0.1:6060
```

| Endpoint | Purpose |
|----------|---------|
| `/healthz` | Liveness |
| `/metrics` | JSON latency histograms and counters |
| `/discovery` | Cluster member list (cluster mode) |
| `/debug/pprof/*` | CPU, heap, trace profiles |

TCP alternative: `go run ./cmd/client -addr 127.0.0.1:6379 METRICS`

## Quick Start

Requires Go 1.24+.

```powershell
# Terminal 1
go run ./cmd/node -addr :6379

# Terminal 2
go run ./cmd/client -addr 127.0.0.1:6379 SET mykey myvalue
go run ./cmd/client -addr 127.0.0.1:6379 GET mykey
```

Expected:

```
OK
VALUE myvalue
```

## Docker Deployment

```powershell
docker compose -f deployments/docker/docker-compose.yml up --build
```

| Node | Cache | Debug |
|------|-------|-------|
| node-a | `127.0.0.1:6379` | `127.0.0.1:6060` |
| node-b | `127.0.0.1:6380` | `127.0.0.1:6061` |
| node-c | `127.0.0.1:6381` | `127.0.0.1:6062` |

Sharding-only (RF=1, no Raft):

```powershell
docker compose -f deployments/docker/docker-compose.yml `
  -f deployments/docker/docker-compose.rf1.yml up --build
```

Verify:

```powershell
curl http://127.0.0.1:6060/healthz
curl http://127.0.0.1:6060/discovery
```

Tear down: `docker compose -f deployments/docker/docker-compose.yml down`

Full walkthrough: [docs/demo.md](docs/demo.md)

## Benchmark Results

Run the benchmark matrix:

```powershell
.\scripts\benchmark.ps1 -Duration 15s -Concurrency 64
```

Measured 2026-06-06 on AMD Ryzen 7 7800X3D / 32 GB RAM / Windows 11. Workload: 15s, 64 workers, 80% GET, 64-byte values. Cluster scenarios used a local 3-node process cluster (Docker unavailable); settings match Compose files.

| Scenario | Throughput | p50 | p95 | p99 | Errors |
| --- | --- | --- | --- | --- | --- |
| Single node | 365,824 ops/sec | <1µs | 673µs | 1.51ms | 0 |
| Cluster RF=1 (local 3-node) | 367,326 ops/sec | <1µs | 686µs | 1.51ms | 0 |
| Cluster RF=3 W=2 R=2 (local 3-node) | 357,289 ops/sec | <1µs | 677µs | 1.50ms | 0 |

Full environment details and reproduction steps: [docs/benchmarks.md](docs/benchmarks.md). Load-test bug investigation: [docs/benchmark-validation.md](docs/benchmark-validation.md).

## Failure Recovery

| Event | Behavior |
|-------|----------|
| Raft leader crash | New leader elected in ~150–300 ms; cache ops continue |
| Replica crash | Excluded from quorum; writes succeed if W still met |
| Quorum loss | Writes roll back; quorum reads fail rather than return stale data |
| Node restart | In-memory data lost; replicated keys rebuilt on subsequent writes |
| Redirect to unreachable addr | Use `-addr-map` or `/discovery` from host |

Detailed analysis: [docs/failure-recovery.md](docs/failure-recovery.md)

## Development

```powershell
go test ./...
go test ./tests/integration/... -v
go test ./internal/cache -bench=. -benchmem
```

| Path | Role |
|------|------|
| `cmd/node` | Server entrypoint |
| `cmd/client` | CLI client |
| `cmd/loadtest` | Load generator |
| `internal/cache` | Store, TTL, LRU |
| `internal/cluster` | Hash ring |
| `internal/replication` | Quorum I/O |
| `internal/raft` | Membership consensus |
| `internal/server` | TCP dispatch |
| `deployments/docker` | Container assets |

Enable request tracing: set `CACHE_LOG_LEVEL=debug` or use `server.EnableRequestTrace` in tests.

## Testing

```powershell
# Unit + integration
go test ./...

# Integration only
go test ./tests/integration/... -v

# Load test validation
go test ./cmd/loadtest/... -v

# Race detector (requires CGO_ENABLED=1)
go test ./... -race
```

Integration tests cover metrics, replication, Raft, Docker configuration, and multi-phase cluster behavior under `tests/integration/`.

## Future Work

- Persistent storage with snapshot/recovery
- TLS and authentication
- Slot migration and automatic rebalancing
- Prometheus metrics export
- Client library with topology-aware routing
- Partition tolerance testing and read repair

## Documentation

| Document | Description |
|----------|-------------|
| [architecture.md](docs/architecture.md) | Diagrams and data flows |
| [benchmarks.md](docs/benchmarks.md) | Measured performance |
| [benchmark-validation.md](docs/benchmark-validation.md) | Load-test bug postmortem |
| [demo.md](docs/demo.md) | Cluster walkthrough |
| [failure-recovery.md](docs/failure-recovery.md) | Failure modes |
| [project-summary.md](docs/project-summary.md) | Interview reference |
| [release-checklist.md](docs/release-checklist.md) | Pre-release verification |
