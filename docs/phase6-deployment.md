# Phase 6 — Deployment

Phase 6 packages the distributed cache for local multi-node deployment with Docker, health checks, and service discovery.

## Architecture

```
Client / loadtest
      │
      ├── TCP :6379  ──► any node (redirects via MOVED)
      │
      └── HTTP :6060 ──► /healthz      (liveness)
                         /metrics      (latency + counters)
                         /discovery    (cluster member list)
                         /debug/pprof/ (profiling)
```

| Component | Purpose |
|-----------|---------|
| `deployments/docker/Dockerfile` | Multi-stage image with `node` + `healthcheck` binaries |
| `deployments/docker/docker-compose.yml` | 3-node cluster with replication (RF=3) and Raft |
| `internal/discovery` | JSON service-discovery snapshot from cluster membership |
| `cmd/healthcheck` | Container health probe binary |
| `internal/config` | Environment variable overrides for container config |

## Service discovery

When cluster mode is enabled and `-debug-addr` is set, each node exposes:

```powershell
curl http://127.0.0.1:6060/discovery
```

Example response:

```json
{
  "self_id": "node-a",
  "nodes": [
    {"id": "node-a", "addr": "node-a:6379"},
    {"id": "node-b", "addr": "node-b:6379"},
    {"id": "node-c", "addr": "node-c:6379"}
  ]
}
```

Clients can query any node's `/discovery` endpoint to learn the full cluster topology. Inside Docker Compose, nodes resolve each other by service name (`node-a`, `node-b`, `node-c`) on the `cache-net` network.

## Environment variables

| Variable | Flag equivalent | Description |
|----------|-----------------|-------------|
| `CACHE_NODE_ID` | `-node-id` | Cluster node identifier |
| `CACHE_ADVERTISE_ADDR` | `-advertise-addr` | Address advertised to peers |
| `CACHE_PEERS` | `-peers` | `id=host:port,id=host:port` peer list |
| `CACHE_DEBUG_ADDR` | `-debug-addr` | Debug HTTP listen address |
| `CACHE_RAFT` | `-raft` | Enable Raft (`true`/`false`) |
| `CACHE_REPLICATION_FACTOR` | `-replication-factor` | Replication factor |
| `CACHE_MAX_BYTES` | `-max-bytes` | Memory limit |

## Docker Compose cluster

From the repository root:

```powershell
docker compose -f deployments/docker/docker-compose.yml up --build
```

Exposed ports:

| Node | Cache TCP | Debug HTTP |
|------|-----------|------------|
| node-a | `127.0.0.1:6379` | `127.0.0.1:6060` |
| node-b | `127.0.0.1:6380` | `127.0.0.1:6061` |
| node-c | `127.0.0.1:6381` | `127.0.0.1:6062` |

### Verify health

```powershell
curl http://127.0.0.1:6060/healthz
curl http://127.0.0.1:6060/discovery
docker compose -f deployments/docker/docker-compose.yml ps
```

### Verify cache operations

```powershell
go run ./cmd/client -addr 127.0.0.1:6379 SET hello world
go run ./cmd/client -addr 127.0.0.1:6379 GET hello
go run ./cmd/loadtest -addr 127.0.0.1:6379 -duration 10s -concurrency 16 `
  -addr-map "node-a:6379=127.0.0.1:6379,node-b:6379=127.0.0.1:6380,node-c:6379=127.0.0.1:6381"
```

### Tear down

```powershell
docker compose -f deployments/docker/docker-compose.yml down
```

## Health checks

Each container runs `/usr/local/bin/healthcheck`, which probes `http://127.0.0.1:6060/healthz`. Docker Compose uses this for `depends_on: condition: service_healthy` so nodes start in order.

Override the probe target with `HEALTHCHECK_URL` if needed.

## Integration tests

```powershell
go test ./tests/integration -run TestDiscovery -v
go test ./... -count=1
```

## Failure modes

- **Peer DNS failure**: ensure all nodes share the Compose network and use service names in `CACHE_PEERS`.
- **Split views during startup**: Compose waits for health checks before starting dependent nodes.
- **Discovery without cluster mode**: `/discovery` is only available when `-node-id` is set and debug HTTP is enabled.
