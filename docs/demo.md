# Demo Walkthrough

Hands-on commands for a three-node Docker cluster with RF=3 replication and Raft. Run from the repository root.

Port map: node-a → 6379/6060, node-b → 6380/6061, node-c → 6381/6062.

---

## 1. Cluster startup

```powershell
docker compose -f deployments/docker/docker-compose.yml up --build -d
docker compose -f deployments/docker/docker-compose.yml ps
```

Expected: all three services `running` with `(healthy)` after the start period.

```
NAME                          STATUS
distributed-cache-node-a-1    Up (healthy)
distributed-cache-node-b-1    Up (healthy)
distributed-cache-node-c-1    Up (healthy)
```

---

## 2. Verifying health

```powershell
curl http://127.0.0.1:6060/healthz
curl http://127.0.0.1:6061/healthz
curl http://127.0.0.1:6062/healthz
```

Expected (each endpoint):

```
ok
```

---

## 3. Verifying discovery

```powershell
curl http://127.0.0.1:6060/discovery
```

Expected: JSON with `self_id` and three nodes:

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

---

## 4. Writing data

Connect to any node; the server redirects to the primary if needed.

```powershell
go run ./cmd/client -addr 127.0.0.1:6379 SET demo:key "hello from cluster"
```

Expected:

```
OK
```

---

## 5. Reading data

```powershell
go run ./cmd/client -addr 127.0.0.1:6379 GET demo:key
go run ./cmd/client -addr 127.0.0.1:6380 GET demo:key
```

Expected (both):

```
VALUE hello from cluster
```

The client follows `MOVED` redirects automatically when the receiving node is not the owner.

---

## 6. Determining leader

Query Raft status on each node:

```powershell
go run ./cmd/client -addr 127.0.0.1:6379 RAFT_STATUS
go run ./cmd/client -addr 127.0.0.1:6380 RAFT_STATUS
go run ./cmd/client -addr 127.0.0.1:6381 RAFT_STATUS
```

Follower example:

```
RAFT_STATUS {"node_id":"node-b","state":"follower","term":2,"leader_id":"node-a",...}
```

Leader example:

```
RAFT_STATUS {"node_id":"node-a","state":"leader","term":2,"leader_id":"node-a",...}
```

Note the leader's node ID and corresponding host port before proceeding.

---

## 7. Killing leader

Stop the leader container (replace `node-a` if your leader differs):

```powershell
docker compose -f deployments/docker/docker-compose.yml stop node-a
```

Expected: `docker compose ps` shows node-a stopped; node-b and node-c still healthy.

---

## 8. Observing election

Poll remaining nodes until one reports `"state":"leader"` with an incremented term:

```powershell
go run ./cmd/client -addr 127.0.0.1:6380 RAFT_STATUS
go run ./cmd/client -addr 127.0.0.1:6381 RAFT_STATUS
```

Expected within ~300 ms: one node returns `"state":"leader"` and a new `leader_id`.

---

## 9. Reading data after failover

Cache operations do not require the Raft leader. Quorum-replicated data survives leader loss:

```powershell
go run ./cmd/client -addr 127.0.0.1:6380 GET demo:key
```

Expected:

```
VALUE hello from cluster
```

---

## 10. Viewing metrics

HTTP:

```powershell
curl http://127.0.0.1:6061/metrics
```

TCP:

```powershell
go run ./cmd/client -addr 127.0.0.1:6380 METRICS
```

Expected: JSON with `total_requests`, `total_errors`, and per-command latency percentiles (p50/p95/p99). Values depend on traffic sent during the demo.

Example structure:

```json
{
  "generated_at": "...",
  "total_requests": 42,
  "total_errors": 0,
  "commands": [
    {
      "command": "GET",
      "requests": 20,
      "errors": 0,
      "latency": {"p50": "...", "p95": "...", "p99": "..."}
    }
  ]
}
```

---

## 11. Running pprof

Profile index:

```powershell
curl http://127.0.0.1:6061/debug/pprof/
```

CPU profile (30 seconds, interactive analysis):

```powershell
go tool pprof http://127.0.0.1:6061/debug/pprof/profile?seconds=30
```

Heap profile:

```powershell
go tool pprof http://127.0.0.1:6061/debug/pprof/heap
```

At the `pprof` prompt, try `top`, `web`, or `quit`.

---

## Load test from host

When running load tests against Docker from the host, remap advertised addresses:

```powershell
go run ./cmd/loadtest `
  -addr 127.0.0.1:6379 `
  -duration 15s `
  -concurrency 64 `
  -addr-map "node-a:6379=127.0.0.1:6379,node-b:6379=127.0.0.1:6380,node-c:6379=127.0.0.1:6381"
```

Expected: millions of operations over 15s, not 64. See [benchmark-validation.md](benchmark-validation.md).

---

## Restore stopped node

```powershell
docker compose -f deployments/docker/docker-compose.yml start node-a
go run ./cmd/client -addr 127.0.0.1:6379 RAFT_STATUS
```

Expected: node-a returns `"state":"follower"` and catches up on the Raft log.

---

## Cleanup

```powershell
docker compose -f deployments/docker/docker-compose.yml down
```
