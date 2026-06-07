# Project Summary

A concise reference for explaining this distributed cache project in technical interviews and on a resume.

## One-line description

Go implementation of a sharded in-memory cache with consistent hashing, quorum replication, Raft membership management, observability tooling, and Docker deployment.

## Architecture overview

```
Client → TCP server → [routing | replication | raft] → in-memory store
                              ↓
                     peer nodes (REPL_*, Raft RPC)
```

Three operational modes:

1. **Standalone** — single-node cache, no routing
2. **Sharded cluster (RF=1)** — consistent hash ring, `MOVED` redirects
3. **Replicated cluster (RF>1) + optional Raft** — quorum reads/writes, leader-elected membership changes

## Key technical decisions

| Decision | Rationale | Tradeoff |
|----------|-----------|----------|
| Custom TCP text protocol | Simple to debug, minimal dependencies | Not compatible with Redis clients |
| Consistent hashing + vnodes | Even key distribution, bounded remapping on join/leave | Ring rebalance not automatic |
| Primary/replica replication | Straightforward write path; primary coordinates quorum | Primary hotspot for writes |
| Quorum reads/writes (default W=2, R=2) | Balance consistency and fault tolerance | Reduced availability during partial outage |
| Separate Raft for membership | Avoids coupling data replication with consensus | Two distributed subsystems to reason about |
| In-memory storage | Low latency, portfolio-appropriate scope | No durability across restarts |
| HTTP debug server for metrics/pprof | Standard Go observability tooling | Extra port to secure in production |

## Distributed systems concepts demonstrated

- **Consistent hashing** — key-to-node mapping with virtual nodes
- **Leader-based replication** — primary coordinates fan-out to replicas
- **Quorum consensus (replication)** — configurable W and R acks
- **Raft** — leader election, log replication, membership apply callback
- **Redirect-based routing** — `MOVED` / `NOT_LEADER` client-side follow
- **Failure detection** — periodic peer health checks
- **Observability** — latency histograms, pprof, load testing
- **Container deployment** — multi-stage Dockerfile, Compose, health probes

## Scalability considerations

**What scales**

- Horizontal sharding via adding nodes to the hash ring (membership change required)
- Read throughput with `read-consistency=any` or spreading clients across nodes
- Independent per-node request handling for keys owned locally

**Current bottlenecks**

- Primary node handles all writes for its key range
- Synchronous quorum replication adds latency vs fire-and-forget
- Single TCP connection per client in the CLI (load test reuses one connection per worker)
- No persistence layer; memory bound per node (`-max-bytes`)
- Raft and replication share the cache TCP port

**Production gaps (intentionally out of scope)**

- TLS, authentication, rate limiting
- Automatic rebalancing / slot migration
- Persistent storage and snapshot recovery
- Prometheus exporter, structured tracing
- Chaos testing and formal Jepsen-style verification

## Failure handling

| Component | Failure behavior |
|-----------|------------------|
| Raft leader | Re-election in ~150–300 ms; cache ops continue |
| Replica node | Excluded from quorum; writes succeed if W met |
| Primary node | Keys in its range unavailable until ring reroutes (no automatic failover of primaries) |
| Network partition | Partial writes blocked by quorum; no split-brain fencing |

See [failure-recovery.md](failure-recovery.md) for full detail.

## Benchmark credibility

Cluster benchmarks previously reported 64 operations (one per worker) due to load-test bugs. After fixing redirect handling, connection reuse, and timeouts, cluster throughput matches single-node order of magnitude (~350k+ ops/sec local). See [benchmark-validation.md](benchmark-validation.md).

## Interview discussion topics

**"Walk me through a write."**

Client sends `SET` to any node → server checks primary ownership → if not primary, `MOVED` → primary writes locally → parallel `REPL_SET` to replicas → waits for W-1 acks → returns `OK` or rolls back.

**"How does membership change work?"**

Client sends `CLUSTER_JOIN` to Raft leader → leader appends log entry → replicates to majority → apply callback updates hash ring and Raft peer map → new node receives traffic for its key ranges.

**"What happens when the leader dies?"**

Raft followers timeout and elect a new leader. Cache reads/writes continue on surviving nodes with quorum. Membership commands redirect via `NOT_LEADER` until election completes.

**"What would you add for production?"**

Persistence, TLS, connection pooling, metrics export, slot migration, read repair, integration tests under partition, and client SDK with smart routing (cache ring locally from `/discovery`).

## Repository map

| Area | Package / path |
|------|----------------|
| Wire protocol | `internal/protocol` |
| Storage | `internal/cache` |
| Sharding | `internal/cluster` |
| Replication | `internal/replication` |
| Consensus | `internal/raft` |
| Request path | `internal/server` |
| Metrics | `internal/metrics` |
| Deployment | `deployments/docker` |

## Related documentation

- [architecture.md](architecture.md) — diagrams and data flows
- [benchmarks.md](benchmarks.md) — measured performance
- [demo.md](demo.md) — hands-on cluster walkthrough
- [failure-recovery.md](failure-recovery.md) — failure modes and guarantees
