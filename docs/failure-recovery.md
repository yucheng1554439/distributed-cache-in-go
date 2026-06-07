# Failure Recovery

This document describes how the distributed cache behaves under node and network failures, what consistency guarantees apply, and known limitations.

## Consistency model summary

| Layer | Scope | Guarantee |
|-------|-------|-----------|
| Cache data (RF>1) | Key/value operations | Quorum writes (default W=2); quorum reads (default R=2) when `read-consistency=quorum` |
| Cache data (RF=1) | Key/value operations | Single primary owner; no cross-node replication |
| Membership | Cluster topology | Raft-majority committed join/leave operations |
| Process storage | All modes | In-memory only; restart clears local data |

Raft manages **membership** (who is in the hash ring). It does **not** replicate cache entries. Data durability depends on replication factor and quorum settings.

---

## Leader crash (Raft)

**What happens**

1. Raft leader stops sending heartbeats (process crash, container stop, network isolation).
2. Followers exceed the election timeout (150–300 ms randomized).
3. A follower becomes candidate, requests votes, and becomes leader on majority approval.
4. Clients issuing `CLUSTER_JOIN` or `CLUSTER_LEAVE` to a non-leader receive `NOT_LEADER <leader-id> <leader-addr>` until a new leader is elected.

**Cache impact**

- `GET`, `SET`, and `DEL` do **not** require the Raft leader. They use the hash ring and replication manager.
- Data written with quorum before the crash remains readable from surviving replicas.

**Client behavior**

- `cmd/client` and `cmd/loadtest` follow `NOT_LEADER` redirects automatically (up to 5 hops).
- Membership changes may fail briefly during election; retry after a few hundred milliseconds.

**Tradeoff**

- Availability of membership changes is tied to Raft leader availability.
- Cache operations remain available if enough replication peers are healthy.

---

## Follower crash

**Raft**

- The cluster continues with a leader and remaining followers.
- A crashed follower rejoins by catching up on the Raft log when it restarts (if it retains state; this implementation uses in-memory Raft state).

**Replication**

- The crashed node is marked **unhealthy** by periodic `PING` checks (default 5s interval).
- Quorum calculations exclude unhealthy peers.
- With RF=3 and W=2, one replica loss still allows writes if the primary and one replica are healthy.

**Reads**

- Quorum reads require R matching responses from healthy replica-set members.
- If too few healthy replicas exist, reads may fail or return errors depending on consistency mode.

---

## Replica failure (non-Raft)

When a replica node handling cache data fails but Raft continues:

1. Primary still accepts writes if it can reach enough replicas for write quorum.
2. Failed replica is skipped until health checks mark it unhealthy.
3. On recovery, the replica has **stale or empty** local state until new writes replicate to it.

There is no automatic anti-entropy or read repair in the current implementation. A long-lived failed replica may serve stale data if reached with `read-consistency=any`.

---

## Quorum loss

Write quorum loss occurs when the primary cannot obtain enough replica acknowledgements:

```
RequiredWriteAcks = write-quorum - 1   (quorum consistency mode)
```

With RF=3, W=2: primary needs 1 replica ACK. If both replicas are unhealthy, writes fail and the primary **rolls back** the local write.

Read quorum loss occurs when fewer than R healthy replicas return matching values. Quorum reads fail rather than return ambiguous data.

**Availability tradeoff**

- Higher RF and quorum values improve fault tolerance for data but reduce write availability during partial outages.
- `write-consistency=one` maximizes write availability at the cost of durability on replica loss.
- `read-consistency=any` maximizes read availability at the cost of potentially stale reads.

---

## Network partition limitations

This implementation does **not** fully solve the partition tolerance tradeoffs of distributed systems:

| Scenario | Behavior |
|----------|----------|
| Client isolated from owner | Requests fail or time out; client must retry |
| Split cluster (Raft) | Minority partition cannot elect a leader or commit membership changes |
| Split cluster (replication) | Each side may diverge if both sides accept writes without quorum overlap |
| Docker host vs bridge network | Redirect addresses may be unreachable without `-addr-map` |

There is no fencing, version vectors, or conflict resolution for concurrent writes to the same key from partitioned clients.

**Known limitation:** During a partition, a minority side should not serve quorum writes; the current implementation relies on peer health checks and TCP failures rather than explicit partition detection.

---

## Node rejoin behavior

When a stopped node returns:

1. **Raft:** Rejoining node receives `AppendEntries` from the leader and applies missed log entries. Membership view converges to the committed log.
2. **Cache store:** Local in-memory store starts empty. Previously held keys are not automatically synced.
3. **Replication:** New writes to keys owned by the rejoined node replicate via normal `REPL_SET` traffic.
4. **Health checks:** After successful `PING`, the node re-enters the healthy peer set for quorum.

**Operational note:** A node that was down for an extended period may temporarily reduce quorum capacity until it receives replicated writes for its key ranges.

---

## Process crash and restart

All cache data is in memory. Restarting any node clears its local store.

| Mode | Effect |
|------|--------|
| Standalone | All data lost |
| RF=1 cluster | Keys owned by restarted node are unavailable until rewritten |
| RF>1 cluster | Keys replicated to surviving nodes remain available; restarted node rebuilds from subsequent writes |

There is no snapshot, WAL, or AOF persistence.

---

## Redirect and discovery failures

**MOVED to unreachable address**

- Common when connecting from the Docker host without address remapping.
- Fix: `-addr-map node-a:6379=127.0.0.1:6379,...` or query `/discovery` for topology.

**Health endpoint down**

- Container orchestrators mark the node unhealthy via `cmd/healthcheck` against `/healthz`.
- Other nodes exclude it from quorum after replication health checks fail.

---

## Recommended recovery procedures

| Failure | Action |
|---------|--------|
| Raft leader lost | Wait for election (~300 ms); retry membership commands |
| Replica lost | Verify surviving nodes serve reads/writes; restart failed node |
| Quorum write failures | Check peer health; reduce load; ensure at least W nodes healthy |
| Stale data suspected | Rewrite keys; use `read-consistency=quorum` |
| Full cluster restart | Restart Compose stack; re-seed data; verify `/discovery` shows 3 members |

See [demo.md](demo.md) for a hands-on leader failover walkthrough.
