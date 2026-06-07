# Architecture

Design documentation for the distributed cache: cluster topology, request routing, replication, quorum writes, and Raft consensus.

## High-level cluster architecture

Each node runs three cooperating subsystems on top of an in-memory store:

1. **TCP server** — decodes the wire protocol, routes by hash ring ownership, exposes metrics
2. **Replication manager** — fan-out writes and quorum reads to peer nodes (when RF > 1)
3. **Raft node** — replicates membership log entries (when `-raft` is enabled)

Cache keys are partitioned by consistent hash. Replication places each key on the next *RF* nodes clockwise on the ring. Raft maintains the authoritative member list but does not replicate cache values.

```mermaid
flowchart TB
    subgraph clients [Clients]
        CLI[cmd/client]
        LT[cmd/loadtest]
    end

    subgraph cluster [Cluster]
        subgraph nodeA [node-a]
            SA[server]
            RA[replication]
            AA[raft]
            CA[(cache store)]
            SA --> CA
            SA --> RA
            SA --> AA
        end
        subgraph nodeB [node-b]
            SB[server]
            RB[replication]
            AB[raft]
            CB[(cache store)]
            SB --> CB
            SB --> RB
            SB --> AB
        end
        subgraph nodeC [node-c]
            SC[server]
            RC[replication]
            AC[raft]
            CC[(cache store)]
            SC --> CC
            SC --> RC
            SC --> AC
        end
    end

    CLI -->|TCP :6379| SA
    LT -->|TCP :6379| SA
    RA <-->|REPL_* / PING| RB
    RB <-->|REPL_* / PING| RC
    RA <-->|REPL_* / PING| RC
    AA <-->|AppendEntries / RequestVote| AB
    AB <-->|AppendEntries / RequestVote| AC
    AA <-->|AppendEntries / RequestVote| AC
```

The debug HTTP server (`-debug-addr`) is colocated on each node and serves `/healthz`, `/metrics`, `/discovery`, and Go pprof endpoints independently of cache TCP traffic.

---

## Request routing

Keys map to ring positions via FNV-1a hashing. Each physical node contributes virtual nodes (default 128) to reduce skew. `Owners(key, RF)` walks clockwise to find the primary and replicas.

```mermaid
sequenceDiagram
    participant C as Client
    participant N1 as node-x (receiver)
    participant N2 as node-y (owner)

    C->>N1: GET key
    N1->>N1: ring lookup
    alt key owned locally
        N1->>N1: read store / replication
        N1-->>C: VAL ... / NIL
    else wrong node
        N1-->>C: MOVED owner-id owner-addr
        C->>N2: GET key (follow redirect)
        N2-->>C: VAL ... / NIL
    end
```

| Mode | Write (SET/DEL) | Read (GET) |
|------|-----------------|------------|
| Standalone | Local store | Local store |
| RF=1 cluster | Primary owner; else `MOVED` | Owner; else `MOVED` |
| RF>1 cluster | Primary only; else `MOVED` | Must be in replica set; else `MOVED` to primary |

`OWNER` returns the primary without redirecting. `CLUSTER_MEMBERS` lists ring members. Raft commands (`CLUSTER_JOIN`, `CLUSTER_LEAVE`) require the leader; other nodes return `NOT_LEADER`.

Clients and the load generator follow up to five redirects per request.

---

## Replication flow

When `replication-factor > 1`, each key has a primary (first ring owner) and RF−1 downstream replicas. The primary coordinates all writes and selects read paths based on `read-consistency`.

Default for RF=3: **W=2**, **R=2** (`RF/2 + 1`).

```mermaid
sequenceDiagram
    participant C as Client
    participant P as Primary
    participant R1 as Replica 1
    participant R2 as Replica 2

    Note over C,R2: Read path (read-consistency=quorum, R=2)
    C->>P: GET key
    par quorum read
        P->>P: local read
        P->>R1: REPL_GET key
    end
    P->>P: compare responses
    P-->>C: VAL value (R=2 matching)
```

Read modes:

- **primary** — only the primary serves reads
- **quorum** — parallel reads; majority must agree on value
- **any** — local first, then any healthy peer (may be stale)

Peer health is tracked by periodic `PING`. Unhealthy peers are excluded from quorum.

---

## Quorum write path

Writes always go through the primary. The primary applies locally first, then collects replica acknowledgements before confirming to the client. Failure to reach write quorum triggers a local rollback.

```mermaid
sequenceDiagram
    participant C as Client
    participant P as Primary
    participant R1 as Replica 1
    participant R2 as Replica 2

    C->>P: SET key value
    P->>P: write local store
    par async replication
        P->>R1: REPL_SET key value
        P->>R2: REPL_SET key value
    end
    R1-->>P: OK
    Note over P: wait for write-quorum - 1 replica acks
    alt quorum met (W=2 incl. primary)
        P-->>C: OK
    else quorum not met
        P->>P: rollback local write
        P-->>C: ERR write quorum not met
    end
```

Write consistency modes control `RequiredWriteAcks()`:

| Mode | Replica ACKs required |
|------|---------------------|
| `one` | 0 (primary only) |
| `quorum` | `write-quorum - 1` (default) |
| `all` | `RF - 1` |

Implementation: `internal/replication/manager.go`.

---

## Raft leader election

Raft elects a single leader per term to serialize membership changes. Followers promote to candidate when heartbeats stop, then request votes from a majority.

```mermaid
sequenceDiagram
    participant F1 as Follower 1
    participant F2 as Follower 2 (candidate)
    participant F3 as Follower 3

    Note over F1,F3: Leader stops sending heartbeats
    F2->>F2: election timeout fires
    F2->>F2: increment term, vote for self
    F2->>F1: RequestVote(term, candidate)
    F2->>F3: RequestVote(term, candidate)
    F1-->>F2: vote granted
    F3-->>F2: vote granted
    F2->>F2: become leader
    loop every 50ms
        F2->>F1: AppendEntries (heartbeat)
        F2->>F3: AppendEntries (heartbeat)
    end
```

Election parameters (`internal/raft` defaults):

- Election timeout: 150–300 ms (randomized per follower)
- Heartbeat interval: 50 ms
- Votes required: majority of cluster (`len(peers)/2 + 1`)

Only the leader accepts `CLUSTER_JOIN` and `CLUSTER_LEAVE`. Clients receive `NOT_LEADER` with the known leader address when contacting a follower.

---

## Membership consensus

Committed Raft log entries update the shared hash ring view on every node. Membership and cache data use separate mechanisms.

```mermaid
sequenceDiagram
    participant C as Client
    participant L as Leader
    participant F1 as Follower 1
    participant F2 as Follower 2

    C->>L: CLUSTER_JOIN new-id new-addr
    L->>L: append add_member log entry
    L->>F1: AppendEntries (entry)
    L->>F2: AppendEntries (entry)
    F1-->>L: ACK
    F2-->>L: ACK
    L->>L: commit entry (majority)
    L->>L: apply: ring.Join + AddPeer
    F1->>F1: apply: ring.Join
    F2->>F2: apply: ring.Join
    L-->>C: OK
```

Log command types:

| Type | Trigger |
|------|---------|
| `add_member` | `CLUSTER_JOIN` |
| `remove_member` | `CLUSTER_LEAVE` |
| `noop` | Internal sentinel |

Apply callback (`cmd/node/main.go`): updates `cluster.Join` / `cluster.Leave` and the Raft transport peer map.

---

## Protocol summary

Length-prefixed text protocol (`internal/protocol`). Requests end with `END`.

**Client commands:** `SET`, `GET`, `DEL`, `PING`, `OWNER`, `CLUSTER_MEMBERS`, `CLUSTER_JOIN`, `CLUSTER_LEAVE`, `METRICS`, `RAFT_STATUS`

**Internal replication:** `REPL_SET`, `REPL_DEL`, `REPL_GET`

**Raft RPC:** `RAFT_REQUEST_VOTE`, `RAFT_APPEND_ENTRIES`

**Responses:** `OK`, `VAL`, `NIL`, `ERR`, `MOVED`, `OWNER`, `MEMBERS`, `NOT_LEADER`

---

## Deployment topology

Docker Compose default: three nodes, RF=3, Raft on.

| Service | Cache (host) | Debug (host) |
|---------|--------------|--------------|
| node-a | 6379 | 6060 |
| node-b | 6380 | 6061 |
| node-c | 6381 | 6062 |

Inside the bridge network, peers use `node-{a,b,c}:6379`. Host clients must remap redirect addresses. See [phase6-deployment.md](phase6-deployment.md).

---

## Further reading

- [failure-recovery.md](failure-recovery.md) — failure modes and guarantees
- [benchmarks.md](benchmarks.md) — measured performance
- [project-summary.md](project-summary.md) — interview-oriented overview
