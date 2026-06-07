# Benchmark Validation

This document records the investigation and fix for invalid cluster load-test results observed before release.

## Symptom

Single-node benchmarks looked reasonable (~356k ops/sec, p95 ~779µs, p99 ~1.18ms).

Cluster benchmarks reported exactly **64 operations** regardless of duration:

| Scenario | Concurrency | Operations | Throughput | Latency |
|----------|-------------|------------|------------|---------|
| RF=1 cluster | 64 | 64 | 0.48 ops/sec | ~2 min |
| RF=3 cluster | 64 | 64 | 0.64 ops/sec | ~1.5 min |

The operation count matching concurrency (64) indicated each worker completed roughly one request per run.

## Root cause

Four interacting bugs in the original `cmd/loadtest`:

### 1. No redirect handling

The load generator treated any decodable response as success. When a node returned `MOVED owner-id owner-addr`, the load test recorded a successful operation without contacting the key owner. Cluster traffic therefore never exercised the actual data path for most keys.

### 2. Unreachable redirect targets (Docker)

Docker Compose advertises internal hostnames (`node-b:6379`). Clients running on the host cannot dial those addresses without port mapping. Without remapping, redirects pointed at unreachable targets.

### 3. Blocking without per-request deadlines

The original load test reused one TCP connection per worker but did not set per-request read/write deadlines. When a request stalled (wrong node, partial protocol state, or server waiting on replication), the worker blocked until the **server read timeout** (~30s–2m depending on configuration). With 64 workers each blocked once, the run produced ~64 completions over a 15s–120s window.

### 4. Connection-per-request port exhaustion (intermediate fix)

An intermediate fix opened a new TCP connection for every attempt. That correctly applied dial and I/O timeouts but caused **ephemeral port exhaustion** on Windows at concurrency 64, producing massive error counts and near-zero throughput on single-node runs.

## Where execution stopped

Per worker:

1. Worker started and issued one request on a persistent connection.
2. Server responded with `MOVED` (or the connection blocked on the wrong node).
3. Worker either counted `MOVED` as success **or** blocked until the server read timeout fired.
4. Worker loop continued, but with ~2 minute blocking per iteration only **one** operation completed per 15s benchmark window.
5. After 64 workers each finished one slow operation → **64 total operations**.

Workers did **not** exit permanently after failure; they were blocked in I/O, not crashed.

## Fixes applied

Changes in `cmd/loadtest/main.go`:

| Requirement | Implementation |
|-------------|----------------|
| Workers run full duration | `for ctx.Err() == nil` loop; failures call `continue` |
| MOVED handling | Follow redirect, reconnect to target, retry (max 5 hops) |
| NOT_LEADER handling | Reconnect to leader address from response |
| Timeouts as errors | `SetDeadline(now + timeout)` on each request; I/O errors increment error counter |
| Redirect loop detection | `maxRedirects = 5`; return error after limit |
| Docker hostname mapping | `-addr-map advertised=reachable,...` remaps redirect targets |
| Accurate operation count | Only `OK`, `NOT_FOUND`, `VALUE` count as successful operations |
| Connection efficiency | One reusable connection per worker; reconnect on redirect or error |

Unit tests in `cmd/loadtest/main_test.go` cover addr-map parsing, MOVED/NOT_LEADER redirects, timeouts, redirect loops, and Docker address remapping.

## Verification steps

### 1. Unit tests

```powershell
go test ./cmd/loadtest/... -v
```

Expected: all tests pass, including redirect loop and timeout cases.

### 2. Single-node sanity

```powershell
go build -o node-test.exe ./cmd/node
go build -o loadtest-test.exe ./cmd/loadtest
.\node-test.exe -addr :16379
# separate terminal:
.\loadtest-test.exe -addr 127.0.0.1:16379 -duration 15s -concurrency 64 -json
```

Expected: millions of operations (not 64), throughput in hundreds of thousands ops/sec, low error rate.

### 3. Local three-node cluster

```powershell
.\scripts\benchmark.ps1 -Duration 15s -Concurrency 64
```

Expected: RF=1 and RF=3 scenarios each report millions of operations and 0 errors (local fallback when Docker unavailable).

### 4. Docker cluster (when Docker Desktop is running)

```powershell
docker compose -f deployments/docker/docker-compose.yml up --build --wait
.\scripts\benchmark.ps1 -Duration 15s -Concurrency 64
```

Expected: results saved as `cluster-rf1-docker.json` and `cluster-rf3-docker.json` with `-addr-map` applied.

### 5. Manual redirect check

```powershell
go run ./cmd/client -addr 127.0.0.1:6379 SET testkey value
go run ./cmd/client -addr 127.0.0.1:6380 GET testkey
```

Expected: second command succeeds (client follows `MOVED` internally).

## Post-fix results

After the fix, a 15s run at concurrency 64 reports ~5.3M operations per scenario instead of 64. See [benchmarks.md](benchmarks.md) for measured throughput and latency values.

## Related files

- `cmd/loadtest/main.go` — load generator
- `cmd/loadtest/main_test.go` — validation tests
- `scripts/benchmark.ps1` / `scripts/benchmark.sh` — repeatable benchmark matrix
- `cmd/client/main.go` — reference client redirect handling
