# Benchmarks

Measured load-test results for the distributed cache. All values come from `scripts/benchmark.ps1` runs on this machine. No numbers are estimated or extrapolated.

## Environment

| Parameter | Value |
|-----------|-------|
| Date | 2026-06-06 19:49:45 |
| OS | Windows 11 Home (Build 26200), x64 |
| CPU | AMD Ryzen 7 7800X3D 8-Core Processor (16 logical processors) |
| RAM | 32 GB |
| Go version | 1.24 |
| Benchmark tool | `loadtest-test.exe` (built from `./cmd/loadtest`) |
| Server binary | `node-test.exe` (built from `./cmd/node`) |

## Workload parameters

| Parameter | Value |
|-----------|-------|
| Duration | 15s |
| Concurrency | 64 workers |
| GET ratio | 80% GET / 20% SET |
| Value size | 64 bytes |
| Key space | 10,000 distinct keys |
| Per-request timeout | 5s |

## Cluster execution mode

Docker Desktop was **not available** during this run. Cluster scenarios used a **local three-node process cluster** with the same settings as Docker Compose:

- **RF=1:** three nodes, `-replication-factor 1`, no Raft (matches `docker-compose.rf1.yml`)
- **RF=3:** three nodes, `-replication-factor 3`, `-raft`, default W=2 R=2 quorums (matches `docker-compose.yml`)

Single-node tests bind to port **16379** to avoid conflict with cluster ports.

Re-run with Docker running to produce Docker-labelled results:

```powershell
.\scripts\benchmark.ps1 -Duration 15s -Concurrency 64
```

Raw JSON: `benchmark-results/20260606-194818/`

## Results

| Scenario | Throughput | p50 | p95 | p99 | Errors |
| --- | --- | --- | --- | --- | --- |
| Single node | 365,824 ops/sec | <1µs | 673µs | 1.51ms | 0 |
| Cluster RF=1 (local 3-node) | 367,326 ops/sec | <1µs | 686µs | 1.51ms | 0 |
| Cluster RF=3 W=2 R=2 (local 3-node) | 357,289 ops/sec | <1µs | 677µs | 1.50ms | 0 |

### Notes on p50

p50 reports as `<1µs` because many request latencies fall below the Windows timer resolution captured in JSON (stored as 0 nanoseconds). p95 and p99 are reliable indicators on this host. Use `-json` output for raw nanosecond values.

### Interpreting cluster vs single-node throughput

Cluster throughput is similar to single-node because:

- Keys are spread across three nodes; each worker follows redirects to the correct owner.
- RF=1 cluster has no replication overhead (consistent hashing only).
- RF=3 adds quorum replication but local loopback latency remains low.

Docker-based runs may show lower throughput due to bridge networking, container overhead, and `-addr-map` redirect hops from the host.

## Reproduce

```powershell
# Full matrix (single node + cluster scenarios)
.\scripts\benchmark.ps1 -Duration 15s -Concurrency 64

# Single scenario manually
go run ./cmd/node -addr :16379
go run ./cmd/loadtest -addr 127.0.0.1:16379 -duration 15s -concurrency 64 -json
```

Docker cluster from host (requires running Compose stack):

```powershell
go run ./cmd/loadtest `
  -addr 127.0.0.1:6379 `
  -duration 15s `
  -concurrency 64 `
  -addr-map "node-a:6379=127.0.0.1:6379,node-b:6379=127.0.0.1:6380,node-c:6379=127.0.0.1:6381" `
  -json
```

## Micro-benchmarks

Package-level Go benchmarks (not end-to-end load tests):

```powershell
go test ./internal/cache -bench=. -benchmem
go test ./internal/protocol -bench=. -benchmem
go test ./internal/server -bench=. -benchmem
```

See [benchmark-validation.md](benchmark-validation.md) for the load-test bug investigation.
