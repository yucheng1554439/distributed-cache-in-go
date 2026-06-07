# Release Checklist

Pre-release verification for the distributed cache open-source release.

## Tests

- [x] Unit tests pass: `go test ./...`
- [x] Integration tests pass: `go test ./tests/integration/... -v`
- [ ] Race detector passes: `go test ./... -race` (requires `CGO_ENABLED=1`; not verified on Windows CI host)
- [x] Load test unit tests pass: `go test ./cmd/loadtest/... -v`

## Deployment

- [ ] Docker image builds: `docker compose -f deployments/docker/docker-compose.yml build`
- [ ] Docker Compose cluster starts healthy: `docker compose -f deployments/docker/docker-compose.yml up --build --wait`
- [ ] Health endpoints respond on all nodes:
  - `curl http://127.0.0.1:6060/healthz`
  - `curl http://127.0.0.1:6061/healthz`
  - `curl http://127.0.0.1:6062/healthz`
- [ ] Service discovery returns three members: `curl http://127.0.0.1:6060/discovery`
- [ ] RF=1 override starts: `docker compose -f deployments/docker/docker-compose.yml -f deployments/docker/docker-compose.rf1.yml up --build --wait`

## Benchmarks

- [x] Load test handles `MOVED` redirects
- [x] Load test handles `NOT_LEADER` redirects
- [x] Timeouts count as errors; workers continue after failures
- [x] Redirect loops detected (max 5 hops)
- [x] Benchmark script completes: `.\scripts\benchmark.ps1 -Duration 15s -Concurrency 64`
- [x] Throughput orders of magnitude above 1 ops/sec (measured ~350k+ ops/sec)
- [x] Results documented in [benchmarks.md](benchmarks.md) with measured values only
- [ ] Docker cluster benchmarks verified (`cluster-rf1-docker` / `cluster-rf3-docker` JSON in `benchmark-results/`)

## Failover

- [ ] Leader failover demo verified per [demo.md](demo.md)
- [ ] Key readable after leader kill
- [ ] New leader elected (`RAFT_STATUS`)

## Observability

- [x] Metrics endpoint returns JSON (`/metrics`, TCP `METRICS`)
- [ ] pprof endpoints verified against running cluster (`/debug/pprof/`, profile, heap)

## Documentation

- [x] [README.md](../README.md) complete
- [x] [architecture.md](architecture.md) — six Mermaid diagrams with explanatory text
- [x] [benchmark-validation.md](benchmark-validation.md) — root cause and fixes
- [x] [benchmarks.md](benchmarks.md) — measured matrix with environment specs
- [x] [demo.md](demo.md) — 11 steps with expected outputs
- [x] [failure-recovery.md](failure-recovery.md)
- [x] [project-summary.md](project-summary.md)
- [x] No fabricated benchmark numbers

## Repository hygiene

- [x] `.gitignore` excludes build artifacts and `benchmark-results/`
- [x] No stale TODO/FIXME markers in source
- [x] Load test and benchmark scripts documented

## Pre-release commands

```powershell
go test ./...
$env:CGO_ENABLED=1; go test ./... -race
go test ./tests/integration/... -v
go test ./cmd/loadtest/... -v
.\scripts\benchmark.ps1 -Duration 15s -Concurrency 64
docker compose -f deployments/docker/docker-compose.yml up --build --wait
curl http://127.0.0.1:6060/healthz
docker compose -f deployments/docker/docker-compose.yml down
```

## Sign-off

| Item | Date | Result |
|------|------|--------|
| Tests | 2026-06-06 | `go test ./...` pass |
| Race detector | | Pending CGO-enabled run |
| Docker Compose | | Pending Docker Desktop availability |
| Benchmarks | 2026-06-06 | Local 3-node validated; see benchmarks.md |
| Failover demo | | Pending Docker verification |
| Docs | 2026-06-06 | Complete |
