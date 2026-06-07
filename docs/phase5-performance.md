# Phase 5 — Performance

This phase adds observability and performance tooling for the distributed cache.

## Metrics

Each node can collect per-command latency samples and expose them in two ways:

- **TCP `METRICS` command** — returns a JSON snapshot of counters and p50/p95/p99 latencies
- **HTTP `/metrics`** — same JSON snapshot when the debug server is enabled

Enable metrics collection by starting a node with the debug server (metrics are always collected when `-debug-addr` is set):

```powershell
go run ./cmd/node -debug-addr 127.0.0.1:6060
```

Query metrics over TCP:

```powershell
go run ./cmd/client -addr 127.0.0.1:6379 METRICS
```

Query metrics over HTTP:

```powershell
curl http://127.0.0.1:6060/metrics
```

## Profiling

The debug server also exposes Go pprof endpoints for CPU and memory profiling:

```powershell
go tool pprof http://127.0.0.1:6060/debug/pprof/profile?seconds=30
go tool pprof http://127.0.0.1:6060/debug/pprof/heap
```

## Benchmarks

Run micro-benchmarks for the cache store, protocol codec, and server dispatch path:

```powershell
go test ./internal/cache -bench=. -benchmem
go test ./internal/protocol -bench=. -benchmem
go test ./internal/server -bench=. -benchmem
```

## Load testing

The load generator runs concurrent GET/SET traffic and reports throughput plus p50/p95/p99 latency:

```powershell
# Start a node in one terminal
go run ./cmd/node

# Run load test in another terminal
go run ./cmd/loadtest -addr 127.0.0.1:6379 -duration 15s -concurrency 64 -get-ratio 0.8
```

For Docker cluster tests from the host, follow `MOVED` redirects by remapping advertised addresses:

```powershell
go run ./cmd/loadtest `
  -addr 127.0.0.1:6379 `
  -duration 15s `
  -concurrency 64 `
  -addr-map "node-a:6379=127.0.0.1:6379,node-b:6379=127.0.0.1:6380,node-c:6379=127.0.0.1:6381"
```

Repeatable benchmark matrix: `.\scripts\benchmark.ps1`

Use `-json` for machine-readable output.

## Integration tests

```powershell
go test ./tests/integration -run TestMetrics -v
```
