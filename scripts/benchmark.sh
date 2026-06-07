#!/usr/bin/env bash
# Repeatable load-test benchmarks for distributed-cache.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

DURATION="${DURATION:-15s}"
CONCURRENCY="${CONCURRENCY:-64}"
SKIP_CLUSTER="${SKIP_CLUSTER:-0}"
ADDR_MAP="node-a:6379=127.0.0.1:6379,node-b:6379=127.0.0.1:6380,node-c:6379=127.0.0.1:6381"
COMPOSE_FILE="deployments/docker/docker-compose.yml"
COMPOSE_RF1="deployments/docker/docker-compose.rf1.yml"
TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
RUN_DIR="benchmark-results/$TIMESTAMP"
PEERS="node-a=127.0.0.1:6379,node-b=127.0.0.1:6380,node-c=127.0.0.1:6381"
NODE_BIN="$ROOT/node-test"
LOAD_BIN="$ROOT/loadtest-test"

go build -o "$NODE_BIN" ./cmd/node
go build -o "$LOAD_BIN" ./cmd/loadtest
mkdir -p "$RUN_DIR"

wait_port() {
  local port="$1"
  local timeout="${2:-30}"
  local i=0
  while [ "$i" -lt "$timeout" ]; do
    if (echo >/dev/tcp/127.0.0.1/"$port") >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.25
    i=$((i + 1))
  done
  echo "port $port not ready after ${timeout}s" >&2
  return 1
}

run_loadtest() {
  local name="$1"
  local addr="$2"
  shift 2
  echo
  echo "=== $name ==="
  "$LOAD_BIN" \
    -addr "$addr" \
    -duration "$DURATION" \
    -concurrency "$CONCURRENCY" \
    -get-ratio 0.8 \
    -timeout 5s \
    -json \
    "$@" | tee "$RUN_DIR/$name.json"
}

start_local_cluster() {
  local rf="$1"
  local raft="${2:-0}"
  local pids=()
  for spec in "node-a:6379" "node-b:6380" "node-c:6381"; do
    local id="${spec%%:*}"
    local port="${spec##*:}"
    local args=(-addr ":$port" -node-id "$id" -advertise-addr "127.0.0.1:$port" -peers "$PEERS" -replication-factor "$rf")
    if [ "$raft" = "1" ]; then args+=(-raft); fi
    "$NODE_BIN" "${args[@]}" &
    pids+=("$!")
    sleep 0.75
  done
  wait_port 6379 30
  if [ "$raft" = "1" ]; then sleep 5; fi
  echo "${pids[@]}"
}

stop_pids() {
  for pid in "$@"; do
    kill "$pid" 2>/dev/null || true
  done
}

# A) Single node
"$NODE_BIN" -addr :16379 &
NODE_PID=$!
trap 'kill "$NODE_PID" 2>/dev/null || true' EXIT
wait_port 16379 30
run_loadtest single-node 127.0.0.1:16379
kill "$NODE_PID" 2>/dev/null || true
trap - EXIT

if [ "$SKIP_CLUSTER" = "1" ]; then
  echo "SKIP_CLUSTER=1, skipping cluster benchmarks"
  exit 0
fi

CLUSTER_MODE="local"
if docker info >/dev/null 2>&1; then
  CLUSTER_MODE="docker"
  echo "Docker available; running cluster benchmarks via Compose."
  docker compose -f "$COMPOSE_FILE" -f "$COMPOSE_RF1" down -v >/dev/null 2>&1 || true
  docker compose -f "$COMPOSE_FILE" -f "$COMPOSE_RF1" up -d --build --wait
  wait_port 6379 90
  run_loadtest cluster-rf1-docker 127.0.0.1:6379 -addr-map "$ADDR_MAP"
  docker compose -f "$COMPOSE_FILE" -f "$COMPOSE_RF1" down -v

  docker compose -f "$COMPOSE_FILE" down -v >/dev/null 2>&1 || true
  docker compose -f "$COMPOSE_FILE" up -d --build --wait
  wait_port 6379 120
  sleep 5
  run_loadtest cluster-rf3-docker 127.0.0.1:6379 -addr-map "$ADDR_MAP"
  docker compose -f "$COMPOSE_FILE" down -v
else
  echo "Docker unavailable; running local 3-node cluster with equivalent Compose settings." >&2
  read -r -a PIDS <<< "$(start_local_cluster 1 0)"
  run_loadtest cluster-rf1-local 127.0.0.1:6379
  stop_pids "${PIDS[@]}"

  read -r -a PIDS <<< "$(start_local_cluster 3 1)"
  run_loadtest cluster-rf3-local 127.0.0.1:6379
  stop_pids "${PIDS[@]}"
fi

echo
echo "Results saved to $RUN_DIR (cluster mode: $CLUSTER_MODE)"
