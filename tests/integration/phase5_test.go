package integration_test

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/distributed-cache/distributed-cache/internal/cache"
	"github.com/distributed-cache/distributed-cache/internal/metrics"
	"github.com/distributed-cache/distributed-cache/internal/protocol"
	"github.com/distributed-cache/distributed-cache/internal/server"
)

func startMetricsServer(t *testing.T) (addr string, debugAddr string, stop func()) {
	t.Helper()

	reg := metrics.NewRegistry()
	store := cache.NewStore(cache.DefaultConfig())
	srv, err := server.New(server.Config{
		Addr:    "127.0.0.1:0",
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics: reg,
	}, store)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		_ = srv.ListenAndServe(ctx)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		addr = srv.Addr()
		if addr != "" && addr != "127.0.0.1:0" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if addr == "" || addr == "127.0.0.1:0" {
		cancel()
		t.Fatal("server did not bind")
	}

	debugAddr, err = metrics.StartDebug(ctx, "127.0.0.1:0", metrics.DebugOptions{Registry: reg}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		cancel()
		t.Fatalf("StartDebug() error = %v", err)
	}

	return addr, debugAddr, cancel
}

func TestMetricsCommandAndHTTPEndpoint(t *testing.T) {
	addr, debugAddr, stop := startMetricsServer(t)
	defer stop()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	setReq := protocol.Request{Command: protocol.CommandSet, Key: "metrics-key", Value: []byte("v")}
	if err := writeRequest(conn, setReq); err != nil {
		t.Fatalf("writeRequest(set) error = %v", err)
	}
	if _, err := protocol.DecodeResponse(reader); err != nil {
		t.Fatalf("DecodeResponse(set) error = %v", err)
	}

	getReq := protocol.Request{Command: protocol.CommandGet, Key: "metrics-key"}
	if err := writeRequest(conn, getReq); err != nil {
		t.Fatalf("writeRequest(get) error = %v", err)
	}
	if _, err := protocol.DecodeResponse(reader); err != nil {
		t.Fatalf("DecodeResponse(get) error = %v", err)
	}

	metricsReq := protocol.Request{Command: protocol.CommandMetrics}
	if err := writeRequest(conn, metricsReq); err != nil {
		t.Fatalf("writeRequest(metrics) error = %v", err)
	}
	resp, err := protocol.DecodeResponse(reader)
	if err != nil {
		t.Fatalf("DecodeResponse(metrics) error = %v", err)
	}
	if resp.Kind != protocol.ResponseValue {
		t.Fatalf("metrics response kind = %v", resp.Kind)
	}

	var snap metrics.Snapshot
	if err := json.Unmarshal(resp.Value, &snap); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if snap.Total < 2 {
		t.Fatalf("total = %d, want at least 2", snap.Total)
	}

	httpResp, err := http.Get("http://" + debugAddr + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics error = %v", err)
	}
	defer httpResp.Body.Close()
	if httpResp.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics status = %d", httpResp.StatusCode)
	}

	var httpSnap metrics.Snapshot
	if err := json.NewDecoder(httpResp.Body).Decode(&httpSnap); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if httpSnap.Total < 3 {
		t.Fatalf("http total = %d, want at least 3", httpSnap.Total)
	}

	pprofResp, err := http.Get("http://" + debugAddr + "/debug/pprof/")
	if err != nil {
		t.Fatalf("GET /debug/pprof/ error = %v", err)
	}
	defer pprofResp.Body.Close()
	if pprofResp.StatusCode != http.StatusOK {
		t.Fatalf("GET /debug/pprof/ status = %d, want 200", pprofResp.StatusCode)
	}
}

func writeRequest(conn net.Conn, req protocol.Request) error {
	payload, err := protocol.EncodeRequest(req)
	if err != nil {
		return err
	}
	_, err = conn.Write(payload)
	return err
}
