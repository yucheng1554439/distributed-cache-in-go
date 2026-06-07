package integration_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/distributed-cache/distributed-cache/internal/cache"
	"github.com/distributed-cache/distributed-cache/internal/cluster"
	"github.com/distributed-cache/distributed-cache/internal/discovery"
	"github.com/distributed-cache/distributed-cache/internal/metrics"
	"github.com/distributed-cache/distributed-cache/internal/server"
)

func startDiscoveryServer(t *testing.T, self cluster.Node, peers []cluster.Node) (debugAddr string, stop func()) {
	t.Helper()

	clusterView, err := cluster.NewCluster(self, peers, cluster.RingConfig{VirtualNodes: 32})
	if err != nil {
		t.Fatalf("NewCluster() error = %v", err)
	}

	store := cache.NewStore(cache.DefaultConfig())
	srv, err := server.New(server.Config{
		Addr:    "127.0.0.1:0",
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		Cluster: clusterView,
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
		if addr := srv.Addr(); addr != "" && addr != "127.0.0.1:0" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	debugAddr, err = metrics.StartDebug(ctx, "127.0.0.1:0", metrics.DebugOptions{
		Discovery: discovery.NewClusterProvider(clusterView),
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		cancel()
		t.Fatalf("StartDebug() error = %v", err)
	}

	return debugAddr, cancel
}

func TestDiscoveryEndpoint(t *testing.T) {
	debugAddr, stop := startDiscoveryServer(t,
		cluster.Node{ID: "node-a", Addr: "node-a:6379"},
		[]cluster.Node{
			{ID: "node-b", Addr: "node-b:6379"},
			{ID: "node-c", Addr: "node-c:6379"},
		},
	)
	defer stop()

	resp, err := http.Get("http://" + debugAddr + "/discovery")
	if err != nil {
		t.Fatalf("GET /discovery error = %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /discovery status = %d, want 200", resp.StatusCode)
	}

	var snap discovery.Snapshot
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if snap.SelfID != "node-a" {
		t.Fatalf("SelfID = %q, want node-a", snap.SelfID)
	}
	if len(snap.Nodes) != 3 {
		t.Fatalf("len(Nodes) = %d, want 3", len(snap.Nodes))
	}
}

func TestHealthcheckBinaryContract(t *testing.T) {
	debugAddr, stop := startDiscoveryServer(t,
		cluster.Node{ID: "node-a", Addr: "node-a:6379"},
		nil,
	)
	defer stop()

	resp, err := http.Get("http://" + debugAddr + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz error = %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /healthz status = %d, want 200", resp.StatusCode)
	}
}
