package metrics

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/distributed-cache/distributed-cache/internal/cluster"
	"github.com/distributed-cache/distributed-cache/internal/discovery"
)

func TestDebugMuxPProfIndex(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	debugAddr, err := StartDebug(ctx, "127.0.0.1:0", DebugOptions{Registry: NewRegistry()}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("StartDebug() error = %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + debugAddr + "/debug/pprof/")
		if err == nil {
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				body, readErr := io.ReadAll(resp.Body)
				if readErr != nil {
					t.Fatalf("ReadAll() error = %v", readErr)
				}
				if len(body) == 0 {
					t.Fatal("expected non-empty pprof index body")
				}
				return
			}
			t.Fatalf("GET /debug/pprof/ status = %d, want 200", resp.StatusCode)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("GET /debug/pprof/ did not succeed against %s", debugAddr)
}

func TestDebugMuxDiscovery(t *testing.T) {
	t.Parallel()

	clusterView, err := cluster.NewCluster(
		cluster.Node{ID: "node-a", Addr: "node-a:6379"},
		[]cluster.Node{{ID: "node-b", Addr: "node-b:6379"}},
		cluster.RingConfig{VirtualNodes: 8},
	)
	if err != nil {
		t.Fatalf("NewCluster() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	debugAddr, err := StartDebug(ctx, "127.0.0.1:0", DebugOptions{
		Discovery: discovery.NewClusterProvider(clusterView),
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("StartDebug() error = %v", err)
	}

	resp, err := http.Get("http://" + debugAddr + "/discovery")
	if err != nil {
		t.Fatalf("GET /discovery error = %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /discovery status = %d, want 200", resp.StatusCode)
	}
}

func TestRegisterPProfRoutes(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	registerPProf(mux)

	server := &http.Server{Handler: mux}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	defer listener.Close()

	go func() {
		_ = server.Serve(listener)
	}()
	defer server.Close()

	for _, path := range []string{
		"/debug/pprof/",
		"/debug/pprof/cmdline",
		"/debug/pprof/profile?seconds=1",
	} {
		resp, err := http.Get("http://" + listener.Addr().String() + path)
		if err != nil {
			t.Fatalf("GET %s error = %v", path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s status = %d, want 200", path, resp.StatusCode)
		}
	}
}
