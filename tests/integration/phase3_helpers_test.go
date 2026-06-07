package integration_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/distributed-cache/distributed-cache/internal/cache"
	"github.com/distributed-cache/distributed-cache/internal/cluster"
	"github.com/distributed-cache/distributed-cache/internal/replication"
	"github.com/distributed-cache/distributed-cache/internal/server"
)

func startReplicatedNode(t *testing.T, self cluster.Node, peers []cluster.Node, replCfg replication.Config) (addr string, stop func()) {
	t.Helper()

	store := cache.NewStore(cache.DefaultConfig())
	clusterView, err := cluster.NewCluster(self, peers, cluster.RingConfig{VirtualNodes: 64})
	if err != nil {
		t.Fatalf("NewCluster() error = %v", err)
	}

	replManager, err := replication.NewManager(replCfg, clusterView, store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	srv, err := server.New(server.Config{
		Addr:        "127.0.0.1:0",
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Cluster:     clusterView,
		Replication: replManager,
	}, store)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- srv.ListenAndServe(ctx)
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

	stop = func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Log("server shutdown timed out")
		}
	}
	return addr, stop
}
