package integration_test

import (
	"context"
	"io"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/distributed-cache/distributed-cache/internal/cache"
	"github.com/distributed-cache/distributed-cache/internal/cluster"
	"github.com/distributed-cache/distributed-cache/internal/server"
)

func startClusterNode(t *testing.T, node cluster.Node, peers []cluster.Node) (addr string, stop func()) {
	t.Helper()

	clusterView, err := cluster.NewCluster(node, peers, cluster.RingConfig{VirtualNodes: 64})
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
	done := make(chan error, 1)
	go func() {
		done <- srv.ListenAndServe(ctx)
	}()

	deadline := time.Now().Add(2 * time.Second)
	var bound string
	for time.Now().Before(deadline) {
		bound = srv.Addr()
		if bound != "" && bound != "127.0.0.1:0" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if bound == "" || bound == "127.0.0.1:0" {
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
	return bound, stop
}

func TestClusterRedirectAndLocalWrite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	addrA, stopA := startClusterNode(t, cluster.Node{ID: "node-a", Addr: "127.0.0.1:0"}, nil)
	defer stopA()

	addrB, stopB := startClusterNode(t, cluster.Node{ID: "node-b", Addr: "127.0.0.1:0"}, []cluster.Node{
		{ID: "node-a", Addr: addrA},
	})
	defer stopB()

	clientPath := buildClient(t)
	runClientWithFlags(t, clientPath, addrA, []string{"CLUSTER_JOIN", "node-b", addrB}, "OK\n")

	clusterView, err := cluster.NewCluster(
		cluster.Node{ID: "node-a", Addr: addrA},
		[]cluster.Node{{ID: "node-b", Addr: addrB}},
		cluster.RingConfig{VirtualNodes: 64},
	)
	if err != nil {
		t.Fatalf("NewCluster() error = %v", err)
	}

	var keyA string
	var keyB string
	for i := 0; i < 4096; i++ {
		key := "cluster-key-" + strconv.Itoa(i)
		owner, ok := clusterView.Owner(key)
		if !ok {
			continue
		}
		switch owner.ID {
		case "node-a":
			if keyA == "" {
				keyA = key
			}
		case "node-b":
			if keyB == "" {
				keyB = key
			}
		}
		if keyA != "" && keyB != "" {
			break
		}
	}
	if keyA == "" || keyB == "" {
		t.Fatal("could not find sample keys for both nodes")
	}

	runClientWithFlags(t, clientPath, addrA, []string{"SET", keyB, "remote"}, "OK\n")
	runClientWithFlags(t, clientPath, addrB, []string{"GET", keyB}, "VALUE remote\n")

	runClientWithFlags(t, clientPath, addrA, []string{"SET", keyA, "local"}, "OK\n")
	runClientWithFlags(t, clientPath, addrA, []string{"GET", keyA}, "VALUE local\n")
	runClientWithFlags(t, clientPath, addrA, []string{"OWNER", keyA}, "OWNER node-a "+addrA+"\n")
}
