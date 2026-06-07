package server

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/distributed-cache/distributed-cache/internal/cache"
	"github.com/distributed-cache/distributed-cache/internal/cluster"
	"github.com/distributed-cache/distributed-cache/internal/protocol"
	"github.com/distributed-cache/distributed-cache/internal/replication"
)

func startReplicatedServer(t *testing.T, self cluster.Node, peers []cluster.Node, replCfg replication.Config) (addr string, stop func()) {
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

	srv, err := New(Config{
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

func TestServerReplicationWriteAndRead(t *testing.T) {
	replCfg := replication.Config{
		ReplicationFactor: 3,
		WriteQuorum:       2,
		ReadQuorum:        1,
		WriteConsistency:  replication.WriteQuorum,
		ReadConsistency:   replication.ReadAny,
	}

	addrA, stopA := startReplicatedServer(t, cluster.Node{ID: "node-a", Addr: "127.0.0.1:0"}, nil, replCfg)
	defer stopA()

	addrB, stopB := startReplicatedServer(t, cluster.Node{ID: "node-b", Addr: "127.0.0.1:0"}, []cluster.Node{
		{ID: "node-a", Addr: addrA},
	}, replCfg)
	defer stopB()

	addrC, stopC := startReplicatedServer(t, cluster.Node{ID: "node-c", Addr: "127.0.0.1:0"}, []cluster.Node{
		{ID: "node-a", Addr: addrA},
		{ID: "node-b", Addr: addrB},
	}, replCfg)
	defer stopC()

	joinNode(t, addrA, "node-b", addrB)
	joinNode(t, addrA, "node-c", addrC)
	joinNode(t, addrB, "node-c", addrC)

	clusterView, err := cluster.NewCluster(
		cluster.Node{ID: "node-a", Addr: addrA},
		[]cluster.Node{
			{ID: "node-b", Addr: addrB},
			{ID: "node-c", Addr: addrC},
		},
		cluster.RingConfig{VirtualNodes: 64},
	)
	if err != nil {
		t.Fatalf("NewCluster() error = %v", err)
	}

	key := findPrimaryKey(t, clusterView, "node-a")

	if err := writeRequest(addrA, protocol.Request{
		Command: protocol.CommandSet,
		Key:     key,
		Value:   []byte("replicated"),
	}); err != nil {
		t.Fatalf("SET on primary error = %v", err)
	}

	value, err := readRequest(addrB, key)
	if err != nil {
		t.Fatalf("GET on replica error = %v", err)
	}
	if string(value) != "replicated" {
		t.Fatalf("replica value = %q, want replicated", value)
	}
}

func joinNode(t *testing.T, addr, nodeID, nodeAddr string) {
	t.Helper()
	if err := writeRequest(addr, protocol.Request{
		Command: protocol.CommandClusterJoin,
		Key:     nodeID,
		Value:   []byte(nodeAddr),
	}); err != nil {
		t.Fatalf("CLUSTER_JOIN %s error = %v", nodeID, err)
	}
}

func writeRequest(addr string, req protocol.Request) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	payload, err := protocol.EncodeRequest(req)
	if err != nil {
		return err
	}
	if _, err := conn.Write(payload); err != nil {
		return err
	}

	resp, err := protocol.DecodeResponse(bufio.NewReader(conn))
	if err != nil {
		return err
	}
	if resp.Kind == protocol.ResponseError {
		return fmt.Errorf("%s", resp.Message)
	}
	if resp.Kind != protocol.ResponseOK {
		return fmt.Errorf("unexpected response kind %v", resp.Kind)
	}
	return nil
}

func readRequest(addr, key string) ([]byte, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	payload, err := protocol.EncodeRequest(protocol.Request{
		Command: protocol.CommandGet,
		Key:     key,
	})
	if err != nil {
		return nil, err
	}
	if _, err := conn.Write(payload); err != nil {
		return nil, err
	}

	resp, err := protocol.DecodeResponse(bufio.NewReader(conn))
	if err != nil {
		return nil, err
	}
	switch resp.Kind {
	case protocol.ResponseValue:
		return resp.Value, nil
	case protocol.ResponseNotFound:
		return nil, nil
	case protocol.ResponseError:
		return nil, fmt.Errorf("%s", resp.Message)
	default:
		return nil, fmt.Errorf("unexpected response kind %v", resp.Kind)
	}
}

func findPrimaryKey(t *testing.T, clusterView *cluster.Cluster, nodeID string) string {
	t.Helper()
	for i := 0; i < 4096; i++ {
		key := "repl-key-" + strconv.Itoa(i)
		owner, ok := clusterView.Owner(key)
		if ok && owner.ID == nodeID {
			return key
		}
	}
	t.Fatalf("no primary key found for %q", nodeID)
	return ""
}
