package server

import (
	"bufio"
	"net"
	"strconv"
	"testing"

	"github.com/distributed-cache/distributed-cache/internal/cache"
	"github.com/distributed-cache/distributed-cache/internal/cluster"
	"github.com/distributed-cache/distributed-cache/internal/protocol"
)

func TestServerOwnershipRedirect(t *testing.T) {
	clusterView, err := cluster.NewCluster(
		cluster.Node{ID: "node-a", Addr: "127.0.0.1:6379"},
		[]cluster.Node{{ID: "node-b", Addr: "127.0.0.1:6380"}},
		cluster.RingConfig{VirtualNodes: 32},
	)
	if err != nil {
		t.Fatalf("NewCluster() error = %v", err)
	}

	key := findKeyOwnedBy(t, clusterView, "node-b")

	store := cache.NewStore(cache.DefaultConfig())
	srv, err := New(Config{
		Addr:    "127.0.0.1:0",
		Cluster: clusterView,
	}, store)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	addr, stop := listenServer(t, srv)
	defer stop()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer conn.Close()

	setReq, err := protocol.EncodeRequest(protocol.Request{
		Command: protocol.CommandSet,
		Key:     key,
		Value:   []byte("value"),
	})
	if err != nil {
		t.Fatalf("EncodeRequest() error = %v", err)
	}
	if _, err := conn.Write(setReq); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	resp, err := protocol.DecodeResponse(bufio.NewReader(conn))
	if err != nil {
		t.Fatalf("DecodeResponse() error = %v", err)
	}
	if resp.Kind != protocol.ResponseMoved {
		t.Fatalf("response kind = %v, want MOVED", resp.Kind)
	}
	if resp.NodeID != "node-b" || resp.Addr != "127.0.0.1:6380" {
		t.Fatalf("response = %+v, want node-b redirect", resp)
	}
}

func TestServerOwnerLookup(t *testing.T) {
	clusterView, err := cluster.NewCluster(
		cluster.Node{ID: "node-a", Addr: "127.0.0.1:6379"},
		[]cluster.Node{{ID: "node-b", Addr: "127.0.0.1:6380"}},
		cluster.RingConfig{VirtualNodes: 32},
	)
	if err != nil {
		t.Fatalf("NewCluster() error = %v", err)
	}

	key := findKeyOwnedBy(t, clusterView, "node-a")

	store := cache.NewStore(cache.DefaultConfig())
	srv, err := New(Config{
		Addr:    "127.0.0.1:0",
		Cluster: clusterView,
	}, store)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	addr, stop := listenServer(t, srv)
	defer stop()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer conn.Close()

	ownerReq, err := protocol.EncodeRequest(protocol.Request{
		Command: protocol.CommandOwner,
		Key:     key,
	})
	if err != nil {
		t.Fatalf("EncodeRequest() error = %v", err)
	}
	if _, err := conn.Write(ownerReq); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	resp, err := protocol.DecodeResponse(bufio.NewReader(conn))
	if err != nil {
		t.Fatalf("DecodeResponse() error = %v", err)
	}
	if resp.Kind != protocol.ResponseOwner || resp.NodeID != "node-a" {
		t.Fatalf("response = %+v, want owner node-a", resp)
	}
}

func TestServerClusterJoinLeave(t *testing.T) {
	clusterView, err := cluster.NewCluster(
		cluster.Node{ID: "node-a", Addr: "127.0.0.1:6379"},
		nil,
		cluster.DefaultRingConfig(),
	)
	if err != nil {
		t.Fatalf("NewCluster() error = %v", err)
	}

	store := cache.NewStore(cache.DefaultConfig())
	srv, err := New(Config{
		Addr:    "127.0.0.1:0",
		Cluster: clusterView,
	}, store)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	addr, stop := listenServer(t, srv)
	defer stop()

	writeRequest := func(t *testing.T, conn net.Conn, req protocol.Request) protocol.Response {
		t.Helper()
		payload, err := protocol.EncodeRequest(req)
		if err != nil {
			t.Fatalf("EncodeRequest() error = %v", err)
		}
		if _, err := conn.Write(payload); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
		resp, err := protocol.DecodeResponse(bufio.NewReader(conn))
		if err != nil {
			t.Fatalf("DecodeResponse() error = %v", err)
		}
		return resp
	}

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer conn.Close()

	if resp := writeRequest(t, conn, protocol.Request{
		Command: protocol.CommandClusterJoin,
		Key:     "node-b",
		Value:   []byte("127.0.0.1:6380"),
	}); resp.Kind != protocol.ResponseOK {
		t.Fatalf("join response = %v", resp.Kind)
	}

	if resp := writeRequest(t, conn, protocol.Request{Command: protocol.CommandClusterMembers}); resp.Kind != protocol.ResponseMembers || len(resp.Members) != 2 {
		t.Fatalf("members response = %+v", resp)
	}

	if resp := writeRequest(t, conn, protocol.Request{
		Command: protocol.CommandClusterLeave,
		Key:     "node-b",
	}); resp.Kind != protocol.ResponseOK {
		t.Fatalf("leave response = %v", resp.Kind)
	}
}

func findKeyOwnedBy(t *testing.T, clusterView *cluster.Cluster, nodeID string) string {
	t.Helper()

	for i := 0; i < 4096; i++ {
		key := "lookup-key-" + strconv.Itoa(i)
		owner, ok := clusterView.Owner(key)
		if ok && owner.ID == nodeID {
			return key
		}
	}
	t.Fatalf("no key found for node %q", nodeID)
	return ""
}
