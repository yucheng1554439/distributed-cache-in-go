package server

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/distributed-cache/distributed-cache/internal/cache"
	"github.com/distributed-cache/distributed-cache/internal/cluster"
	"github.com/distributed-cache/distributed-cache/internal/protocol"
	"github.com/distributed-cache/distributed-cache/internal/raft"
)

func startRaftServer(t *testing.T, self cluster.Node, peers []cluster.Node, apply func(raft.Entry)) (addr string, node *raft.Node, stop func()) {
	t.Helper()

	store := cache.NewStore(cache.DefaultConfig())
	clusterView, err := cluster.NewCluster(self, peers, cluster.RingConfig{VirtualNodes: 32})
	if err != nil {
		t.Fatalf("NewCluster() error = %v", err)
	}

	peerMap := make(map[string]string, len(peers))
	for _, peer := range peers {
		peerMap[peer.ID] = peer.Addr
	}

	var raftNode *raft.Node
	applyFn := func(entry raft.Entry) {
		switch entry.Command.Type {
		case raft.CommandAddMember:
			_ = clusterView.Join(cluster.Node{ID: entry.Command.NodeID, Addr: entry.Command.Addr})
			if raftNode != nil {
				raftNode.AddPeer(entry.Command.NodeID, entry.Command.Addr)
			}
		case raft.CommandRemoveMember:
			_ = clusterView.Leave(entry.Command.NodeID)
			if raftNode != nil {
				raftNode.RemovePeer(entry.Command.NodeID)
			}
		}
		if apply != nil {
			apply(entry)
		}
	}

	raftNode = raft.NewNode(self.ID, "127.0.0.1:0", peerMap, raft.DefaultConfig(), &raft.TCPTransport{Timeout: time.Second}, applyFn, slog.New(slog.NewTextHandler(io.Discard, nil)))

	srv, err := New(Config{
		Addr:    "127.0.0.1:0",
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		Cluster: clusterView,
		Raft:    raftNode,
	}, store)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- srv.ListenAndServe(ctx)
	}()
	go raftNode.Start(ctx)

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

	// Update advertised address after the server binds to its listen port.
	raftNode.SetAddr(addr)

	stop = func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
	}
	return addr, raftNode, stop
}

func TestServerRaftMembership(t *testing.T) {
	addrA, raftA, stopA := startRaftServer(t, cluster.Node{ID: "node-a", Addr: "127.0.0.1:0"}, nil, nil)
	defer stopA()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if raftA.IsLeader() {
			break
		}
		raftA.ForceElectionTimeout()
		time.Sleep(30 * time.Millisecond)
	}
	if !raftA.IsLeader() {
		t.Fatal("node-a did not become raft leader")
	}

	addrB, _, stopB := startRaftServer(t, cluster.Node{ID: "node-b", Addr: "127.0.0.1:0"}, []cluster.Node{
		{ID: "node-a", Addr: addrA},
	}, nil)
	defer stopB()

	raftA.SetPeerAddr("node-b", addrB)

	if err := writeClusterJoin(addrA, "node-b", addrB); err != nil {
		t.Fatalf("CLUSTER_JOIN error = %v", err)
	}

	conn, err := net.Dial("tcp", addrA)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer conn.Close()

	if err := writeClusterMembers(conn); err != nil {
		t.Fatalf("CLUSTER_MEMBERS error = %v", err)
	}
}

func writeClusterJoin(addr, nodeID, nodeAddr string) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	payload, err := protocol.EncodeRequest(protocol.Request{
		Command: protocol.CommandClusterJoin,
		Key:     nodeID,
		Value:   []byte(nodeAddr),
	})
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
	if resp.Kind != protocol.ResponseOK {
		return fmt.Errorf("unexpected response %v", resp.Kind)
	}
	return nil
}

func writeClusterMembers(conn net.Conn) error {
	payload, err := protocol.EncodeRequest(protocol.Request{Command: protocol.CommandClusterMembers})
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
	if resp.Kind != protocol.ResponseMembers {
		return fmt.Errorf("unexpected response %v", resp.Kind)
	}
	if len(resp.Members) < 2 {
		return fmt.Errorf("members = %d, want >= 2", len(resp.Members))
	}
	return nil
}
