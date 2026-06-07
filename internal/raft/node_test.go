package raft

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestRaftLeaderElection(t *testing.T) {
	cluster := NewMemoryCluster()
	peers := map[string]string{
		"b": "127.0.0.1:6380",
		"c": "127.0.0.1:6381",
	}

	a := NewNode("a", "127.0.0.1:6379", peers, DefaultConfig(), nil, func(Entry) {}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	b := NewNode("b", "127.0.0.1:6380", map[string]string{"a": "127.0.0.1:6379", "c": "127.0.0.1:6381"}, DefaultConfig(), nil, func(Entry) {}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	c := NewNode("c", "127.0.0.1:6381", map[string]string{"a": "127.0.0.1:6379", "b": "127.0.0.1:6380"}, DefaultConfig(), nil, func(Entry) {}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	cluster.Add(a)
	cluster.Add(b)
	cluster.Add(c)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go a.Start(ctx)
	go b.Start(ctx)
	go c.Start(ctx)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		leaders := 0
		for _, node := range []*Node{a, b, c} {
			if node.IsLeader() {
				leaders++
			}
		}
		if leaders == 1 {
			return
		}
		a.ForceElectionTimeout()
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("expected exactly one leader")
}

func TestRaftLogReplicationMembership(t *testing.T) {
	cluster := NewMemoryCluster()

	applyB := 0
	b := NewNode("b", "127.0.0.1:6380", map[string]string{"a": "127.0.0.1:6379"}, DefaultConfig(), nil, func(entry Entry) {
		if entry.Command.Type == CommandAddMember {
			applyB++
		}
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	a := NewNode("a", "127.0.0.1:6379", map[string]string{"b": "127.0.0.1:6380"}, DefaultConfig(), nil, func(Entry) {}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	cluster.Add(a)
	cluster.Add(b)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go a.Start(ctx)
	go b.Start(ctx)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if a.IsLeader() {
			break
		}
		a.ForceElectionTimeout()
		time.Sleep(20 * time.Millisecond)
	}
	if !a.IsLeader() {
		t.Fatal("node a did not become leader")
	}

	propCtx, propCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer propCancel()
	if err := a.Propose(propCtx, Command{Type: CommandAddMember, NodeID: "c", Addr: "127.0.0.1:6381"}); err != nil {
		t.Fatalf("Propose() error = %v", err)
	}

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if applyB > 0 && b.Status().CommitIndex >= 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("replica did not apply entry, applyB=%d commit=%d", applyB, b.Status().CommitIndex)
}

func TestRaftLeaderFailover(t *testing.T) {
	cluster := NewMemoryCluster()
	peersAB := map[string]string{"b": "127.0.0.1:6380"}
	a := NewNode("a", "127.0.0.1:6379", peersAB, DefaultConfig(), nil, func(Entry) {}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	b := NewNode("b", "127.0.0.1:6380", map[string]string{"a": "127.0.0.1:6379"}, DefaultConfig(), nil, func(Entry) {}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	cluster.Add(a)
	cluster.Add(b)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go a.Start(ctx)
	go b.Start(ctx)

	for i := 0; i < 50; i++ {
		if a.IsLeader() {
			break
		}
		a.ForceElectionTimeout()
		time.Sleep(20 * time.Millisecond)
	}
	if !a.IsLeader() {
		t.Fatal("node a did not become leader")
	}

	cancel()
	time.Sleep(100 * time.Millisecond)

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go b.Start(ctx2)

	for i := 0; i < 100; i++ {
		if b.IsLeader() {
			return
		}
		b.ForceElectionTimeout()
		time.Sleep(30 * time.Millisecond)
	}
	t.Fatal("node b did not become leader after failover")
}
