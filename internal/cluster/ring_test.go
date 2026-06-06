package cluster

import (
	"fmt"
	"math"
	"strconv"
	"testing"
)

func TestRingLookupDistributesKeys(t *testing.T) {
	t.Parallel()

	ring := NewRing(RingConfig{VirtualNodes: 128})
	nodes := []Node{
		{ID: "node-a", Addr: "127.0.0.1:6379"},
		{ID: "node-b", Addr: "127.0.0.1:6380"},
		{ID: "node-c", Addr: "127.0.0.1:6381"},
	}
	for _, node := range nodes {
		if err := ring.AddNode(node); err != nil {
			t.Fatalf("AddNode(%q) error = %v", node.ID, err)
		}
	}

	counts := map[string]int{}
	const keys = 3000
	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("user:%d:session:%d", i, i*9973)
		owner, ok := ring.Lookup(key)
		if !ok {
			t.Fatalf("lookup failed for key %d", i)
		}
		counts[owner.ID]++
	}

	for _, node := range nodes {
		share := float64(counts[node.ID]) / float64(keys)
		if share < 0.20 || share > 0.50 {
			t.Fatalf("node %s got %.2f share, want roughly even", node.ID, share)
		}
	}
}

func TestRingJoinLeaveRebalance(t *testing.T) {
	t.Parallel()

	ring := NewRing(RingConfig{VirtualNodes: 64})
	for _, node := range []Node{
		{ID: "a", Addr: "127.0.0.1:6379"},
		{ID: "b", Addr: "127.0.0.1:6380"},
	} {
		if err := ring.AddNode(node); err != nil {
			t.Fatalf("AddNode() error = %v", err)
		}
	}

	before := snapshotOwners(t, ring, 512)
	if err := ring.AddNode(Node{ID: "c", Addr: "127.0.0.1:6381"}); err != nil {
		t.Fatalf("AddNode(c) error = %v", err)
	}
	afterJoin := snapshotOwners(t, ring, 512)

	changed := 0
	for key, owner := range before {
		if afterJoin[key] != owner {
			changed++
		}
	}
	ratio := float64(changed) / float64(len(before))
	if ratio < 0.20 || ratio > 0.60 {
		t.Fatalf("join rebalance ratio = %.2f, want ~1/3", ratio)
	}

	if err := ring.RemoveNode("c"); err != nil {
		t.Fatalf("RemoveNode(c) error = %v", err)
	}
	afterLeave := snapshotOwners(t, ring, 512)
	if len(afterLeave) != len(before) {
		t.Fatalf("owner map size = %d, want %d", len(afterLeave), len(before))
	}
}

func TestRingLookupN(t *testing.T) {
	t.Parallel()

	ring := NewRing(RingConfig{VirtualNodes: 32})
	for _, node := range []Node{
		{ID: "a", Addr: "1"},
		{ID: "b", Addr: "2"},
		{ID: "c", Addr: "3"},
	} {
		if err := ring.AddNode(node); err != nil {
			t.Fatalf("AddNode() error = %v", err)
		}
	}

	owners, err := ring.LookupN("user:42", 2)
	if err != nil {
		t.Fatalf("LookupN() error = %v", err)
	}
	if len(owners) != 2 {
		t.Fatalf("owners = %d, want 2", len(owners))
	}
	if owners[0].ID == owners[1].ID {
		t.Fatal("expected distinct owners")
	}

	all, err := ring.LookupN("user:42", 10)
	if err != nil {
		t.Fatalf("LookupN(all) error = %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("owners = %d, want 3", len(all))
	}
}

func TestClusterJoinLeave(t *testing.T) {
	t.Parallel()

	cluster, err := NewCluster(
		Node{ID: "node-a", Addr: "127.0.0.1:6379"},
		[]Node{{ID: "node-b", Addr: "127.0.0.1:6380"}},
		DefaultRingConfig(),
	)
	if err != nil {
		t.Fatalf("NewCluster() error = %v", err)
	}

	if !cluster.IsOwner(ownerKeyFor(t, cluster, "node-a")) {
		t.Fatal("expected node-a to own at least one sampled key")
	}
	if cluster.IsOwner(ownerKeyFor(t, cluster, "node-b")) {
		t.Fatal("did not expect every key to belong to node-b")
	}

	ratio, err := cluster.EstimateKeyMovementAfterJoin(Node{ID: "node-c", Addr: "127.0.0.1:6381"})
	if err != nil {
		t.Fatalf("EstimateKeyMovementAfterJoin() error = %v", err)
	}
	if math.Abs(ratio-0.33) > 0.20 {
		t.Fatalf("join rebalance ratio = %.2f, want roughly 0.33", ratio)
	}

	if err := cluster.Join(Node{ID: "node-c", Addr: "127.0.0.1:6381"}); err != nil {
		t.Fatalf("Join() error = %v", err)
	}
	if len(cluster.Members()) != 3 {
		t.Fatalf("members = %d, want 3", len(cluster.Members()))
	}

	if err := cluster.Leave("node-c"); err != nil {
		t.Fatalf("Leave() error = %v", err)
	}
	if len(cluster.Members()) != 2 {
		t.Fatalf("members = %d, want 2", len(cluster.Members()))
	}
}

func TestParsePeers(t *testing.T) {
	t.Parallel()

	peers, err := ParsePeers("a=127.0.0.1:6379,b=127.0.0.1:6380")
	if err != nil {
		t.Fatalf("ParsePeers() error = %v", err)
	}
	if len(peers) != 2 || peers[0].ID != "a" {
		t.Fatalf("peers = %+v", peers)
	}

	if _, err := ParsePeers("bad-peer"); err == nil {
		t.Fatal("expected parse error")
	}
}

func snapshotOwners(t *testing.T, ring *Ring, samples int) map[string]string {
	t.Helper()

	out := make(map[string]string, samples)
	for i := 0; i < samples; i++ {
		key := "key-" + strconv.Itoa(i)
		owner, ok := ring.Lookup(key)
		if !ok {
			t.Fatalf("lookup failed for %q", key)
		}
		out[key] = owner.ID
	}
	return out
}

func ownerKeyFor(t *testing.T, cluster *Cluster, nodeID string) string {
	t.Helper()

	for i := 0; i < 4096; i++ {
		key := "key-" + strconv.Itoa(i)
		owner, ok := cluster.Owner(key)
		if ok && owner.ID == nodeID {
			return key
		}
	}
	t.Fatalf("no key found for node %q", nodeID)
	return ""
}
