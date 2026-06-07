package discovery

import (
	"testing"

	"github.com/distributed-cache/distributed-cache/internal/cluster"
)

func TestClusterProviderSnapshot(t *testing.T) {
	t.Parallel()

	clusterView, err := cluster.NewCluster(
		cluster.Node{ID: "node-a", Addr: "node-a:6379"},
		[]cluster.Node{
			{ID: "node-b", Addr: "node-b:6379"},
		},
		cluster.RingConfig{VirtualNodes: 8},
	)
	if err != nil {
		t.Fatalf("NewCluster() error = %v", err)
	}

	provider := NewClusterProvider(clusterView)
	snap := provider.Snapshot()

	if snap.SelfID != "node-a" {
		t.Fatalf("SelfID = %q, want node-a", snap.SelfID)
	}
	if len(snap.Nodes) != 2 {
		t.Fatalf("len(Nodes) = %d, want 2", len(snap.Nodes))
	}
}
