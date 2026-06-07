package replication

import (
	"strconv"
	"testing"

	"github.com/distributed-cache/distributed-cache/internal/cache"
	"github.com/distributed-cache/distributed-cache/internal/cluster"
)

func TestManagerReplicaSet(t *testing.T) {
	t.Parallel()

	clusterView, err := cluster.NewCluster(
		cluster.Node{ID: "a", Addr: "127.0.0.1:6379"},
		[]cluster.Node{
			{ID: "b", Addr: "127.0.0.1:6380"},
			{ID: "c", Addr: "127.0.0.1:6381"},
		},
		cluster.DefaultRingConfig(),
	)
	if err != nil {
		t.Fatalf("NewCluster() error = %v", err)
	}

	manager, err := NewManager(DefaultConfig(), clusterView, cache.NewStore(cache.DefaultConfig()), nil)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	owners, err := manager.ReplicaSet("user:100")
	if err != nil {
		t.Fatalf("ReplicaSet() error = %v", err)
	}
	if len(owners) != 3 {
		t.Fatalf("owners = %d, want 3", len(owners))
	}
}

func TestManagerIsPrimaryAndReplica(t *testing.T) {
	t.Parallel()

	clusterView, err := cluster.NewCluster(
		cluster.Node{ID: "a", Addr: "127.0.0.1:6379"},
		[]cluster.Node{{ID: "b", Addr: "127.0.0.1:6380"}},
		cluster.RingConfig{VirtualNodes: 32},
	)
	if err != nil {
		t.Fatalf("NewCluster() error = %v", err)
	}

	manager, err := NewManager(Config{
		ReplicationFactor: 2,
		WriteQuorum:       2,
		ReadQuorum:        1,
		WriteConsistency:  WriteQuorum,
		ReadConsistency:   ReadAny,
	}, clusterView, cache.NewStore(cache.DefaultConfig()), nil)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	var primaryKey string
	for i := 0; i < 4096; i++ {
		key := "key-" + strconv.Itoa(i)
		if manager.IsPrimary(key) {
			primaryKey = key
			break
		}
	}
	if primaryKey == "" {
		t.Fatal("expected node-a to be primary for some key")
	}
	if !manager.IsInReplicaSet(primaryKey) {
		t.Fatal("primary key should be in local replica set")
	}
}
