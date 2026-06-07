package integration_test

import (
	"strconv"
	"testing"

	"github.com/distributed-cache/distributed-cache/internal/cluster"
	"github.com/distributed-cache/distributed-cache/internal/replication"
)

func TestReplicatedClusterClientRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	replCfg := replication.Config{
		ReplicationFactor: 3,
		WriteQuorum:       2,
		ReadQuorum:        1,
		WriteConsistency:  replication.WriteQuorum,
		ReadConsistency:   replication.ReadAny,
	}

	addrA, stopA := startReplicatedNode(t, cluster.Node{ID: "node-a", Addr: "127.0.0.1:0"}, nil, replCfg)
	defer stopA()
	addrB, stopB := startReplicatedNode(t, cluster.Node{ID: "node-b", Addr: "127.0.0.1:0"}, []cluster.Node{
		{ID: "node-a", Addr: addrA},
	}, replCfg)
	defer stopB()
	addrC, stopC := startReplicatedNode(t, cluster.Node{ID: "node-c", Addr: "127.0.0.1:0"}, []cluster.Node{
		{ID: "node-a", Addr: addrA},
		{ID: "node-b", Addr: addrB},
	}, replCfg)
	defer stopC()

	clientPath := buildClient(t)
	runClientWithFlags(t, clientPath, addrA, []string{"CLUSTER_JOIN", "node-b", addrB}, "OK\n")
	runClientWithFlags(t, clientPath, addrA, []string{"CLUSTER_JOIN", "node-c", addrC}, "OK\n")
	runClientWithFlags(t, clientPath, addrB, []string{"CLUSTER_JOIN", "node-c", addrC}, "OK\n")

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

	key := findKeyForPrimary(t, clusterView, "node-a")

	runClientWithFlags(t, clientPath, addrA, []string{"SET", key, "payload"}, "OK\n")
	runClientWithFlags(t, clientPath, addrB, []string{"GET", key}, "VALUE payload\n")
	runClientWithFlags(t, clientPath, addrC, []string{"GET", key}, "VALUE payload\n")
}

func findKeyForPrimary(t *testing.T, clusterView *cluster.Cluster, nodeID string) string {
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
