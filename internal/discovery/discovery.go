package discovery

import "github.com/distributed-cache/distributed-cache/internal/cluster"

// Node describes a reachable cache member.
type Node struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
}

// Snapshot is a service-discovery view of the cluster.
type Snapshot struct {
	SelfID string `json:"self_id"`
	Nodes  []Node `json:"nodes"`
}

// Provider exposes the current cluster topology.
type Provider interface {
	Snapshot() Snapshot
}

// ClusterProvider reads membership from a cluster view.
type ClusterProvider struct {
	cluster *cluster.Cluster
}

// NewClusterProvider creates a discovery provider backed by cluster membership.
func NewClusterProvider(clusterView *cluster.Cluster) *ClusterProvider {
	return &ClusterProvider{cluster: clusterView}
}

// Snapshot returns all known cluster members.
func (p *ClusterProvider) Snapshot() Snapshot {
	if p == nil || p.cluster == nil {
		return Snapshot{}
	}

	members := p.cluster.Members()
	nodes := make([]Node, 0, len(members))
	for _, member := range members {
		nodes = append(nodes, Node{ID: member.ID, Addr: member.Addr})
	}

	return Snapshot{
		SelfID: p.cluster.SelfID(),
		Nodes:  nodes,
	}
}
