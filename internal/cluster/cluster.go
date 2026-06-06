package cluster

import (
	"fmt"
	"strconv"
	"sync"
)

// Cluster manages membership and key ownership for a distributed cache.
type Cluster struct {
	mu     sync.RWMutex
	ring   *Ring
	selfID string
}

// NewCluster builds a cluster ring containing the local node and optional peers.
func NewCluster(self Node, peers []Node, cfg RingConfig) (*Cluster, error) {
	if self.ID == "" {
		return nil, fmt.Errorf("local node id is required")
	}
	if self.Addr == "" {
		return nil, fmt.Errorf("local node address is required")
	}

	ring := NewRing(cfg)
	if err := ring.AddNode(self); err != nil {
		return nil, err
	}

	for _, peer := range peers {
		if peer.ID == self.ID {
			return nil, fmt.Errorf("peer list contains local node id %q", self.ID)
		}
		if err := ring.AddNode(peer); err != nil {
			return nil, fmt.Errorf("add peer %q: %w", peer.ID, err)
		}
	}

	return &Cluster{
		ring:   ring,
		selfID: self.ID,
	}, nil
}

// SelfID returns the local node identifier.
func (c *Cluster) SelfID() string {
	return c.selfID
}

// Owner returns the node responsible for key.
func (c *Cluster) Owner(key string) (Node, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ring.Lookup(key)
}

// Owners returns up to n distinct nodes responsible for key.
func (c *Cluster) Owners(key string, n int) ([]Node, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ring.LookupN(key, n)
}

// IsOwner reports whether the local node owns key.
func (c *Cluster) IsOwner(key string) bool {
	owner, ok := c.Owner(key)
	return ok && owner.ID == c.selfID
}

// Join adds a node to the cluster ring.
func (c *Cluster) Join(node Node) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ring.AddNode(node)
}

// Leave removes a node from the cluster ring.
func (c *Cluster) Leave(nodeID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ring.RemoveNode(nodeID)
}

// Members returns all nodes known to the cluster.
func (c *Cluster) Members() []Node {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ring.Members()
}

// EstimateKeyMovementAfterJoin approximates the fraction of keys that would move when node joins.
func (c *Cluster) EstimateKeyMovementAfterJoin(node Node) (float64, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.estimateMovement(func(temp *Ring) error {
		return temp.AddNode(node)
	})
}

// EstimateKeyMovementAfterLeave approximates the fraction of keys that would move when node leaves.
func (c *Cluster) EstimateKeyMovementAfterLeave(nodeID string) (float64, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.estimateMovement(func(temp *Ring) error {
		return temp.RemoveNode(nodeID)
	})
}

func (c *Cluster) estimateMovement(apply func(*Ring) error) (float64, error) {
	if c.ring.Len() == 0 {
		return 0, ErrEmptyRing
	}

	temp := NewRing(c.ring.cfg)
	for _, node := range c.ring.Members() {
		if err := temp.AddNode(node); err != nil {
			return 0, err
		}
	}
	if err := apply(temp); err != nil {
		return 0, err
	}

	const samples = 2048
	changed := 0
	for i := 0; i < samples; i++ {
		key := "rebalance-key-" + strconv.Itoa(i)
		beforeOwner, ok := c.ring.Lookup(key)
		if !ok {
			continue
		}
		afterOwner, ok := temp.Lookup(key)
		if !ok {
			continue
		}
		if beforeOwner.ID != afterOwner.ID {
			changed++
		}
	}

	return float64(changed) / float64(samples), nil
}
