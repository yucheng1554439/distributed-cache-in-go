package raft

import (
	"context"
	"sync"
)

// MemoryCluster wires in-process Raft nodes together for tests.
type MemoryCluster struct {
	mu    sync.RWMutex
	nodes map[string]*Node
}

// NewMemoryCluster creates an empty in-memory Raft cluster.
func NewMemoryCluster() *MemoryCluster {
	return &MemoryCluster{
		nodes: make(map[string]*Node),
	}
}

// Add registers a node and connects transport to the cluster.
func (c *MemoryCluster) Add(node *Node) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nodes[node.id] = node
	node.SetTransport(&memoryTransport{cluster: c})
}

// Get returns a node by id.
func (c *MemoryCluster) Get(id string) *Node {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.nodes[id]
}

type memoryTransport struct {
	cluster *MemoryCluster
}

func (t *memoryTransport) RequestVote(ctx context.Context, addr string, req RequestVoteRequest) (RequestVoteResponse, error) {
	target := t.findByAddr(addr)
	if target == nil {
		return RequestVoteResponse{}, context.Canceled
	}
	return target.HandleRequestVote(req), nil
}

func (t *memoryTransport) AppendEntries(ctx context.Context, addr string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	target := t.findByAddr(addr)
	if target == nil {
		return AppendEntriesResponse{}, context.Canceled
	}
	return target.HandleAppendEntries(req), nil
}

func (t *memoryTransport) findByAddr(addr string) *Node {
	t.cluster.mu.RLock()
	defer t.cluster.mu.RUnlock()
	for _, node := range t.cluster.nodes {
		if node.addr == addr {
			return node
		}
	}
	return nil
}
