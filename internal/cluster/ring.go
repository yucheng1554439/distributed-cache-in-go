package cluster

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

var (
	ErrNodeExists  = errors.New("node already exists")
	ErrNodeMissing = errors.New("node not found")
	ErrEmptyRing   = errors.New("hash ring is empty")
)

// RingConfig controls consistent hashing behavior.
type RingConfig struct {
	VirtualNodes int
	Hash         HashFunc
}

// DefaultRingConfig returns production-style defaults.
func DefaultRingConfig() RingConfig {
	return RingConfig{
		VirtualNodes: 128,
		Hash:         HashKey,
	}
}

// Ring is a consistent hash ring with virtual nodes.
type Ring struct {
	mu     sync.RWMutex
	cfg    RingConfig
	nodes  map[string]Node
	hashes []uint32
	lookup map[uint32]string
}

// NewRing creates an empty hash ring.
func NewRing(cfg RingConfig) *Ring {
	if cfg.VirtualNodes <= 0 {
		cfg.VirtualNodes = DefaultRingConfig().VirtualNodes
	}
	if cfg.Hash == nil {
		cfg.Hash = HashKey
	}

	return &Ring{
		cfg:    cfg,
		nodes:  make(map[string]Node),
		lookup: make(map[uint32]string),
	}
}

// AddNode inserts a physical node and its virtual nodes into the ring.
func (r *Ring) AddNode(node Node) error {
	if node.ID == "" {
		return errors.New("node id is required")
	}
	if node.Addr == "" {
		return errors.New("node address is required")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.nodes[node.ID]; exists {
		return fmt.Errorf("%w: %s", ErrNodeExists, node.ID)
	}

	r.nodes[node.ID] = node
	for i := 0; i < r.cfg.VirtualNodes; i++ {
		hash := r.cfg.Hash(vnodeKey(node.ID, i))
		r.lookup[hash] = node.ID
		r.hashes = append(r.hashes, hash)
	}
	sort.Slice(r.hashes, func(i, j int) bool { return r.hashes[i] < r.hashes[j] })
	return nil
}

// RemoveNode deletes a physical node and all of its virtual nodes.
func (r *Ring) RemoveNode(nodeID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.nodes[nodeID]; !ok {
		return fmt.Errorf("%w: %s", ErrNodeMissing, nodeID)
	}

	delete(r.nodes, nodeID)
	for i := 0; i < r.cfg.VirtualNodes; i++ {
		delete(r.lookup, r.cfg.Hash(vnodeKey(nodeID, i)))
	}
	r.rebuildHashes()
	return nil
}

// Lookup returns the primary owner for key using clockwise ring traversal.
func (r *Ring) Lookup(key string) (Node, bool) {
	return r.lookupHash(r.cfg.Hash(key))
}

// LookupN returns up to n distinct node owners for key walking clockwise on the ring.
func (r *Ring) LookupN(key string, n int) ([]Node, error) {
	if n <= 0 {
		return nil, errors.New("n must be positive")
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.hashes) == 0 {
		return nil, ErrEmptyRing
	}

	start := sort.Search(len(r.hashes), func(i int) bool {
		return r.hashes[i] >= r.cfg.Hash(key)
	})

	owners := make([]Node, 0, n)
	seen := make(map[string]struct{}, n)

	for step := 0; step < len(r.hashes) && len(owners) < n; step++ {
		idx := (start + step) % len(r.hashes)
		nodeID := r.lookup[r.hashes[idx]]
		if _, ok := seen[nodeID]; ok {
			continue
		}

		node, ok := r.nodes[nodeID]
		if !ok {
			continue
		}

		seen[nodeID] = struct{}{}
		owners = append(owners, node)
	}

	if len(owners) == 0 {
		return nil, ErrEmptyRing
	}
	return owners, nil
}

// Members returns all physical nodes on the ring in stable ID order.
func (r *Ring) Members() []Node {
	r.mu.RLock()
	defer r.mu.RUnlock()

	members := make([]Node, 0, len(r.nodes))
	for _, node := range r.nodes {
		members = append(members, node)
	}

	sort.Slice(members, func(i, j int) bool {
		return members[i].ID < members[j].ID
	})
	return members
}

// Len returns the number of physical nodes on the ring.
func (r *Ring) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.nodes)
}

// VirtualNodeCount returns the number of virtual nodes currently on the ring.
func (r *Ring) VirtualNodeCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.hashes)
}

func (r *Ring) lookupHash(keyHash uint32) (Node, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.hashes) == 0 {
		return Node{}, false
	}

	idx := sort.Search(len(r.hashes), func(i int) bool {
		return r.hashes[i] >= keyHash
	})
	if idx == len(r.hashes) {
		idx = 0
	}

	nodeID := r.lookup[r.hashes[idx]]
	node, ok := r.nodes[nodeID]
	return node, ok
}

func (r *Ring) rebuildHashes() {
	r.hashes = r.hashes[:0]
	for hash := range r.lookup {
		r.hashes = append(r.hashes, hash)
	}
	sort.Slice(r.hashes, func(i, j int) bool { return r.hashes[i] < r.hashes[j] })
}

func vnodeKey(nodeID string, index int) string {
	return fmt.Sprintf("%s#%d", nodeID, index)
}
