package replication

import (
	"sync"
	"time"
)

// HealthTracker records node reachability for quorum calculations.
type HealthTracker struct {
	mu      sync.RWMutex
	healthy map[string]bool
}

// NewHealthTracker creates a tracker that assumes all nodes are healthy.
func NewHealthTracker() *HealthTracker {
	return &HealthTracker{
		healthy: make(map[string]bool),
	}
}

// MarkHealthy marks nodeID as reachable.
func (h *HealthTracker) MarkHealthy(nodeID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.healthy[nodeID] = true
}

// MarkUnhealthy marks nodeID as unreachable.
func (h *HealthTracker) MarkUnhealthy(nodeID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.healthy[nodeID] = false
}

// IsHealthy reports whether nodeID is considered reachable.
// Unknown nodes default to healthy.
func (h *HealthTracker) IsHealthy(nodeID string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	healthy, ok := h.healthy[nodeID]
	if !ok {
		return true
	}
	return healthy
}

// Snapshot returns a copy of the current health map.
func (h *HealthTracker) Snapshot() map[string]bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make(map[string]bool, len(h.healthy))
	for id, healthy := range h.healthy {
		out[id] = healthy
	}
	return out
}

// LastChecked is reserved for future metrics integration.
type LastChecked struct {
	NodeID    string
	Healthy   bool
	CheckedAt time.Time
}
