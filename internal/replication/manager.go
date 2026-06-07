package replication

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/distributed-cache/distributed-cache/internal/cache"
	"github.com/distributed-cache/distributed-cache/internal/cluster"
)

var (
	ErrQuorumNotMet   = errors.New("write quorum not met")
	ErrReadQuorumNotMet = errors.New("read quorum not met")
	ErrNotInReplicaSet = errors.New("key not in local replica set")
)

// Manager coordinates primary-replica replication for a node.
type Manager struct {
	cfg     Config
	cluster *cluster.Cluster
	store   *cache.Store
	health  *HealthTracker
	peer    Peer
	logger  *slog.Logger
}

// NewManager creates a replication manager.
func NewManager(cfg Config, clusterView *cluster.Cluster, store *cache.Store, logger *slog.Logger) (*Manager, error) {
	cfg = cfg.Normalize()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if clusterView == nil {
		return nil, errors.New("cluster is required for replication")
	}
	if store == nil {
		return nil, errors.New("store is required")
	}
	if logger == nil {
		logger = slog.Default()
	}

	return &Manager{
		cfg:     cfg,
		cluster: clusterView,
		store:   store,
		health:  NewHealthTracker(),
		peer: Peer{
			Timeout: 2 * time.Second,
		},
		logger: logger,
	}, nil
}

// Config returns the active replication configuration.
func (m *Manager) Config() Config {
	return m.cfg
}

// Enabled reports whether replication is active.
func (m *Manager) Enabled() bool {
	return m.cfg.Enabled()
}

// IsPrimary reports whether the local node is the primary owner for key.
func (m *Manager) IsPrimary(key string) bool {
	owner, ok := m.cluster.Owner(key)
	return ok && owner.ID == m.cluster.SelfID()
}

// IsInReplicaSet reports whether the local node stores key in its replica set.
func (m *Manager) IsInReplicaSet(key string) bool {
	if !m.Enabled() {
		return m.cluster.IsOwner(key)
	}
	owners, err := m.replicaSet(key)
	if err != nil {
		return false
	}
	for _, owner := range owners {
		if owner.ID == m.cluster.SelfID() {
			return true
		}
	}
	return false
}

// Primary returns the primary node for key.
func (m *Manager) Primary(key string) (cluster.Node, bool) {
	return m.cluster.Owner(key)
}

// ReplicaSet returns the replication group for key.
func (m *Manager) ReplicaSet(key string) ([]cluster.Node, error) {
	return m.replicaSet(key)
}

// WriteSet stores key on the primary and replicates to replicas when enabled.
func (m *Manager) WriteSet(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if !m.IsPrimary(key) {
		return fmt.Errorf("local node is not primary for key %q", key)
	}
	if !m.Enabled() {
		return m.store.Set(key, value, ttl)
	}

	required := m.cfg.RequiredWriteAcks()
	if required == 0 {
		return m.store.Set(key, value, ttl)
	}

	owners, err := m.replicaSet(key)
	if err != nil {
		return err
	}

	if err := m.store.Set(key, value, ttl); err != nil {
		return err
	}

	type replResult struct {
		node cluster.Node
		err  error
	}

	results := make(chan replResult, len(owners))
	var wg sync.WaitGroup

	for _, node := range owners {
		if node.ID == m.cluster.SelfID() {
			continue
		}
		if !m.health.IsHealthy(node.ID) {
			continue
		}

		wg.Add(1)
		go func(node cluster.Node) {
			defer wg.Done()
			err := m.peer.ReplicateSet(ctx, node.Addr, key, value, ttl)
			if err != nil {
				m.health.MarkUnhealthy(node.ID)
				m.logger.Warn("replication set failed", "node_id", node.ID, "key", key, "error", err)
			} else {
				m.health.MarkHealthy(node.ID)
			}
			results <- replResult{node: node, err: err}
		}(node)
	}

	waitDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(results)
		close(waitDone)
	}()

	select {
	case <-ctx.Done():
		_ = m.store.Delete(key)
		return ctx.Err()
	case <-waitDone:
	}

	acked := make([]cluster.Node, 0)
	for result := range results {
		if result.err == nil {
			acked = append(acked, result.node)
		}
	}

	if len(acked) < required {
		for _, node := range acked {
			_ = m.peer.ReplicateDelete(context.Background(), node.Addr, key)
		}
		_ = m.store.Delete(key)
		return fmt.Errorf("%w: got %d replica acks, need %d", ErrQuorumNotMet, len(acked), required)
	}

	return nil
}

// WriteDelete deletes key on the primary and replicates the delete.
func (m *Manager) WriteDelete(ctx context.Context, key string) (bool, error) {
	if !m.IsPrimary(key) {
		return false, fmt.Errorf("local node is not primary for key %q", key)
	}

	existing, ok := m.store.Get(key)
	if !ok {
		return false, nil
	}
	if !m.Enabled() {
		return m.store.Delete(key), nil
	}

	required := m.cfg.RequiredWriteAcks()
	if required == 0 {
		return m.store.Delete(key), nil
	}

	owners, err := m.replicaSet(key)
	if err != nil {
		return false, err
	}

	acked := make([]cluster.Node, 0)
	for _, node := range owners {
		if node.ID == m.cluster.SelfID() || !m.health.IsHealthy(node.ID) {
			continue
		}
		if err := m.peer.ReplicateDelete(ctx, node.Addr, key); err != nil {
			m.health.MarkUnhealthy(node.ID)
			m.logger.Warn("replication delete failed", "node_id", node.ID, "key", key, "error", err)
			continue
		}
		m.health.MarkHealthy(node.ID)
		acked = append(acked, node)
	}

	if len(acked) < required {
		for _, node := range acked {
			_ = m.peer.ReplicateSet(context.Background(), node.Addr, key, existing, 0)
		}
		return false, fmt.Errorf("%w: got %d replica acks, need %d", ErrQuorumNotMet, len(acked), required)
	}

	return m.store.Delete(key), nil
}

// Read fetches key according to the configured read consistency.
func (m *Manager) Read(ctx context.Context, key string) ([]byte, bool, error) {
	if !m.IsInReplicaSet(key) {
		return nil, false, ErrNotInReplicaSet
	}

	switch m.cfg.ReadConsistency {
	case ReadPrimary:
		if !m.IsPrimary(key) {
			return nil, false, ErrNotInReplicaSet
		}
		value, ok := m.store.Get(key)
		return value, ok, nil
	case ReadAny:
		value, ok := m.store.Get(key)
		if ok {
			return value, true, nil
		}
		return m.readFromPeers(ctx, key, 1)
	default:
		return m.readQuorum(ctx, key)
	}
}

// ApplyReplicaSet stores a replicated write on a replica node.
func (m *Manager) ApplyReplicaSet(key string, value []byte, ttl time.Duration) error {
	if !m.IsInReplicaSet(key) {
		return ErrNotInReplicaSet
	}
	return m.store.Set(key, value, ttl)
}

// ApplyReplicaDelete applies a replicated delete on a replica node.
func (m *Manager) ApplyReplicaDelete(key string) (bool, error) {
	if !m.IsInReplicaSet(key) {
		return false, ErrNotInReplicaSet
	}
	return m.store.Delete(key), nil
}

// RunHealthChecks periodically pings cluster members.
func (m *Manager) RunHealthChecks(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 5 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.checkHealth(ctx)
		}
	}
}

func (m *Manager) checkHealth(ctx context.Context) {
	for _, member := range m.cluster.Members() {
		if member.ID == m.cluster.SelfID() {
			continue
		}
		if err := m.peer.Ping(ctx, member.Addr); err != nil {
			m.health.MarkUnhealthy(member.ID)
			m.logger.Warn("health check failed", "node_id", member.ID, "error", err)
			continue
		}
		m.health.MarkHealthy(member.ID)
	}
}

func (m *Manager) readQuorum(ctx context.Context, key string) ([]byte, bool, error) {
	owners, err := m.replicaSet(key)
	if err != nil {
		return nil, false, err
	}

	type readResult struct {
		value []byte
		ok    bool
	}

	results := make(chan readResult, len(owners))
	var wg sync.WaitGroup

	for _, node := range owners {
		if !m.health.IsHealthy(node.ID) {
			continue
		}

		wg.Add(1)
		go func(node cluster.Node) {
			defer wg.Done()
			if node.ID == m.cluster.SelfID() {
				value, ok := m.store.Get(key)
				results <- readResult{value: value, ok: ok}
				return
			}
			value, ok, err := m.peer.Get(ctx, node.Addr, key)
			if err != nil {
				m.health.MarkUnhealthy(node.ID)
				return
			}
			m.health.MarkHealthy(node.ID)
			results <- readResult{value: value, ok: ok}
		}(node)
	}

	waitDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(results)
		close(waitDone)
	}()

	select {
	case <-ctx.Done():
		return nil, false, ctx.Err()
	case <-waitDone:
	}

	values := make(map[string]int)
	found := 0
	var winner []byte

	for result := range results {
		if !result.ok {
			continue
		}
		found++
		keyValue := string(result.value)
		values[keyValue]++
		if values[keyValue] >= m.cfg.ReadQuorum {
			return result.value, true, nil
		}
		if winner == nil {
			winner = result.value
		}
	}

	if found >= m.cfg.ReadQuorum {
		return winner, true, nil
	}
	if found == 0 {
		return nil, false, nil
	}
	return nil, false, ErrReadQuorumNotMet
}

func (m *Manager) readFromPeers(ctx context.Context, key string, required int) ([]byte, bool, error) {
	owners, err := m.replicaSet(key)
	if err != nil {
		return nil, false, err
	}

	for _, node := range owners {
		if node.ID == m.cluster.SelfID() || !m.health.IsHealthy(node.ID) {
			continue
		}
		value, ok, err := m.peer.Get(ctx, node.Addr, key)
		if err != nil {
			m.health.MarkUnhealthy(node.ID)
			continue
		}
		m.health.MarkHealthy(node.ID)
		if ok {
			return value, true, nil
		}
		if required <= 0 {
			return nil, false, nil
		}
	}

	return nil, false, nil
}

func (m *Manager) replicaSet(key string) ([]cluster.Node, error) {
	if !m.cfg.Enabled() {
		owner, ok := m.cluster.Owner(key)
		if !ok {
			return nil, cluster.ErrEmptyRing
		}
		return []cluster.Node{owner}, nil
	}
	return m.cluster.Owners(key, m.cfg.ReplicationFactor)
}
