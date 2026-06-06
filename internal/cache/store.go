package cache

import (
	"container/list"
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrEntryTooLarge = errors.New("entry exceeds maximum memory limit")
	ErrMemoryLimit   = errors.New("unable to store entry within memory limit")
)

// Config controls cache capacity and expiration behavior.
type Config struct {
	// MaxBytes limits total memory used by stored keys and values.
	// Zero disables the limit.
	MaxBytes int64

	// CleanupInterval sets how often the background worker scans for expired keys.
	CleanupInterval time.Duration

	// Now provides the current time, primarily for tests.
	Now func() time.Time
}

// DefaultConfig returns sensible defaults for a standalone cache node.
func DefaultConfig() Config {
	return Config{
		CleanupInterval: time.Second,
		Now:             time.Now,
	}
}

type entry struct {
	key       string
	value     []byte
	expiresAt time.Time
}

// Store is a thread-safe in-memory cache with TTL and LRU eviction.
type Store struct {
	cfg Config

	mu          sync.Mutex
	entries     map[string]*list.Element
	lru         *list.List
	memoryBytes int64
}

// NewStore creates an empty Store.
func NewStore(cfg Config) *Store {
	if cfg.CleanupInterval <= 0 {
		cfg.CleanupInterval = time.Second
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}

	return &Store{
		cfg:     cfg,
		entries: make(map[string]*list.Element),
		lru:     list.New(),
	}
}

// Set stores key and value with an optional TTL. Zero TTL means no expiration.
func (s *Store) Set(key string, value []byte, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if elem, ok := s.entries[key]; ok {
		s.removeElement(elem)
	}

	stored := make([]byte, len(value))
	copy(stored, value)

	size := entrySize(key, stored)
	if s.cfg.MaxBytes > 0 && size > s.cfg.MaxBytes {
		return ErrEntryTooLarge
	}

	s.ensureCapacity(size)
	if s.cfg.MaxBytes > 0 && s.memoryBytes+size > s.cfg.MaxBytes {
		return ErrMemoryLimit
	}

	item := &entry{
		key:   key,
		value: stored,
	}
	if ttl > 0 {
		item.expiresAt = s.cfg.Now().Add(ttl)
	}

	elem := s.lru.PushFront(item)
	s.entries[key] = elem
	s.memoryBytes += size
	return nil
}

// Get returns the value for key and whether it exists and is not expired.
func (s *Store) Get(key string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	elem, ok := s.entries[key]
	if !ok {
		return nil, false
	}

	item := elem.Value.(*entry)
	if s.isExpired(item) {
		s.removeElement(elem)
		return nil, false
	}

	s.lru.MoveToFront(elem)

	out := make([]byte, len(item.value))
	copy(out, item.value)
	return out, true
}

// Delete removes key from the store and reports whether it existed and was not expired.
func (s *Store) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	elem, ok := s.entries[key]
	if !ok {
		return false
	}

	item := elem.Value.(*entry)
	if s.isExpired(item) {
		s.removeElement(elem)
		return false
	}

	s.removeElement(elem)
	return true
}

// Len returns the number of live keys in the store.
func (s *Store) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.entries)
}

// MemoryBytes returns the tracked memory used by keys and values.
func (s *Store) MemoryBytes() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.memoryBytes
}

// RunCleanup periodically removes expired entries until ctx is canceled.
func (s *Store) RunCleanup(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.cleanupExpired()
		}
	}
}

func (s *Store) cleanupExpired() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.cfg.Now()
	var expired []*list.Element
	for _, elem := range s.entries {
		item := elem.Value.(*entry)
		if item.expiresAt.IsZero() || now.Before(item.expiresAt) {
			continue
		}
		expired = append(expired, elem)
	}
	for _, elem := range expired {
		s.removeElement(elem)
	}
}

func (s *Store) ensureCapacity(needed int64) {
	for s.cfg.MaxBytes > 0 && s.memoryBytes+needed > s.cfg.MaxBytes {
		oldest := s.lru.Back()
		if oldest == nil {
			return
		}
		s.removeElement(oldest)
	}
}

func (s *Store) removeElement(elem *list.Element) {
	item := elem.Value.(*entry)
	s.memoryBytes -= entrySize(item.key, item.value)
	s.lru.Remove(elem)
	delete(s.entries, item.key)
}

func (s *Store) isExpired(item *entry) bool {
	if item.expiresAt.IsZero() {
		return false
	}
	return !s.cfg.Now().Before(item.expiresAt)
}

func entrySize(key string, value []byte) int64 {
	return int64(len(key) + len(value))
}
