package cache

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestStoreSetGetDelete(t *testing.T) {
	t.Parallel()

	store := NewStore(DefaultConfig())

	if err := store.Set("foo", []byte("bar"), 0); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	value, ok := store.Get("foo")
	if !ok {
		t.Fatal("expected key to exist")
	}
	if string(value) != "bar" {
		t.Fatalf("got %q, want %q", value, "bar")
	}

	if !store.Delete("foo") {
		t.Fatal("expected delete to succeed")
	}
	if _, ok := store.Get("foo"); ok {
		t.Fatal("expected key to be deleted")
	}
	if store.Delete("foo") {
		t.Fatal("expected second delete to report missing key")
	}
}

func TestStoreGetMissing(t *testing.T) {
	t.Parallel()

	store := NewStore(DefaultConfig())
	if _, ok := store.Get("missing"); ok {
		t.Fatal("expected missing key")
	}
}

func TestStoreSetOverwrites(t *testing.T) {
	t.Parallel()

	store := NewStore(DefaultConfig())
	if err := store.Set("key", []byte("first"), 0); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	if err := store.Set("key", []byte("second"), 0); err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	value, ok := store.Get("key")
	if !ok {
		t.Fatal("expected key to exist")
	}
	if string(value) != "second" {
		t.Fatalf("got %q, want %q", value, "second")
	}
}

func TestStoreReturnsCopy(t *testing.T) {
	t.Parallel()

	store := NewStore(DefaultConfig())
	if err := store.Set("key", []byte("value"), 0); err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	value, ok := store.Get("key")
	if !ok {
		t.Fatal("expected key to exist")
	}
	value[0] = 'X'

	stored, ok := store.Get("key")
	if !ok {
		t.Fatal("expected key to exist")
	}
	if string(stored) != "value" {
		t.Fatalf("store mutated through returned slice: %q", stored)
	}
}

func TestStoreConcurrentAccess(t *testing.T) {
	t.Parallel()

	store := NewStore(DefaultConfig())
	const workers = 32
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(workers)

	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			key := "key"
			for j := 0; j < iterations; j++ {
				_ = store.Set(key, []byte("value"), 0)
				store.Get(key)
				store.Delete(key)
			}
		}()
	}

	wg.Wait()
}

func TestStoreTTLExpiresOnGet(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 5, 12, 0, 0, 0, time.UTC)
	store := NewStore(Config{
		Now: func() time.Time { return now },
	})

	if err := store.Set("temp", []byte("value"), time.Second); err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	if _, ok := store.Get("temp"); !ok {
		t.Fatal("expected key before expiry")
	}

	now = now.Add(2 * time.Second)
	if _, ok := store.Get("temp"); ok {
		t.Fatal("expected expired key to be missing")
	}
	if store.Len() != 0 {
		t.Fatalf("len = %d, want 0", store.Len())
	}
}

func TestStoreTTLExpiresOnDelete(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 5, 12, 0, 0, 0, time.UTC)
	store := NewStore(Config{
		Now: func() time.Time { return now },
	})

	if err := store.Set("temp", []byte("value"), time.Second); err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	now = now.Add(2 * time.Second)
	if store.Delete("temp") {
		t.Fatal("expected delete on expired key to report missing")
	}
}

func TestStoreBackgroundCleanup(t *testing.T) {
	now := time.Date(2026, 6, 5, 12, 0, 0, 0, time.UTC)
	store := NewStore(Config{
		CleanupInterval: 10 * time.Millisecond,
		Now:             func() time.Time { return now },
	})

	if err := store.Set("a", []byte("1"), 50*time.Millisecond); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	if err := store.Set("b", []byte("2"), 0); err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go store.RunCleanup(ctx)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		now = now.Add(20 * time.Millisecond)
		if store.Len() == 1 {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}

	if store.Len() != 1 {
		t.Fatalf("len = %d, want 1 after cleanup", store.Len())
	}
	if _, ok := store.Get("b"); !ok {
		t.Fatal("expected non-expiring key to remain")
	}
}

func TestStoreLRUEviction(t *testing.T) {
	t.Parallel()

	store := NewStore(Config{
		MaxBytes: 4,
	})

	if err := store.Set("a", []byte("1"), 0); err != nil {
		t.Fatalf("Set(a) error = %v", err)
	}
	if err := store.Set("b", []byte("2"), 0); err != nil {
		t.Fatalf("Set(b) error = %v", err)
	}

	if _, ok := store.Get("a"); !ok {
		t.Fatal("expected a to exist before eviction")
	}

	if err := store.Set("c", []byte("3"), 0); err != nil {
		t.Fatalf("Set(c) error = %v", err)
	}

	if _, ok := store.Get("b"); ok {
		t.Fatal("expected LRU key b to be evicted")
	}
	if _, ok := store.Get("a"); !ok {
		t.Fatal("expected recently accessed a to remain")
	}
	if _, ok := store.Get("c"); !ok {
		t.Fatal("expected c to remain")
	}
}

func TestStoreEntryTooLarge(t *testing.T) {
	t.Parallel()

	store := NewStore(Config{MaxBytes: 4})
	err := store.Set("key", []byte("value"), 0)
	if err != ErrEntryTooLarge {
		t.Fatalf("Set() error = %v, want ErrEntryTooLarge", err)
	}
}

func TestStoreMemoryBytes(t *testing.T) {
	t.Parallel()

	store := NewStore(DefaultConfig())
	if err := store.Set("ab", []byte("cd"), 0); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	if got := store.MemoryBytes(); got != 4 {
		t.Fatalf("MemoryBytes() = %d, want 4", got)
	}

	if err := store.Set("ab", []byte("efg"), 0); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	if got := store.MemoryBytes(); got != 5 {
		t.Fatalf("MemoryBytes() = %d, want 5", got)
	}
}

func TestStoreGetPromotesLRU(t *testing.T) {
	t.Parallel()

	store := NewStore(Config{MaxBytes: 5})

	_ = store.Set("a", []byte("1"), 0) // a:2
	_ = store.Set("b", []byte("2"), 0) // b:2
	_, _ = store.Get("a")                // promote a
	_ = store.Set("c", []byte("3"), 0) // evict b

	if _, ok := store.Get("b"); ok {
		t.Fatal("expected b to be evicted")
	}
	if _, ok := store.Get("a"); !ok {
		t.Fatal("expected a to remain")
	}
}
