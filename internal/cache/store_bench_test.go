package cache

import (
	"fmt"
	"testing"
)

func BenchmarkStoreSet(b *testing.B) {
	store := NewStore(DefaultConfig())
	value := []byte("benchmark-value")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i%10000)
		if err := store.Set(key, value, 0); err != nil {
			b.Fatalf("Set() error = %v", err)
		}
	}
}

func BenchmarkStoreGetHit(b *testing.B) {
	store := NewStore(DefaultConfig())
	value := []byte("benchmark-value")
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key-%d", i)
		if err := store.Set(key, value, 0); err != nil {
			b.Fatalf("Set() error = %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i%10000)
		if _, ok := store.Get(key); !ok {
			b.Fatalf("Get(%q) miss", key)
		}
	}
}

func BenchmarkStoreGetMiss(b *testing.B) {
	store := NewStore(DefaultConfig())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("missing-%d", i)
		store.Get(key)
	}
}

func BenchmarkStoreDelete(b *testing.B) {
	store := NewStore(DefaultConfig())
	value := []byte("benchmark-value")
	keys := make([]string, b.N)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
		if err := store.Set(keys[i], value, 0); err != nil {
			b.Fatalf("Set() error = %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.Delete(keys[i])
	}
}
