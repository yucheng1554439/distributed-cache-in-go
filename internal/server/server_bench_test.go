package server

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/distributed-cache/distributed-cache/internal/cache"
	"github.com/distributed-cache/distributed-cache/internal/metrics"
	"github.com/distributed-cache/distributed-cache/internal/protocol"
)

func BenchmarkServerDispatchSet(b *testing.B) {
	store := cache.NewStore(cache.DefaultConfig())
	srv, err := New(Config{
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics: metrics.NewRegistry(),
	}, store)
	if err != nil {
		b.Fatalf("New() error = %v", err)
	}

	req := protocol.Request{
		Command: protocol.CommandSet,
		Key:     "benchmark-key",
		Value:   []byte("benchmark-value"),
	}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp := srv.dispatch(ctx, req)
		if resp.Kind != protocol.ResponseOK {
			b.Fatalf("dispatch() kind = %v", resp.Kind)
		}
	}
}

func BenchmarkServerDispatchGetHit(b *testing.B) {
	store := cache.NewStore(cache.DefaultConfig())
	if err := store.Set("benchmark-key", []byte("benchmark-value"), 0); err != nil {
		b.Fatalf("Set() error = %v", err)
	}

	srv, err := New(Config{
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics: metrics.NewRegistry(),
	}, store)
	if err != nil {
		b.Fatalf("New() error = %v", err)
	}

	req := protocol.Request{
		Command: protocol.CommandGet,
		Key:     "benchmark-key",
	}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp := srv.dispatch(ctx, req)
		if resp.Kind != protocol.ResponseValue {
			b.Fatalf("dispatch() kind = %v", resp.Kind)
		}
	}
}
