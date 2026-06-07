package metrics

import (
	"encoding/json"
	"testing"
	"time"
)

func TestComputeLatencySnapshotPercentiles(t *testing.T) {
	t.Parallel()

	samples := make([]time.Duration, 100)
	for i := range samples {
		samples[i] = time.Duration(i+1) * time.Millisecond
	}

	snap := computeLatencySnapshot(samples)
	if snap.Count != 100 {
		t.Fatalf("count = %d, want 100", snap.Count)
	}
	if snap.P50 != 50*time.Millisecond {
		t.Fatalf("p50 = %v, want 50ms", snap.P50)
	}
	if snap.P95 != 95*time.Millisecond {
		t.Fatalf("p95 = %v, want 95ms", snap.P95)
	}
	if snap.P99 != 99*time.Millisecond {
		t.Fatalf("p99 = %v, want 99ms", snap.P99)
	}
}

func TestRegistryObserveAndSnapshot(t *testing.T) {
	t.Parallel()

	reg := NewRegistry()
	reg.Observe("GET", 10*time.Millisecond, false)
	reg.Observe("GET", 20*time.Millisecond, false)
	reg.Observe("SET", 30*time.Millisecond, true)

	snap := reg.Snapshot()
	if snap.Total != 3 {
		t.Fatalf("total = %d, want 3", snap.Total)
	}
	if snap.Errors != 1 {
		t.Fatalf("errors = %d, want 1", snap.Errors)
	}
	if len(snap.Commands) != 2 {
		t.Fatalf("commands = %d, want 2", len(snap.Commands))
	}

	out, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	if len(out) == 0 {
		t.Fatal("expected json output")
	}
}

func TestRegistryReset(t *testing.T) {
	t.Parallel()

	reg := NewRegistry()
	reg.Observe("GET", time.Millisecond, false)
	reg.Reset()

	snap := reg.Snapshot()
	if snap.Total != 0 {
		t.Fatalf("total = %d, want 0", snap.Total)
	}
}
