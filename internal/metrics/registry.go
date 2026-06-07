package metrics

import (
	"sort"
	"sync"
	"time"
)

const defaultMaxSamples = 8192

// LatencySnapshot summarizes observed request latencies.
type LatencySnapshot struct {
	Count int64         `json:"count"`
	Min   time.Duration `json:"min"`
	Max   time.Duration `json:"max"`
	Mean  time.Duration `json:"mean"`
	P50   time.Duration `json:"p50"`
	P95   time.Duration `json:"p95"`
	P99   time.Duration `json:"p99"`
}

// CommandSnapshot contains metrics for one command type.
type CommandSnapshot struct {
	Command  string          `json:"command"`
	Requests int64           `json:"requests"`
	Errors   int64           `json:"errors"`
	Latency  LatencySnapshot `json:"latency"`
}

// Snapshot is a point-in-time metrics report.
type Snapshot struct {
	GeneratedAt time.Time         `json:"generated_at"`
	Total       int64             `json:"total_requests"`
	Errors      int64             `json:"total_errors"`
	Commands    []CommandSnapshot `json:"commands"`
}

// Registry collects request counters and latency samples.
type Registry struct {
	mu       sync.RWMutex
	commands map[string]*commandMetrics
	total    int64
	errors   int64
	maxSamples int
}

type commandMetrics struct {
	requests int64
	errors   int64
	latency  *latencyTracker
}

type latencyTracker struct {
	mu       sync.Mutex
	samples  []time.Duration
	capacity int
}

// NewRegistry creates a metrics registry.
func NewRegistry() *Registry {
	return &Registry{
		commands:   make(map[string]*commandMetrics),
		maxSamples: defaultMaxSamples,
	}
}

// Observe records one request outcome.
func (r *Registry) Observe(command string, latency time.Duration, isError bool) {
	r.mu.Lock()
	metrics, ok := r.commands[command]
	if !ok {
		metrics = &commandMetrics{
			latency: &latencyTracker{capacity: r.maxSamples},
		}
		r.commands[command] = metrics
	}
	r.total++
	metrics.requests++
	if isError {
		r.errors++
		metrics.errors++
	}
	r.mu.Unlock()

	metrics.latency.observe(latency)
}

// Snapshot returns a copy of current metrics.
func (r *Registry) Snapshot() Snapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()

	report := Snapshot{
		GeneratedAt: time.Now().UTC(),
		Total:       r.total,
		Errors:      r.errors,
		Commands:    make([]CommandSnapshot, 0, len(r.commands)),
	}

	for command, metrics := range r.commands {
		report.Commands = append(report.Commands, CommandSnapshot{
			Command:  command,
			Requests: metrics.requests,
			Errors:   metrics.errors,
			Latency:  metrics.latency.snapshot(),
		})
	}

	sort.Slice(report.Commands, func(i, j int) bool {
		return report.Commands[i].Command < report.Commands[j].Command
	})
	return report
}

// Reset clears all collected metrics.
func (r *Registry) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.commands = make(map[string]*commandMetrics)
	r.total = 0
	r.errors = 0
}

func (t *latencyTracker) observe(d time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.samples) < t.capacity {
		t.samples = append(t.samples, d)
		return
	}
	t.samples = append(t.samples[:0], t.samples[1:]...)
	t.samples = append(t.samples, d)
}

func (t *latencyTracker) snapshot() LatencySnapshot {
	t.mu.Lock()
	defer t.mu.Unlock()
	return computeLatencySnapshot(t.samples)
}

func computeLatencySnapshot(samples []time.Duration) LatencySnapshot {
	if len(samples) == 0 {
		return LatencySnapshot{}
	}

	sorted := append([]time.Duration(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	var total time.Duration
	for _, sample := range sorted {
		total += sample
	}

	return LatencySnapshot{
		Count: int64(len(sorted)),
		Min:   sorted[0],
		Max:   sorted[len(sorted)-1],
		Mean:  total / time.Duration(len(sorted)),
		P50:   percentile(sorted, 0.50),
		P95:   percentile(sorted, 0.95),
		P99:   percentile(sorted, 0.99),
	}
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 1 {
		return sorted[len(sorted)-1]
	}

	rank := int(float64(len(sorted)-1) * p)
	return sorted[rank]
}
