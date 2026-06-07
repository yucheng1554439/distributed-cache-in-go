package metrics

import (
	"sort"
	"time"
)

// LoadResult summarizes a load test run.
type LoadResult struct {
	Duration     time.Duration   `json:"duration"`
	Concurrency  int             `json:"concurrency"`
	Operations   int64           `json:"operations"`
	Errors       int64           `json:"errors"`
	Throughput   float64         `json:"throughput_ops_per_sec"`
	Latency      LatencySnapshot `json:"latency"`
}

// SummarizeLoad computes throughput and latency percentiles from samples.
func SummarizeLoad(duration time.Duration, concurrency int, samples []time.Duration, errors int64) LoadResult {
	throughput := 0.0
	if duration > 0 {
		throughput = float64(len(samples)) / duration.Seconds()
	}

	return LoadResult{
		Duration:    duration,
		Concurrency: concurrency,
		Operations:  int64(len(samples)),
		Errors:      errors,
		Throughput:  throughput,
		Latency:     computeLatencySnapshot(samples),
	}
}

// CollectLatencies sorts durations for stable reporting.
func CollectLatencies(samples []time.Duration) []time.Duration {
	out := append([]time.Duration(nil), samples...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
