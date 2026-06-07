package server

import (
	"log/slog"
	"sync/atomic"
)

var traceSeq atomic.Uint64

func (s *Server) trace(remote, stage, detail string) {
	if s == nil || s.cfg.Logger == nil {
		return
	}
	id := traceSeq.Add(1)
	s.cfg.Logger.Debug("request trace",
		"trace_id", id,
		"remote", remote,
		"stage", stage,
		"detail", detail,
	)
}

// EnableRequestTrace turns on debug-level request tracing for a server config.
func EnableRequestTrace(cfg *Config) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	cfg.Logger = cfg.Logger.With("request_trace", true)
}
