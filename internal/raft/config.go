package raft

import "time"

// Config controls Raft timing and behavior.
type Config struct {
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
	RPCTimeout         time.Duration
}

// DefaultConfig returns sensible Raft timing defaults.
func DefaultConfig() Config {
	return Config{
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
		RPCTimeout:         500 * time.Millisecond,
	}
}

func (c Config) normalize() Config {
	if c.ElectionTimeoutMin <= 0 {
		c.ElectionTimeoutMin = DefaultConfig().ElectionTimeoutMin
	}
	if c.ElectionTimeoutMax <= c.ElectionTimeoutMin {
		c.ElectionTimeoutMax = c.ElectionTimeoutMin + 150*time.Millisecond
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = DefaultConfig().HeartbeatInterval
	}
	if c.RPCTimeout <= 0 {
		c.RPCTimeout = DefaultConfig().RPCTimeout
	}
	return c
}
