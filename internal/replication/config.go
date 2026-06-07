package replication

import (
	"errors"
	"fmt"
)

// WriteConsistency controls how many replicas must acknowledge a write.
type WriteConsistency string

const (
	WriteOne    WriteConsistency = "one"
	WriteQuorum WriteConsistency = "quorum"
	WriteAll    WriteConsistency = "all"
)

// ReadConsistency controls which replicas may serve reads.
type ReadConsistency string

const (
	ReadPrimary ReadConsistency = "primary"
	ReadQuorum  ReadConsistency = "quorum"
	ReadAny     ReadConsistency = "any"
)

// Config controls replication behavior.
type Config struct {
	ReplicationFactor int
	WriteQuorum       int
	ReadQuorum        int
	WriteConsistency  WriteConsistency
	ReadConsistency   ReadConsistency
}

// DefaultConfig returns defaults for a three-node replicated cluster.
func DefaultConfig() Config {
	return Config{
		ReplicationFactor: 3,
		WriteQuorum:       2,
		ReadQuorum:        2,
		WriteConsistency:  WriteQuorum,
		ReadConsistency:   ReadQuorum,
	}
}

// Validate checks replication settings.
func (c Config) Validate() error {
	if c.ReplicationFactor < 1 {
		return errors.New("replication factor must be at least 1")
	}
	if c.ReplicationFactor == 1 {
		return nil
	}
	if c.WriteQuorum < 1 || c.WriteQuorum > c.ReplicationFactor {
		return fmt.Errorf("write quorum must be between 1 and %d", c.ReplicationFactor)
	}
	if c.ReadQuorum < 1 || c.ReadQuorum > c.ReplicationFactor {
		return fmt.Errorf("read quorum must be between 1 and %d", c.ReplicationFactor)
	}
	switch c.WriteConsistency {
	case WriteOne, WriteQuorum, WriteAll:
	default:
		return fmt.Errorf("unsupported write consistency %q", c.WriteConsistency)
	}
	switch c.ReadConsistency {
	case ReadPrimary, ReadQuorum, ReadAny:
	default:
		return fmt.Errorf("unsupported read consistency %q", c.ReadConsistency)
	}
	return nil
}

// Enabled reports whether replication is active.
func (c Config) Enabled() bool {
	return c.ReplicationFactor > 1
}

// Normalize fills in quorum defaults derived from the replication factor.
func (c Config) Normalize() Config {
	if c.ReplicationFactor <= 1 {
		c.ReplicationFactor = 1
		return c
	}
	defaultQuorum := c.ReplicationFactor/2 + 1
	if c.WriteQuorum <= 0 {
		c.WriteQuorum = defaultQuorum
	}
	if c.ReadQuorum <= 0 {
		c.ReadQuorum = defaultQuorum
	}
	if c.WriteConsistency == "" {
		c.WriteConsistency = WriteQuorum
	}
	if c.ReadConsistency == "" {
		c.ReadConsistency = ReadQuorum
	}
	return c
}

// RequiredWriteAcks returns the number of replica acknowledgements required.
func (c Config) RequiredWriteAcks() int {
	switch c.WriteConsistency {
	case WriteOne:
		return 0
	case WriteAll:
		return c.ReplicationFactor - 1
	default:
		return c.WriteQuorum - 1
	}
}
