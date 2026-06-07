package raft

import "time"

// CommandType identifies a replicated state machine operation.
type CommandType string

const (
	CommandNoop         CommandType = "noop"
	CommandAddMember    CommandType = "add_member"
	CommandRemoveMember CommandType = "remove_member"
)

// Command is an entry applied to the replicated state machine.
type Command struct {
	Type   CommandType   `json:"type"`
	NodeID string        `json:"node_id,omitempty"`
	Addr   string        `json:"addr,omitempty"`
	Key    string        `json:"key,omitempty"`
	Value  []byte        `json:"value,omitempty"`
	TTL    time.Duration `json:"ttl,omitempty"`
}

// Entry is a Raft log entry.
type Entry struct {
	Term    uint64  `json:"term"`
	Index   uint64  `json:"index"`
	Command Command `json:"command"`
}

// Role is the Raft node role.
type Role string

const (
	RoleFollower  Role = "follower"
	RoleCandidate Role = "candidate"
	RoleLeader    Role = "leader"
)
