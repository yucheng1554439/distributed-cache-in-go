package raft

// RequestVoteRequest is sent during leader election.
type RequestVoteRequest struct {
	Term         uint64 `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

// RequestVoteResponse is returned to a candidate.
type RequestVoteResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

// AppendEntriesRequest replicates log entries and heartbeats.
type AppendEntriesRequest struct {
	Term         uint64  `json:"term"`
	LeaderID     string  `json:"leader_id"`
	PrevLogIndex uint64  `json:"prev_log_index"`
	PrevLogTerm  uint64  `json:"prev_log_term"`
	Entries      []Entry `json:"entries"`
	LeaderCommit uint64  `json:"leader_commit"`
}

// AppendEntriesResponse acknowledges log replication.
type AppendEntriesResponse struct {
	Term    uint64 `json:"term"`
	Success bool   `json:"success"`
}

// Status exposes node state for observability.
type Status struct {
	ID           string `json:"id"`
	Role         Role   `json:"role"`
	Term         uint64 `json:"term"`
	LeaderID     string `json:"leader_id"`
	CommitIndex  uint64 `json:"commit_index"`
	LastApplied  uint64 `json:"last_applied"`
	LogLength    int    `json:"log_length"`
}
