package raft

import "context"

// Transport sends Raft RPCs to peer nodes.
type Transport interface {
	RequestVote(ctx context.Context, addr string, req RequestVoteRequest) (RequestVoteResponse, error)
	AppendEntries(ctx context.Context, addr string, req AppendEntriesRequest) (AppendEntriesResponse, error)
}
