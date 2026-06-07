package raft

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/distributed-cache/distributed-cache/internal/protocol"
)

// TCPTransport sends Raft RPCs over the cache TCP protocol.
type TCPTransport struct {
	Timeout time.Duration
}

// RequestVote sends a RequestVote RPC to addr.
func (t *TCPTransport) RequestVote(ctx context.Context, addr string, req RequestVoteRequest) (RequestVoteResponse, error) {
	payload, err := json.Marshal(req)
	if err != nil {
		return RequestVoteResponse{}, err
	}
	respPayload, err := t.roundTrip(ctx, addr, protocol.CommandRaftRequestVote, payload)
	if err != nil {
		return RequestVoteResponse{}, err
	}
	var resp RequestVoteResponse
	if err := json.Unmarshal(respPayload, &resp); err != nil {
		return RequestVoteResponse{}, err
	}
	return resp, nil
}

// AppendEntries sends an AppendEntries RPC to addr.
func (t *TCPTransport) AppendEntries(ctx context.Context, addr string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	payload, err := json.Marshal(req)
	if err != nil {
		return AppendEntriesResponse{}, err
	}
	respPayload, err := t.roundTrip(ctx, addr, protocol.CommandRaftAppendEntries, payload)
	if err != nil {
		return AppendEntriesResponse{}, err
	}
	var resp AppendEntriesResponse
	if err := json.Unmarshal(respPayload, &resp); err != nil {
		return AppendEntriesResponse{}, err
	}
	return resp, nil
}

func (t *TCPTransport) roundTrip(ctx context.Context, addr, command string, payload []byte) ([]byte, error) {
	timeout := t.Timeout
	if timeout <= 0 {
		timeout = 500 * time.Millisecond
	}

	req := protocol.Request{
		Command: command,
		Key:     "raft",
		Value:   payload,
	}
	encoded, err := protocol.EncodeRequest(req)
	if err != nil {
		return nil, err
	}

	dialer := net.Dialer{Timeout: timeout}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(timeout)
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return nil, err
	}

	if _, err := conn.Write(encoded); err != nil {
		return nil, err
	}

	resp, err := protocol.DecodeResponse(bufio.NewReader(conn))
	if err != nil {
		return nil, err
	}
	if resp.Kind == protocol.ResponseError {
		return nil, fmt.Errorf("%s", resp.Message)
	}
	if resp.Kind != protocol.ResponseValue {
		return nil, fmt.Errorf("unexpected raft response kind %v", resp.Kind)
	}
	return resp.Value, nil
}
