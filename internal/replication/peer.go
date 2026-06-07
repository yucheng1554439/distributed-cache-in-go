package replication

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"time"

	"github.com/distributed-cache/distributed-cache/internal/protocol"
)

// Peer sends replication and health-check requests to remote nodes.
type Peer struct {
	Timeout time.Duration
}

// Ping checks whether addr is reachable.
func (p *Peer) Ping(ctx context.Context, addr string) error {
	_, err := p.roundTrip(ctx, addr, protocol.Request{Command: protocol.CommandPing})
	return err
}

// ReplicateSet applies a SET on a remote replica.
func (p *Peer) ReplicateSet(ctx context.Context, addr, key string, value []byte, ttl time.Duration) error {
	_, err := p.roundTrip(ctx, addr, protocol.Request{
		Command: protocol.CommandReplSet,
		Key:     key,
		Value:   value,
		TTL:     ttl,
	})
	return err
}

// ReplicateDelete applies a DELETE on a remote replica.
func (p *Peer) ReplicateDelete(ctx context.Context, addr, key string) error {
	_, err := p.roundTrip(ctx, addr, protocol.Request{
		Command: protocol.CommandReplDelete,
		Key:     key,
	})
	return err
}

// Get reads a key from a remote node.
func (p *Peer) Get(ctx context.Context, addr, key string) ([]byte, bool, error) {
	resp, err := p.roundTrip(ctx, addr, protocol.Request{
		Command: protocol.CommandReplGet,
		Key:     key,
	})
	if err != nil {
		return nil, false, err
	}
	switch resp.Kind {
	case protocol.ResponseValue:
		return resp.Value, true, nil
	case protocol.ResponseNotFound:
		return nil, false, nil
	default:
		return nil, false, fmt.Errorf("unexpected get response: %v", resp.Kind)
	}
}

func (p *Peer) roundTrip(ctx context.Context, addr string, req protocol.Request) (protocol.Response, error) {
	timeout := p.Timeout
	if timeout <= 0 {
		timeout = 2 * time.Second
	}

	payload, err := protocol.EncodeRequest(req)
	if err != nil {
		return protocol.Response{}, err
	}

	dialer := net.Dialer{Timeout: timeout}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return protocol.Response{}, err
	}
	defer conn.Close()

	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(timeout)
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return protocol.Response{}, err
	}

	if _, err := conn.Write(payload); err != nil {
		return protocol.Response{}, err
	}

	resp, err := protocol.DecodeResponse(bufio.NewReader(conn))
	if err != nil {
		return protocol.Response{}, err
	}
	if resp.Kind == protocol.ResponseError {
		return protocol.Response{}, fmt.Errorf("%s", resp.Message)
	}
	return resp, nil
}
