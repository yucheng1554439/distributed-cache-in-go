package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/distributed-cache/distributed-cache/internal/protocol"
)

const maxRedirects = 5

func main() {
	addr := flag.String("addr", "127.0.0.1:6379", "cache server address")
	timeout := flag.Duration("timeout", 5*time.Second, "request timeout")
	ttl := flag.Duration("ttl", 0, "TTL for SET commands")
	flag.Parse()

	if flag.NArg() < 1 {
		fmt.Fprintf(os.Stderr, "usage: %s [-addr host:port] [-ttl duration] <SET|GET|DEL|OWNER|CLUSTER_MEMBERS|CLUSTER_JOIN|CLUSTER_LEAVE|RAFT_STATUS> [args...]\n", os.Args[0])
		os.Exit(2)
	}

	command := flag.Arg(0)
	req, err := buildRequest(command, flag.Args()[1:], *ttl)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	targetAddr := *addr
	for redirects := 0; redirects <= maxRedirects; redirects++ {
		resp, err := roundTrip(targetAddr, *timeout, req)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			os.Exit(1)
		}

		switch resp.Kind {
		case protocol.ResponseMoved:
			if redirects == maxRedirects {
				fmt.Fprintf(os.Stderr, "too many redirects\n")
				os.Exit(1)
			}
			targetAddr = resp.Addr
			continue
		case protocol.ResponseNotLeader:
			if redirects == maxRedirects {
				fmt.Fprintf(os.Stderr, "too many redirects\n")
				os.Exit(1)
			}
			if resp.Addr != "" {
				targetAddr = resp.Addr
			}
			continue
		case protocol.ResponseOK:
			fmt.Println("OK")
		case protocol.ResponseNotFound:
			fmt.Println("NOT_FOUND")
		case protocol.ResponseValue:
			if command == protocol.CommandRaftStatus {
				fmt.Printf("RAFT_STATUS %s\n", string(resp.Value))
			} else if command == protocol.CommandMetrics {
				fmt.Printf("METRICS %s\n", string(resp.Value))
			} else {
				fmt.Printf("VALUE %s\n", string(resp.Value))
			}
		case protocol.ResponseOwner:
			fmt.Printf("OWNER %s %s\n", resp.NodeID, resp.Addr)
		case protocol.ResponseMembers:
			fmt.Printf("MEMBERS %d\n", len(resp.Members))
			for _, member := range resp.Members {
				fmt.Printf("%s %s\n", member.ID, member.Addr)
			}
		case protocol.ResponseError:
			fmt.Fprintf(os.Stderr, "ERR %s\n", resp.Message)
			os.Exit(1)
		default:
			fmt.Fprintf(os.Stderr, "unexpected response kind %v\n", resp.Kind)
			os.Exit(1)
		}
		return
	}
}

func buildRequest(command string, args []string, ttl time.Duration) (protocol.Request, error) {
	req := protocol.Request{Command: strings.ToUpper(command), TTL: ttl}

	switch req.Command {
	case protocol.CommandSet:
		if len(args) != 2 {
			return protocol.Request{}, fmt.Errorf("SET requires key and value")
		}
		req.Key = args[0]
		req.Value = []byte(args[1])
	case protocol.CommandGet, protocol.CommandDelete, protocol.CommandOwner, protocol.CommandClusterLeave:
		if len(args) != 1 {
			return protocol.Request{}, fmt.Errorf("%s requires key", req.Command)
		}
		req.Key = args[0]
	case protocol.CommandClusterMembers:
		if len(args) != 0 {
			return protocol.Request{}, fmt.Errorf("CLUSTER_MEMBERS accepts no arguments")
		}
	case protocol.CommandMetrics:
		if len(args) != 0 {
			return protocol.Request{}, fmt.Errorf("METRICS accepts no arguments")
		}
	case protocol.CommandRaftStatus:
		if len(args) != 0 {
			return protocol.Request{}, fmt.Errorf("RAFT_STATUS accepts no arguments")
		}
	case protocol.CommandClusterJoin:
		if len(args) != 2 {
			return protocol.Request{}, fmt.Errorf("CLUSTER_JOIN requires node-id and address")
		}
		req.Key = args[0]
		req.Value = []byte(args[1])
	default:
		return protocol.Request{}, fmt.Errorf("unsupported command %q", command)
	}

	return req, nil
}

func roundTrip(addr string, timeout time.Duration, req protocol.Request) (protocol.Response, error) {
	payload, err := protocol.EncodeRequest(req)
	if err != nil {
		return protocol.Response{}, fmt.Errorf("encode request: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	dialer := net.Dialer{Timeout: timeout}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return protocol.Response{}, fmt.Errorf("connect: %w", err)
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return protocol.Response{}, fmt.Errorf("set deadline: %w", err)
	}

	if _, err := conn.Write(payload); err != nil {
		return protocol.Response{}, fmt.Errorf("write request: %w", err)
	}

	resp, err := protocol.DecodeResponse(bufio.NewReader(conn))
	if err != nil {
		return protocol.Response{}, fmt.Errorf("read response: %w", err)
	}
	return resp, nil
}
