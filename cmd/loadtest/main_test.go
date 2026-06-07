package main

import (
	"net"
	"testing"
	"time"

	"github.com/distributed-cache/distributed-cache/internal/protocol"
)

func TestParseAddrMap(t *testing.T) {
	t.Parallel()

	m, err := parseAddrMap("node-b:6379=127.0.0.1:6380,node-c:6379=127.0.0.1:6381")
	if err != nil {
		t.Fatalf("parseAddrMap() error = %v", err)
	}
	if m["node-b:6379"] != "127.0.0.1:6380" {
		t.Fatalf("node-b mapping = %q", m["node-b:6379"])
	}
}

func TestResolveAddr(t *testing.T) {
	t.Parallel()

	m := map[string]string{"node-a:6379": "127.0.0.1:6379"}
	if got := resolveAddr("node-a:6379", m); got != "127.0.0.1:6379" {
		t.Fatalf("resolveAddr() = %q", got)
	}
	if got := resolveAddr("127.0.0.1:6379", m); got != "127.0.0.1:6379" {
		t.Fatalf("resolveAddr() = %q", got)
	}
}

func TestRoundTripWithRedirectsMoved(t *testing.T) {
	ln, err := netListenStub(
		[]protocol.Response{{Kind: protocol.ResponseMoved, NodeID: "node-b", Addr: "placeholder"}},
		[]protocol.Response{{Kind: protocol.ResponseOK}},
	)
	if err != nil {
		t.Fatalf("netListenStub() error = %v", err)
	}
	defer ln.Close()

	target := ln.Addr().String()
	ln.(*stubListener).batches[0][0].Addr = target

	_, err = roundTripWithRedirects(target, time.Second, protocol.Request{
		Command: protocol.CommandGet,
		Key:     "k",
	}, nil)
	if err != nil {
		t.Fatalf("roundTripWithRedirects() error = %v", err)
	}
}

func TestRoundTripWithRedirectsNotLeader(t *testing.T) {
	ln, err := netListenStub(
		[]protocol.Response{{Kind: protocol.ResponseNotLeader, NodeID: "node-a", Addr: "placeholder"}},
		[]protocol.Response{{Kind: protocol.ResponseOK}},
	)
	if err != nil {
		t.Fatalf("netListenStub() error = %v", err)
	}
	defer ln.Close()

	target := ln.Addr().String()
	ln.(*stubListener).batches[0][0].Addr = target

	_, err = roundTripWithRedirects(target, time.Second, protocol.Request{
		Command: protocol.CommandGet,
		Key:     "k",
	}, nil)
	if err != nil {
		t.Fatalf("roundTripWithRedirects() error = %v", err)
	}
}

func TestRoundTripWithRedirectsTimeout(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		time.Sleep(200 * time.Millisecond)
	}()

	_, err = roundTripWithRedirects(ln.Addr().String(), 50*time.Millisecond, protocol.Request{
		Command: protocol.CommandGet,
		Key:     "k",
	}, nil)
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestRoundTripWithRedirectsRedirectLoop(t *testing.T) {
	batches := make([][]protocol.Response, maxRedirects+2)
	target := "127.0.0.1:1"
	for i := range batches {
		batches[i] = []protocol.Response{{Kind: protocol.ResponseMoved, NodeID: "node-b", Addr: target}}
	}
	ln, err := netListenStub(batches...)
	if err != nil {
		t.Fatalf("netListenStub() error = %v", err)
	}
	defer ln.Close()

	target = ln.Addr().String()
	for i := range ln.(*stubListener).batches {
		ln.(*stubListener).batches[i][0].Addr = target
	}

	_, err = roundTripWithRedirects(target, time.Second, protocol.Request{
		Command: protocol.CommandGet,
		Key:     "k",
	}, nil)
	if err == nil {
		t.Fatal("expected redirect loop error")
	}
}

func TestRoundTripWithRedirectsAddrMap(t *testing.T) {
	ln, err := netListenStub(
		[]protocol.Response{{Kind: protocol.ResponseMoved, NodeID: "node-b", Addr: "node-b:6379"}},
		[]protocol.Response{{Kind: protocol.ResponseOK}},
	)
	if err != nil {
		t.Fatalf("netListenStub() error = %v", err)
	}
	defer ln.Close()

	redirectTarget := ln.Addr().String()
	_, err = roundTripWithRedirects(ln.Addr().String(), time.Second, protocol.Request{
		Command: protocol.CommandGet,
		Key:     "k",
	}, map[string]string{"node-b:6379": redirectTarget})
	if err != nil {
		t.Fatalf("roundTripWithRedirects() error = %v", err)
	}
}
