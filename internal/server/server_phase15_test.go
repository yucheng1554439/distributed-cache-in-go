package server

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/distributed-cache/distributed-cache/internal/cache"
	"github.com/distributed-cache/distributed-cache/internal/protocol"
)

func TestServerSetWithTTL(t *testing.T) {
	now := time.Date(2026, 6, 5, 12, 0, 0, 0, time.UTC)
	store := cache.NewStore(cache.Config{
		Now: func() time.Time { return now },
	})

	srv, err := New(Config{Addr: "127.0.0.1:0"}, store)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	addr, stop := listenServer(t, srv)
	defer stop()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	setReq, err := protocol.EncodeRequest(protocol.Request{
		Command: protocol.CommandSet,
		Key:     "session",
		Value:   []byte("token"),
		TTL:     time.Second,
	})
	if err != nil {
		t.Fatalf("EncodeRequest() error = %v", err)
	}
	if _, err := conn.Write(setReq); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if resp, err := protocol.DecodeResponse(reader); err != nil || resp.Kind != protocol.ResponseOK {
		t.Fatalf("set response = %+v err = %v", resp, err)
	}

	getReq, err := protocol.EncodeRequest(protocol.Request{
		Command: protocol.CommandGet,
		Key:     "session",
	})
	if err != nil {
		t.Fatalf("EncodeRequest() error = %v", err)
	}
	if _, err := conn.Write(getReq); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if resp, err := protocol.DecodeResponse(reader); err != nil || resp.Kind != protocol.ResponseValue {
		t.Fatalf("get response = %+v err = %v", resp, err)
	}

	now = now.Add(2 * time.Second)

	if _, err := conn.Write(getReq); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if resp, err := protocol.DecodeResponse(reader); err != nil || resp.Kind != protocol.ResponseNotFound {
		t.Fatalf("expired get response = %+v err = %v", resp, err)
	}
}

func TestServerMemoryLimitEviction(t *testing.T) {
	store := cache.NewStore(cache.Config{MaxBytes: 4})

	srv, err := New(Config{Addr: "127.0.0.1:0"}, store)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	addr, stop := listenServer(t, srv)
	defer stop()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	writeRequest := func(req protocol.Request) protocol.Response {
		t.Helper()
		payload, err := protocol.EncodeRequest(req)
		if err != nil {
			t.Fatalf("EncodeRequest() error = %v", err)
		}
		if _, err := conn.Write(payload); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
		resp, err := protocol.DecodeResponse(reader)
		if err != nil {
			t.Fatalf("DecodeResponse() error = %v", err)
		}
		return resp
	}

	if resp := writeRequest(protocol.Request{
		Command: protocol.CommandSet,
		Key:     "a",
		Value:   []byte("1"),
	}); resp.Kind != protocol.ResponseOK {
		t.Fatalf("set a response = %v", resp.Kind)
	}
	if resp := writeRequest(protocol.Request{
		Command: protocol.CommandSet,
		Key:     "b",
		Value:   []byte("2"),
	}); resp.Kind != protocol.ResponseOK {
		t.Fatalf("set b response = %v", resp.Kind)
	}
	if resp := writeRequest(protocol.Request{
		Command: protocol.CommandGet,
		Key:     "a",
	}); resp.Kind != protocol.ResponseValue {
		t.Fatalf("get a response = %v", resp.Kind)
	}
	if resp := writeRequest(protocol.Request{
		Command: protocol.CommandSet,
		Key:     "c",
		Value:   []byte("3"),
	}); resp.Kind != protocol.ResponseOK {
		t.Fatalf("set c response = %v", resp.Kind)
	}
	if resp := writeRequest(protocol.Request{
		Command: protocol.CommandGet,
		Key:     "b",
	}); resp.Kind != protocol.ResponseNotFound {
		t.Fatalf("get b response = %v, want NOT_FOUND", resp.Kind)
	}
}
