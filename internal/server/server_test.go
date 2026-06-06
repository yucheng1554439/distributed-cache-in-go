package server

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/distributed-cache/distributed-cache/internal/cache"
	"github.com/distributed-cache/distributed-cache/internal/protocol"
)

func startTestServer(t *testing.T) (addr string, stop func()) {
	t.Helper()

	store := cache.NewStore(cache.DefaultConfig())
	srv, err := New(Config{
		Addr:   "127.0.0.1:0",
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}, store)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	return listenServer(t, srv)
}

func listenServer(t *testing.T, srv *Server) (addr string, stop func()) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- srv.ListenAndServe(ctx)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		addr = srv.Addr()
		if addr != "" && addr != ":6379" && addr != "127.0.0.1:0" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if addr == "" || addr == ":6379" || addr == "127.0.0.1:0" {
		cancel()
		t.Fatal("server did not bind in time")
	}

	stop = func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Log("server shutdown timed out")
		}
	}
	return addr, stop
}

func TestServerSetGetDelete(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	setReq, err := protocol.EncodeRequest(protocol.Request{
		Command: protocol.CommandSet,
		Key:     "name",
		Value:   []byte("cache"),
	})
	if err != nil {
		t.Fatalf("EncodeRequest() error = %v", err)
	}
	if _, err := conn.Write(setReq); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	setResp, err := protocol.DecodeResponse(reader)
	if err != nil {
		t.Fatalf("DecodeResponse() error = %v", err)
	}
	if setResp.Kind != protocol.ResponseOK {
		t.Fatalf("set response = %v, want OK", setResp.Kind)
	}

	getReq, err := protocol.EncodeRequest(protocol.Request{
		Command: protocol.CommandGet,
		Key:     "name",
	})
	if err != nil {
		t.Fatalf("EncodeRequest() error = %v", err)
	}
	if _, err := conn.Write(getReq); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	getResp, err := protocol.DecodeResponse(reader)
	if err != nil {
		t.Fatalf("DecodeResponse() error = %v", err)
	}
	if getResp.Kind != protocol.ResponseValue || string(getResp.Value) != "cache" {
		t.Fatalf("get response = %+v, want value cache", getResp)
	}

	delReq, err := protocol.EncodeRequest(protocol.Request{
		Command: protocol.CommandDelete,
		Key:     "name",
	})
	if err != nil {
		t.Fatalf("EncodeRequest() error = %v", err)
	}
	if _, err := conn.Write(delReq); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	delResp, err := protocol.DecodeResponse(reader)
	if err != nil {
		t.Fatalf("DecodeResponse() error = %v", err)
	}
	if delResp.Kind != protocol.ResponseOK {
		t.Fatalf("delete response = %v, want OK", delResp.Kind)
	}

	missingReq, err := protocol.EncodeRequest(protocol.Request{
		Command: protocol.CommandGet,
		Key:     "name",
	})
	if err != nil {
		t.Fatalf("EncodeRequest() error = %v", err)
	}
	if _, err := conn.Write(missingReq); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	missingResp, err := protocol.DecodeResponse(reader)
	if err != nil {
		t.Fatalf("DecodeResponse() error = %v", err)
	}
	if missingResp.Kind != protocol.ResponseNotFound {
		t.Fatalf("missing response = %v, want NOT_FOUND", missingResp.Kind)
	}
}

func TestServerInvalidRequest(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer conn.Close()

	if _, err := conn.Write([]byte("CMD PING\nKEY foo\nEND\n")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	resp, err := protocol.DecodeResponse(bufio.NewReader(conn))
	if err != nil {
		t.Fatalf("DecodeResponse() error = %v", err)
	}
	if resp.Kind != protocol.ResponseError {
		t.Fatalf("response = %v, want ERROR", resp.Kind)
	}
}

func TestServerConcurrentClients(t *testing.T) {
	addr, stop := startTestServer(t)
	defer stop()

	const clients = 16
	errCh := make(chan error, clients)

	for i := 0; i < clients; i++ {
		go func(id int) {
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				errCh <- err
				return
			}
			defer conn.Close()

			key := "key"
			value := []byte("value")

			setReq, err := protocol.EncodeRequest(protocol.Request{
				Command: protocol.CommandSet,
				Key:     key,
				Value:   value,
			})
			if err != nil {
				errCh <- err
				return
			}
			if _, err := conn.Write(setReq); err != nil {
				errCh <- err
				return
			}

			resp, err := protocol.DecodeResponse(bufio.NewReader(conn))
			if err != nil {
				errCh <- err
				return
			}
			if resp.Kind != protocol.ResponseOK {
				errCh <- fmt.Errorf("client %d: unexpected response %v", id, resp.Kind)
				return
			}
			errCh <- nil
		}(i)
	}

	for i := 0; i < clients; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("concurrent client failed: %v", err)
		}
	}
}
