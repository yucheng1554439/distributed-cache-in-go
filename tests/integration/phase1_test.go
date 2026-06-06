package integration_test

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/distributed-cache/distributed-cache/internal/cache"
	"github.com/distributed-cache/distributed-cache/internal/protocol"
	"github.com/distributed-cache/distributed-cache/internal/server"
)

func startServer(t *testing.T) (addr string, stop func()) {
	t.Helper()

	store := cache.NewStore(cache.DefaultConfig())
	srv, err := server.New(server.Config{
		Addr:   "127.0.0.1:0",
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}, store)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		_ = srv.ListenAndServe(ctx)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		addr = srv.Addr()
		if addr != "" && addr != "127.0.0.1:0" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if addr == "" || addr == "127.0.0.1:0" {
		cancel()
		t.Fatal("server did not bind")
	}

	return addr, cancel
}

func TestProtocolOverTCP(t *testing.T) {
	addr, stop := startServer(t)
	defer stop()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	for _, step := range []struct {
		req  protocol.Request
		kind protocol.ResponseKind
	}{
		{
			req:  protocol.Request{Command: protocol.CommandSet, Key: "k", Value: []byte("v")},
			kind: protocol.ResponseOK,
		},
		{
			req:  protocol.Request{Command: protocol.CommandGet, Key: "k"},
			kind: protocol.ResponseValue,
		},
		{
			req:  protocol.Request{Command: protocol.CommandDelete, Key: "k"},
			kind: protocol.ResponseOK,
		},
	} {
		payload, err := protocol.EncodeRequest(step.req)
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
		if resp.Kind != step.kind {
			t.Fatalf("response kind = %v, want %v", resp.Kind, step.kind)
		}
	}
}

func TestClientBinaryRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clientPath := buildClient(t)
	addr, stop := startServer(t)
	defer stop()

	runClient(t, clientPath, addr, []string{"SET", "session", "abc123"}, "OK\n")
	runClient(t, clientPath, addr, []string{"GET", "session"}, "VALUE abc123\n")
	runClient(t, clientPath, addr, []string{"DEL", "session"}, "OK\n")
	runClient(t, clientPath, addr, []string{"GET", "session"}, "NOT_FOUND\n")
}

func buildClient(t *testing.T) string {
	t.Helper()

	bin := filepath.Join(t.TempDir(), "client")
	if os.PathSeparator == '\\' {
		bin += ".exe"
	}

	cmd := exec.Command("go", "build", "-o", bin, "./cmd/client")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Skipf("go build client failed (is Go installed?): %v\n%s", err, out)
	}

	return bin
}

func runClient(t *testing.T, clientPath, addr string, args []string, wantStdout string) {
	t.Helper()

	fullArgs := append([]string{"-addr", addr}, args...)
	cmd := exec.Command(clientPath, fullArgs...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("client %v error = %v output = %q", fullArgs, err, out)
	}
	if string(out) != wantStdout {
		t.Fatalf("client output = %q, want %q", out, wantStdout)
	}
}
