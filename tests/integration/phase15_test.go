package integration_test

import (
	"os/exec"
	"testing"
	"time"
)

func TestClientTTLExpiration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	clientPath := buildClient(t)
	addr, stop := startServer(t)
	defer stop()

	runClientWithFlags(t, clientPath, addr, []string{"-ttl", "200ms", "SET", "temp", "value"}, "OK\n")
	runClient(t, clientPath, addr, []string{"GET", "temp"}, "VALUE value\n")

	time.Sleep(300 * time.Millisecond)

	runClient(t, clientPath, addr, []string{"GET", "temp"}, "NOT_FOUND\n")
}

func runClientWithFlags(t *testing.T, clientPath, addr string, args []string, wantStdout string) {
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
