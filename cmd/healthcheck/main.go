package main

import (
	"fmt"
	"net/http"
	"os"
	"time"
)

func main() {
	target := os.Getenv("HEALTHCHECK_URL")
	if target == "" {
		target = "http://127.0.0.1:6060/healthz"
	}

	timeout := 3 * time.Second
	if raw := os.Getenv("HEALTHCHECK_TIMEOUT"); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil {
			timeout = parsed
		}
	}

	client := &http.Client{Timeout: timeout}
	resp, err := client.Get(target)
	if err != nil {
		fmt.Fprintf(os.Stderr, "healthcheck request failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "healthcheck status = %d, want 200\n", resp.StatusCode)
		os.Exit(1)
	}
}
