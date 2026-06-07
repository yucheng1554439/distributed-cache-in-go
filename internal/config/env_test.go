package config

import "testing"

func TestStringFromEnv(t *testing.T) {
	t.Setenv("CACHE_NODE_ID", "node-a")
	if got := StringFromEnv("", "CACHE_NODE_ID"); got != "node-a" {
		t.Fatalf("StringFromEnv() = %q, want node-a", got)
	}
	if got := StringFromEnv("explicit", "CACHE_NODE_ID"); got != "explicit" {
		t.Fatalf("StringFromEnv() = %q, want explicit", got)
	}
}

func TestBoolFromEnv(t *testing.T) {
	t.Setenv("CACHE_RAFT", "true")
	if got := BoolFromEnv(false, "CACHE_RAFT"); !got {
		t.Fatal("BoolFromEnv() = false, want true")
	}
}

func TestIntFromEnv(t *testing.T) {
	t.Setenv("CACHE_REPLICATION_FACTOR", "3")
	if got := IntFromEnv(1, "CACHE_REPLICATION_FACTOR"); got != 3 {
		t.Fatalf("IntFromEnv() = %d, want 3", got)
	}
}
