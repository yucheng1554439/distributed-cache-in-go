package cluster

import (
	"fmt"
	"strings"
)

// Node identifies a cache cluster member.
type Node struct {
	ID   string
	Addr string
}

// ParsePeers parses peer descriptors in the form "id=host:port,id=host:port".
func ParsePeers(raw string) ([]Node, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}

	parts := strings.Split(raw, ",")
	peers := make([]Node, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		id, addr, ok := strings.Cut(part, "=")
		if !ok {
			return nil, fmt.Errorf("invalid peer %q: expected id=host:port", part)
		}

		id = strings.TrimSpace(id)
		addr = strings.TrimSpace(addr)
		if id == "" || addr == "" {
			return nil, fmt.Errorf("invalid peer %q: empty id or address", part)
		}
		if _, exists := seen[id]; exists {
			return nil, fmt.Errorf("duplicate peer id %q", id)
		}

		seen[id] = struct{}{}
		peers = append(peers, Node{ID: id, Addr: addr})
	}

	return peers, nil
}
