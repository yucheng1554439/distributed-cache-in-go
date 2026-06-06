package cluster

import "hash/fnv"

// HashFunc maps a string to a position on the hash ring.
type HashFunc func(string) uint32

// HashKey hashes keys and virtual node identifiers using FNV-1a 32-bit.
func HashKey(key string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32()
}
