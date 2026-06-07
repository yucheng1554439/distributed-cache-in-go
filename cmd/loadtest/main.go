package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/distributed-cache/distributed-cache/internal/metrics"
	"github.com/distributed-cache/distributed-cache/internal/protocol"
)

const maxRedirects = 5

func main() {
	addr := flag.String("addr", "127.0.0.1:6379", "initial cache server address")
	duration := flag.Duration("duration", 10*time.Second, "load test duration")
	concurrency := flag.Int("concurrency", 32, "number of concurrent workers")
	valueSize := flag.Int("value-size", 64, "payload size in bytes for SET")
	getRatio := flag.Float64("get-ratio", 0.8, "fraction of GET operations (0-1)")
	keySpace := flag.Int("key-space", 10000, "number of distinct keys")
	timeout := flag.Duration("timeout", 5*time.Second, "per-request timeout")
	addrMapRaw := flag.String("addr-map", "", "redirect address remap: advertised=reachable,advertised=reachable")
	jsonOut := flag.Bool("json", false, "print results as JSON")
	flag.Parse()

	if *getRatio < 0 || *getRatio > 1 {
		fmt.Fprintln(os.Stderr, "get-ratio must be between 0 and 1")
		os.Exit(2)
	}

	addrMap, err := parseAddrMap(*addrMapRaw)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	value := make([]byte, *valueSize)
	for i := range value {
		value[i] = byte('a' + (i % 26))
	}

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	var (
		errors  atomic.Int64
		wg      sync.WaitGroup
		mu      sync.Mutex
		samples []time.Duration
	)

	start := time.Now()
	for worker := 0; worker < *concurrency; worker++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			rng := rand.New(rand.NewSource(int64(workerID + 1)))
			session := &connSession{addrMap: addrMap}
			defer session.close()

			for ctx.Err() == nil {
				var req protocol.Request
				key := fmt.Sprintf("load-%d", rng.Intn(*keySpace))
				if rng.Float64() < *getRatio {
					req = protocol.Request{Command: protocol.CommandGet, Key: key}
				} else {
					req = protocol.Request{Command: protocol.CommandSet, Key: key, Value: value}
				}

				latency, err := session.roundTripWithRedirects(*addr, *timeout, req)
				if err != nil {
					errors.Add(1)
					continue
				}

				mu.Lock()
				samples = append(samples, latency)
				mu.Unlock()
			}
		}(worker)
	}

	wg.Wait()
	elapsed := time.Since(start)

	result := metrics.SummarizeLoad(elapsed, *concurrency, samples, errors.Load())
	if *jsonOut {
		out, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			fmt.Fprintf(os.Stderr, "marshal result: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(string(out))
		return
	}

	printReport(result)
}

type connSession struct {
	addr    string
	conn    net.Conn
	reader  *bufio.Reader
	addrMap map[string]string
}

func (s *connSession) close() {
	if s.conn != nil {
		_ = s.conn.Close()
		s.conn = nil
		s.reader = nil
	}
}

func (s *connSession) connect(addr string, timeout time.Duration) error {
	s.close()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	dialer := net.Dialer{Timeout: timeout}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("connect %s: %w", addr, err)
	}

	s.addr = addr
	s.conn = conn
	s.reader = bufio.NewReader(conn)
	return nil
}

func parseAddrMap(raw string) (map[string]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}

	out := make(map[string]string)
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		from, to, ok := strings.Cut(part, "=")
		if !ok {
			return nil, fmt.Errorf("invalid addr-map entry %q: expected advertised=reachable", part)
		}
		from = strings.TrimSpace(from)
		to = strings.TrimSpace(to)
		if from == "" || to == "" {
			return nil, fmt.Errorf("invalid addr-map entry %q: empty address", part)
		}
		out[from] = to
	}
	return out, nil
}

func resolveAddr(addr string, addrMap map[string]string) string {
	if addrMap == nil {
		return addr
	}
	if mapped, ok := addrMap[addr]; ok {
		return mapped
	}
	return addr
}

func (s *connSession) roundTripWithRedirects(startAddr string, timeout time.Duration, req protocol.Request) (time.Duration, error) {
	start := time.Now()
	target := resolveAddr(startAddr, s.addrMap)

	for redirects := 0; redirects <= maxRedirects; redirects++ {
		if s.conn == nil || s.addr != target {
			if err := s.connect(target, timeout); err != nil {
				return 0, err
			}
		}

		resp, err := s.roundTripOnce(timeout, req)
		if err != nil {
			s.close()
			return 0, err
		}

		switch resp.Kind {
		case protocol.ResponseMoved:
			if redirects == maxRedirects {
				s.close()
				return 0, fmt.Errorf("too many MOVED redirects")
			}
			if resp.Addr == "" {
				s.close()
				return 0, fmt.Errorf("MOVED response missing address")
			}
			s.close()
			target = resolveAddr(resp.Addr, s.addrMap)
			continue
		case protocol.ResponseNotLeader:
			if redirects == maxRedirects {
				s.close()
				return 0, fmt.Errorf("too many NOT_LEADER redirects")
			}
			s.close()
			if resp.Addr != "" {
				target = resolveAddr(resp.Addr, s.addrMap)
			}
			continue
		case protocol.ResponseOK, protocol.ResponseNotFound, protocol.ResponseValue:
			return time.Since(start), nil
		case protocol.ResponseError:
			return 0, fmt.Errorf("server error: %s", resp.Message)
		default:
			return 0, fmt.Errorf("unexpected response kind %v", resp.Kind)
		}
	}

	s.close()
	return 0, fmt.Errorf("too many redirects")
}

func (s *connSession) roundTripOnce(timeout time.Duration, req protocol.Request) (protocol.Response, error) {
	payload, err := protocol.EncodeRequest(req)
	if err != nil {
		return protocol.Response{}, fmt.Errorf("encode request: %w", err)
	}

	if err := s.conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return protocol.Response{}, fmt.Errorf("set deadline: %w", err)
	}

	if _, err := s.conn.Write(payload); err != nil {
		return protocol.Response{}, fmt.Errorf("write request: %w", err)
	}

	resp, err := protocol.DecodeResponse(s.reader)
	if err != nil {
		return protocol.Response{}, fmt.Errorf("read response: %w", err)
	}
	return resp, nil
}

// roundTripWithRedirects is exported for tests via connSession wrapper.
func roundTripWithRedirects(startAddr string, timeout time.Duration, req protocol.Request, addrMap map[string]string) (time.Duration, error) {
	session := &connSession{addrMap: addrMap}
	defer session.close()
	return session.roundTripWithRedirects(startAddr, timeout, req)
}

func printReport(result metrics.LoadResult) {
	fmt.Printf("Load test results\n")
	fmt.Printf("  duration:     %s\n", result.Duration)
	fmt.Printf("  concurrency:  %d\n", result.Concurrency)
	fmt.Printf("  operations:   %d\n", result.Operations)
	fmt.Printf("  errors:       %d\n", result.Errors)
	fmt.Printf("  throughput:   %.2f ops/sec\n", result.Throughput)
	fmt.Printf("  latency min:  %s\n", result.Latency.Min)
	fmt.Printf("  latency mean: %s\n", result.Latency.Mean)
	fmt.Printf("  latency p50:  %s\n", result.Latency.P50)
	fmt.Printf("  latency p95:  %s\n", result.Latency.P95)
	fmt.Printf("  latency p99:  %s\n", result.Latency.P99)
	fmt.Printf("  latency max:  %s\n", result.Latency.Max)
}
