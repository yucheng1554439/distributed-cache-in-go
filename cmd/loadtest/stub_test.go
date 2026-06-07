package main

import (
	"bufio"
	"net"
	"sync"

	"github.com/distributed-cache/distributed-cache/internal/protocol"
)

type stubListener struct {
	net.Listener
	batches [][]protocol.Response
	mu      sync.Mutex
}

func netListenStub(batches ...[]protocol.Response) (net.Listener, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	stub := &stubListener{Listener: ln, batches: batches}
	go stub.serve()
	return stub, nil
}

func (s *stubListener) serve() {
	for {
		conn, err := s.Listener.Accept()
		if err != nil {
			return
		}
		go s.handleConn(conn)
	}
}

func (s *stubListener) nextBatch() []protocol.Response {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.batches) == 0 {
		return []protocol.Response{{Kind: protocol.ResponseOK}}
	}
	batch := s.batches[0]
	s.batches = s.batches[1:]
	return batch
}

func (s *stubListener) handleConn(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	for _, resp := range s.nextBatch() {
		if _, err := protocol.DecodeRequest(reader); err != nil {
			return
		}
		payload, err := protocol.EncodeResponse(resp)
		if err != nil {
			return
		}
		if _, err := conn.Write(payload); err != nil {
			return
		}
	}
}

func (s *stubListener) Close() error {
	return s.Listener.Close()
}

func (s *stubListener) Addr() net.Addr {
	return s.Listener.Addr()
}
