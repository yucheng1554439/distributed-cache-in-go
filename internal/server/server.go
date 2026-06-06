package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/distributed-cache/distributed-cache/internal/cache"
	"github.com/distributed-cache/distributed-cache/internal/cluster"
	"github.com/distributed-cache/distributed-cache/internal/protocol"
)

const defaultMaxConnections = 1024

// Config controls server behavior.
type Config struct {
	Addr            string
	MaxConnections  int
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	ShutdownTimeout time.Duration
	Logger          *slog.Logger
	Cluster         *cluster.Cluster
}

// Server serves cache commands over TCP.
type Server struct {
	cfg   Config
	store *cache.Store

	listener net.Listener
	wg       sync.WaitGroup

	connSem chan struct{}
}

// New creates a Server backed by store.
func New(cfg Config, store *cache.Store) (*Server, error) {
	if cfg.Addr == "" {
		cfg.Addr = ":6379"
	}
	if cfg.MaxConnections <= 0 {
		cfg.MaxConnections = defaultMaxConnections
	}
	if cfg.ReadTimeout <= 0 {
		cfg.ReadTimeout = 30 * time.Second
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = 30 * time.Second
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 10 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if store == nil {
		return nil, errors.New("store is required")
	}

	return &Server{
		cfg:     cfg,
		store:   store,
		connSem: make(chan struct{}, cfg.MaxConnections),
	}, nil
}

// ListenAndServe starts accepting TCP connections until ctx is canceled.
func (s *Server) ListenAndServe(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", s.cfg.Addr, err)
	}
	s.listener = listener
	s.cfg.Logger.Info("cache server listening", "addr", listener.Addr().String())

	serveCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		<-ctx.Done()
		s.cfg.Logger.Info("shutdown requested")
		_ = listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-serveCtx.Done():
				s.wg.Wait()
				return nil
			default:
			}

			if errors.Is(err, net.ErrClosed) {
				s.wg.Wait()
				return nil
			}
			s.cfg.Logger.Error("accept failed", "error", err)
			continue
		}

		select {
		case s.connSem <- struct{}{}:
		case <-serveCtx.Done():
			_ = conn.Close()
			s.wg.Wait()
			return nil
		default:
			s.cfg.Logger.Warn("connection limit reached", "max", s.cfg.MaxConnections)
			_ = conn.Close()
			continue
		}

		s.wg.Add(1)
		go func(c net.Conn) {
			defer s.wg.Done()
			defer func() { <-s.connSem }()
			s.handleConn(serveCtx, c)
		}(conn)
	}
}

func (s *Server) handleConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	remote := conn.RemoteAddr().String()
	s.cfg.Logger.Info("client connected", "remote", remote)
	defer s.cfg.Logger.Info("client disconnected", "remote", remote)

	reader := bufio.NewReader(conn)

	for {
		if deadline, ok := ctx.Deadline(); ok {
			_ = conn.SetReadDeadline(deadline)
		} else if s.cfg.ReadTimeout > 0 {
			_ = conn.SetReadDeadline(time.Now().Add(s.cfg.ReadTimeout))
		}

		req, err := protocol.DecodeRequest(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				s.cfg.Logger.Warn("read timeout", "remote", remote)
				return
			}
			s.writeResponse(conn, protocol.Response{
				Kind:    protocol.ResponseError,
				Message: err.Error(),
			})
			return
		}

		resp := s.dispatch(req)
		if err := s.writeResponse(conn, resp); err != nil {
			s.cfg.Logger.Warn("write failed", "remote", remote, "error", err)
			return
		}
	}
}

func (s *Server) dispatch(req protocol.Request) protocol.Response {
	switch req.Command {
	case protocol.CommandOwner:
		return s.dispatchOwner(req.Key)
	case protocol.CommandClusterMembers:
		return s.dispatchClusterMembers()
	case protocol.CommandClusterJoin:
		return s.dispatchClusterJoin(req.Key, string(req.Value))
	case protocol.CommandClusterLeave:
		return s.dispatchClusterLeave(req.Key)
	case protocol.CommandSet, protocol.CommandGet, protocol.CommandDelete:
		if resp, ok := s.checkOwnership(req.Key); !ok {
			return resp
		}
	default:
		return protocol.Response{
			Kind:    protocol.ResponseError,
			Message: protocol.ErrUnknownCommand.Error(),
		}
	}

	switch req.Command {
	case protocol.CommandSet:
		if err := s.store.Set(req.Key, req.Value, req.TTL); err != nil {
			return protocol.Response{
				Kind:    protocol.ResponseError,
				Message: err.Error(),
			}
		}
		return protocol.Response{Kind: protocol.ResponseOK}
	case protocol.CommandGet:
		value, ok := s.store.Get(req.Key)
		if !ok {
			return protocol.Response{Kind: protocol.ResponseNotFound}
		}
		return protocol.Response{Kind: protocol.ResponseValue, Value: value}
	case protocol.CommandDelete:
		if s.store.Delete(req.Key) {
			return protocol.Response{Kind: protocol.ResponseOK}
		}
		return protocol.Response{Kind: protocol.ResponseNotFound}
	default:
		return protocol.Response{
			Kind:    protocol.ResponseError,
			Message: protocol.ErrUnknownCommand.Error(),
		}
	}
}

func (s *Server) checkOwnership(key string) (protocol.Response, bool) {
	if s.cfg.Cluster == nil {
		return protocol.Response{}, true
	}

	owner, ok := s.cfg.Cluster.Owner(key)
	if !ok {
		return protocol.Response{
			Kind:    protocol.ResponseError,
			Message: cluster.ErrEmptyRing.Error(),
		}, false
	}
	if !s.cfg.Cluster.IsOwner(key) {
		return protocol.Response{
			Kind:   protocol.ResponseMoved,
			NodeID: owner.ID,
			Addr:   owner.Addr,
		}, false
	}
	return protocol.Response{}, true
}

func (s *Server) dispatchOwner(key string) protocol.Response {
	if s.cfg.Cluster == nil {
		return protocol.Response{
			Kind:    protocol.ResponseError,
			Message: "cluster not enabled",
		}
	}

	owner, ok := s.cfg.Cluster.Owner(key)
	if !ok {
		return protocol.Response{
			Kind:    protocol.ResponseError,
			Message: cluster.ErrEmptyRing.Error(),
		}
	}

	return protocol.Response{
		Kind:   protocol.ResponseOwner,
		NodeID: owner.ID,
		Addr:   owner.Addr,
	}
}

func (s *Server) dispatchClusterMembers() protocol.Response {
	if s.cfg.Cluster == nil {
		return protocol.Response{
			Kind:    protocol.ResponseError,
			Message: "cluster not enabled",
		}
	}

	members := s.cfg.Cluster.Members()
	respMembers := make([]protocol.Member, 0, len(members))
	for _, member := range members {
		respMembers = append(respMembers, protocol.Member{
			ID:   member.ID,
			Addr: member.Addr,
		})
	}

	return protocol.Response{
		Kind:    protocol.ResponseMembers,
		Members: respMembers,
	}
}

func (s *Server) dispatchClusterJoin(nodeID, addr string) protocol.Response {
	if s.cfg.Cluster == nil {
		return protocol.Response{
			Kind:    protocol.ResponseError,
			Message: "cluster not enabled",
		}
	}

	if err := s.cfg.Cluster.Join(cluster.Node{ID: nodeID, Addr: addr}); err != nil {
		return protocol.Response{
			Kind:    protocol.ResponseError,
			Message: err.Error(),
		}
	}

	s.cfg.Logger.Info("cluster node joined", "node_id", nodeID, "addr", addr)
	return protocol.Response{Kind: protocol.ResponseOK}
}

func (s *Server) dispatchClusterLeave(nodeID string) protocol.Response {
	if s.cfg.Cluster == nil {
		return protocol.Response{
			Kind:    protocol.ResponseError,
			Message: "cluster not enabled",
		}
	}

	if err := s.cfg.Cluster.Leave(nodeID); err != nil {
		return protocol.Response{
			Kind:    protocol.ResponseError,
			Message: err.Error(),
		}
	}

	s.cfg.Logger.Info("cluster node left", "node_id", nodeID)
	return protocol.Response{Kind: protocol.ResponseOK}
}

func (s *Server) writeResponse(conn net.Conn, resp protocol.Response) error {
	if s.cfg.WriteTimeout > 0 {
		_ = conn.SetWriteDeadline(time.Now().Add(s.cfg.WriteTimeout))
	}

	payload, err := protocol.EncodeResponse(resp)
	if err != nil {
		return err
	}

	_, err = conn.Write(payload)
	return err
}

// Addr returns the bound address after ListenAndServe starts.
func (s *Server) Addr() string {
	if s.listener == nil {
		return s.cfg.Addr
	}
	return s.listener.Addr().String()
}
