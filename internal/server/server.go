package server

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/distributed-cache/distributed-cache/internal/cache"
	"github.com/distributed-cache/distributed-cache/internal/cluster"
	"github.com/distributed-cache/distributed-cache/internal/metrics"
	"github.com/distributed-cache/distributed-cache/internal/protocol"
	"github.com/distributed-cache/distributed-cache/internal/replication"
	"github.com/distributed-cache/distributed-cache/internal/raft"
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
	Replication     *replication.Manager
	Raft            *raft.Node
	Metrics         *metrics.Registry
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
	s.trace(remote, "connection_accepted", "")
	s.cfg.Logger.Info("client connected", "remote", remote)
	defer s.cfg.Logger.Info("client disconnected", "remote", remote)

	reader := bufio.NewReader(conn)

	for {
		if deadline, ok := ctx.Deadline(); ok {
			_ = conn.SetReadDeadline(deadline)
		} else if s.cfg.ReadTimeout > 0 {
			_ = conn.SetReadDeadline(time.Now().Add(s.cfg.ReadTimeout))
		}

		s.trace(remote, "request_decode_start", "")
		req, err := protocol.DecodeRequest(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				s.cfg.Logger.Warn("read timeout", "remote", remote)
				return
			}
			s.trace(remote, "response_encoded", "decode_error")
			s.writeResponse(conn, protocol.Response{
				Kind:    protocol.ResponseError,
				Message: err.Error(),
			})
			s.trace(remote, "response_flushed", "decode_error")
			return
		}
		s.trace(remote, "request_decoded", req.Command)

		start := time.Now()
		s.trace(remote, "dispatch_start", req.Command)
		reqCtx := ctx
		if s.cfg.ReadTimeout > 0 {
			var cancel context.CancelFunc
			reqCtx, cancel = context.WithTimeout(ctx, s.cfg.ReadTimeout)
			defer cancel()
		}
		resp := s.dispatch(reqCtx, req)
		s.trace(remote, "dispatch_complete", req.Command)
		if s.cfg.Metrics != nil {
			s.cfg.Metrics.Observe(req.Command, time.Since(start), resp.Kind == protocol.ResponseError)
		}
		s.trace(remote, "response_encoded", req.Command)
		if err := s.writeResponse(conn, resp); err != nil {
			s.cfg.Logger.Warn("write failed", "remote", remote, "error", err)
			return
		}
		s.trace(remote, "response_flushed", req.Command)
	}
}

func (s *Server) dispatch(ctx context.Context, req protocol.Request) protocol.Response {
	switch req.Command {
	case protocol.CommandPing:
		s.trace("", "handler_entered", protocol.CommandPing)
		return protocol.Response{Kind: protocol.ResponseOK}
	case protocol.CommandMetrics:
		s.trace("", "handler_entered", protocol.CommandMetrics)
		return s.dispatchMetrics()
	case protocol.CommandRaftRequestVote:
		s.trace("", "handler_entered", protocol.CommandRaftRequestVote)
		return s.dispatchRaftRequestVote(req.Value)
	case protocol.CommandRaftAppendEntries:
		s.trace("", "handler_entered", protocol.CommandRaftAppendEntries)
		return s.dispatchRaftAppendEntries(req.Value)
	case protocol.CommandRaftStatus:
		s.trace("", "handler_entered", protocol.CommandRaftStatus)
		return s.dispatchRaftStatus()
	case protocol.CommandReplSet:
		s.trace("", "handler_entered", protocol.CommandReplSet)
		return s.dispatchReplicaSet(req)
	case protocol.CommandReplDelete:
		s.trace("", "handler_entered", protocol.CommandReplDelete)
		return s.dispatchReplicaDelete(req.Key)
	case protocol.CommandReplGet:
		s.trace("", "handler_entered", protocol.CommandReplGet)
		return s.dispatchReplicaGet(req.Key)
	case protocol.CommandOwner:
		s.trace("", "handler_entered", protocol.CommandOwner)
		return s.dispatchOwner(req.Key)
	case protocol.CommandClusterMembers:
		s.trace("", "handler_entered", protocol.CommandClusterMembers)
		return s.dispatchClusterMembers()
	case protocol.CommandClusterJoin:
		s.trace("", "handler_entered", protocol.CommandClusterJoin)
		return s.dispatchClusterJoin(req.Key, string(req.Value))
	case protocol.CommandClusterLeave:
		s.trace("", "handler_entered", protocol.CommandClusterLeave)
		return s.dispatchClusterLeave(req.Key)
	case protocol.CommandSet:
		s.trace("", "handler_entered", protocol.CommandSet)
		if resp, ok := s.checkWriteRouting(req.Key); !ok {
			return resp
		}
		return s.dispatchSet(ctx, req)
	case protocol.CommandDelete:
		s.trace("", "handler_entered", protocol.CommandDelete)
		if resp, ok := s.checkWriteRouting(req.Key); !ok {
			return resp
		}
		return s.dispatchDelete(ctx, req.Key)
	case protocol.CommandGet:
		s.trace("", "handler_entered", protocol.CommandGet)
		if resp, ok := s.checkReadRouting(req.Key); !ok {
			return resp
		}
		return s.dispatchGet(ctx, req.Key)
	default:
		return protocol.Response{
			Kind:    protocol.ResponseError,
			Message: protocol.ErrUnknownCommand.Error(),
		}
	}
}

func (s *Server) dispatchSet(ctx context.Context, req protocol.Request) protocol.Response {
	if s.cfg.Replication != nil && s.cfg.Replication.Enabled() {
		if err := s.cfg.Replication.WriteSet(ctx, req.Key, req.Value, req.TTL); err != nil {
			return protocol.Response{Kind: protocol.ResponseError, Message: err.Error()}
		}
		return protocol.Response{Kind: protocol.ResponseOK}
	}

	if err := s.store.Set(req.Key, req.Value, req.TTL); err != nil {
		return protocol.Response{Kind: protocol.ResponseError, Message: err.Error()}
	}
	return protocol.Response{Kind: protocol.ResponseOK}
}

func (s *Server) dispatchDelete(ctx context.Context, key string) protocol.Response {
	if s.cfg.Replication != nil && s.cfg.Replication.Enabled() {
		deleted, err := s.cfg.Replication.WriteDelete(ctx, key)
		if err != nil {
			return protocol.Response{Kind: protocol.ResponseError, Message: err.Error()}
		}
		if !deleted {
			return protocol.Response{Kind: protocol.ResponseNotFound}
		}
		return protocol.Response{Kind: protocol.ResponseOK}
	}

	if s.store.Delete(key) {
		return protocol.Response{Kind: protocol.ResponseOK}
	}
	return protocol.Response{Kind: protocol.ResponseNotFound}
}

func (s *Server) dispatchGet(ctx context.Context, key string) protocol.Response {
	if s.cfg.Replication != nil && s.cfg.Replication.Enabled() {
		value, ok, err := s.cfg.Replication.Read(ctx, key)
		if errors.Is(err, replication.ErrNotInReplicaSet) {
			return s.redirectToPrimary(key)
		}
		if errors.Is(err, replication.ErrReadQuorumNotMet) {
			return protocol.Response{Kind: protocol.ResponseError, Message: err.Error()}
		}
		if err != nil {
			return protocol.Response{Kind: protocol.ResponseError, Message: err.Error()}
		}
		if !ok {
			return protocol.Response{Kind: protocol.ResponseNotFound}
		}
		return protocol.Response{Kind: protocol.ResponseValue, Value: value}
	}

	value, ok := s.store.Get(key)
	if !ok {
		return protocol.Response{Kind: protocol.ResponseNotFound}
	}
	return protocol.Response{Kind: protocol.ResponseValue, Value: value}
}

func (s *Server) dispatchReplicaSet(req protocol.Request) protocol.Response {
	if s.cfg.Replication == nil {
		return protocol.Response{Kind: protocol.ResponseError, Message: "replication not enabled"}
	}
	if err := s.cfg.Replication.ApplyReplicaSet(req.Key, req.Value, req.TTL); err != nil {
		return protocol.Response{Kind: protocol.ResponseError, Message: err.Error()}
	}
	return protocol.Response{Kind: protocol.ResponseOK}
}

func (s *Server) dispatchReplicaGet(key string) protocol.Response {
	if s.cfg.Replication == nil || !s.cfg.Replication.IsInReplicaSet(key) {
		return protocol.Response{Kind: protocol.ResponseError, Message: replication.ErrNotInReplicaSet.Error()}
	}
	value, ok := s.store.Get(key)
	if !ok {
		return protocol.Response{Kind: protocol.ResponseNotFound}
	}
	return protocol.Response{Kind: protocol.ResponseValue, Value: value}
}

func (s *Server) dispatchReplicaDelete(key string) protocol.Response {
	if s.cfg.Replication == nil {
		return protocol.Response{Kind: protocol.ResponseError, Message: "replication not enabled"}
	}
	deleted, err := s.cfg.Replication.ApplyReplicaDelete(key)
	if err != nil {
		return protocol.Response{Kind: protocol.ResponseError, Message: err.Error()}
	}
	if !deleted {
		return protocol.Response{Kind: protocol.ResponseNotFound}
	}
	return protocol.Response{Kind: protocol.ResponseOK}
}

func (s *Server) checkWriteRouting(key string) (protocol.Response, bool) {
	if s.cfg.Replication != nil && s.cfg.Replication.Enabled() {
		if s.cfg.Replication.IsPrimary(key) {
			return protocol.Response{}, true
		}
		return s.redirectToPrimary(key), false
	}
	return s.checkOwnership(key)
}

func (s *Server) checkReadRouting(key string) (protocol.Response, bool) {
	if s.cfg.Replication != nil && s.cfg.Replication.Enabled() {
		if s.cfg.Replication.IsInReplicaSet(key) {
			return protocol.Response{}, true
		}
		return s.redirectToPrimary(key), false
	}
	return s.checkOwnership(key)
}

func (s *Server) redirectToPrimary(key string) protocol.Response {
	if s.cfg.Cluster == nil {
		return protocol.Response{Kind: protocol.ResponseError, Message: "cluster not enabled"}
	}
	primary, ok := s.cfg.Cluster.Owner(key)
	if !ok {
		return protocol.Response{Kind: protocol.ResponseError, Message: cluster.ErrEmptyRing.Error()}
	}
	return protocol.Response{
		Kind:   protocol.ResponseMoved,
		NodeID: primary.ID,
		Addr:   primary.Addr,
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

	if s.cfg.Raft != nil {
		if !s.cfg.Raft.IsLeader() {
			return s.raftNotLeaderResponse()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.cfg.Raft.Propose(ctx, raft.Command{
			Type:   raft.CommandAddMember,
			NodeID: nodeID,
			Addr:   addr,
		}); err != nil {
			return protocol.Response{Kind: protocol.ResponseError, Message: err.Error()}
		}
		s.cfg.Logger.Info("cluster node joined via raft", "node_id", nodeID, "addr", addr)
		return protocol.Response{Kind: protocol.ResponseOK}
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

	if s.cfg.Raft != nil {
		if !s.cfg.Raft.IsLeader() {
			return s.raftNotLeaderResponse()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.cfg.Raft.Propose(ctx, raft.Command{
			Type:   raft.CommandRemoveMember,
			NodeID: nodeID,
		}); err != nil {
			return protocol.Response{Kind: protocol.ResponseError, Message: err.Error()}
		}
		s.cfg.Logger.Info("cluster node left via raft", "node_id", nodeID)
		return protocol.Response{Kind: protocol.ResponseOK}
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

func (s *Server) dispatchRaftRequestVote(payload []byte) protocol.Response {
	if s.cfg.Raft == nil {
		return protocol.Response{Kind: protocol.ResponseError, Message: "raft not enabled"}
	}
	var req raft.RequestVoteRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return protocol.Response{Kind: protocol.ResponseError, Message: err.Error()}
	}
	resp := s.cfg.Raft.HandleRequestVote(req)
	out, err := json.Marshal(resp)
	if err != nil {
		return protocol.Response{Kind: protocol.ResponseError, Message: err.Error()}
	}
	return protocol.Response{Kind: protocol.ResponseValue, Value: out}
}

func (s *Server) dispatchRaftAppendEntries(payload []byte) protocol.Response {
	if s.cfg.Raft == nil {
		return protocol.Response{Kind: protocol.ResponseError, Message: "raft not enabled"}
	}
	var req raft.AppendEntriesRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return protocol.Response{Kind: protocol.ResponseError, Message: err.Error()}
	}
	s.trace("", "raft_handle_append_entries_enter", fmt.Sprintf("term=%d entries=%d", req.Term, len(req.Entries)))
	resp := s.cfg.Raft.HandleAppendEntries(req)
	s.trace("", "raft_handle_append_entries_exit", fmt.Sprintf("success=%v", resp.Success))
	out, err := json.Marshal(resp)
	if err != nil {
		return protocol.Response{Kind: protocol.ResponseError, Message: err.Error()}
	}
	return protocol.Response{Kind: protocol.ResponseValue, Value: out}
}

func (s *Server) dispatchRaftStatus() protocol.Response {
	if s.cfg.Raft == nil {
		return protocol.Response{Kind: protocol.ResponseError, Message: "raft not enabled"}
	}
	status := s.cfg.Raft.Status()
	out, err := json.Marshal(status)
	if err != nil {
		return protocol.Response{Kind: protocol.ResponseError, Message: err.Error()}
	}
	return protocol.Response{Kind: protocol.ResponseValue, Value: out}
}

func (s *Server) dispatchMetrics() protocol.Response {
	if s.cfg.Metrics == nil {
		return protocol.Response{Kind: protocol.ResponseError, Message: "metrics not enabled"}
	}
	out, err := json.Marshal(s.cfg.Metrics.Snapshot())
	if err != nil {
		return protocol.Response{Kind: protocol.ResponseError, Message: err.Error()}
	}
	return protocol.Response{Kind: protocol.ResponseValue, Value: out}
}

func (s *Server) raftNotLeaderResponse() protocol.Response {
	leaderID := s.cfg.Raft.LeaderID()
	addr := s.cfg.Raft.LeaderAddr()
	if leaderID == "" {
		return protocol.Response{Kind: protocol.ResponseError, Message: raft.ErrNotLeader.Error()}
	}
	return protocol.Response{
		Kind:   protocol.ResponseNotLeader,
		NodeID: leaderID,
		Addr:   addr,
	}
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
