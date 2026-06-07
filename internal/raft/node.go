package raft

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"sync"
	"time"
)

var (
	ErrNotLeader     = errors.New("not the raft leader")
	ErrProposalDropped = errors.New("proposal dropped")
)

type proposal struct {
	command Command
	notify  chan error
}

// Node implements the Raft consensus algorithm.
type Node struct {
	mu sync.Mutex

	id    string
	addr  string
	peers map[string]string

	role        Role
	currentTerm uint64
	votedFor    string
	log         []Entry

	commitIndex uint64
	lastApplied uint64

	leaderID string

	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	electionDeadline  time.Time
	heartbeatDeadline time.Time

	cfg       Config
	transport Transport
	apply     func(Entry)
	logger    *slog.Logger

	proposals map[uint64]*proposal
	rng       *rand.Rand
}

// NewNode creates a Raft node with a sentinel entry at index 0.
func NewNode(id, addr string, peers map[string]string, cfg Config, transport Transport, apply func(Entry), logger *slog.Logger) *Node {
	cfg = cfg.normalize()
	if logger == nil {
		logger = slog.Default()
	}
	if apply == nil {
		apply = func(Entry) {}
	}
	peerCopy := make(map[string]string, len(peers))
	for k, v := range peers {
		peerCopy[k] = v
	}

	node := &Node{
		id:        id,
		addr:      addr,
		peers:     peerCopy,
		role:      RoleFollower,
		log:       []Entry{{Term: 0, Index: 0, Command: Command{Type: CommandNoop}}},
		cfg:       cfg,
		transport: transport,
		apply:     apply,
		logger:    logger,
		proposals: make(map[uint64]*proposal),
		rng:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	node.resetElectionDeadlineLocked(time.Now())
	return node
}

// Start runs the Raft event loop until ctx is canceled.
func (n *Node) Start(ctx context.Context) {
	ticker := time.NewTicker(n.cfg.HeartbeatInterval / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n.tick()
		}
	}
}

// Tick advances timers and triggers elections or heartbeats.
func (n *Node) Tick() {
	n.tick()
}

func (n *Node) tick() {
	n.mu.Lock()
	now := time.Now()
	if n.role == RoleLeader {
		if now.After(n.heartbeatDeadline) {
			n.sendHeartbeatsLocked()
			n.resetHeartbeatDeadlineLocked(now)
		}
		n.mu.Unlock()
		return
	}
	if now.After(n.electionDeadline) {
		n.startElectionLocked()
		return
	}
	n.mu.Unlock()
}

// Propose appends a command and waits until it is committed and applied.
func (n *Node) Propose(ctx context.Context, cmd Command) error {
	n.mu.Lock()
	if n.role != RoleLeader {
		n.mu.Unlock()
		return ErrNotLeader
	}

	index := uint64(len(n.log))
	entry := Entry{
		Term:    n.currentTerm,
		Index:   index,
		Command: cmd,
	}
	n.log = append(n.log, entry)

	waiter := make(chan error, 1)
	n.proposals[index] = &proposal{command: cmd, notify: waiter}
	if len(n.peerAddrsLocked()) == 0 {
		n.commitIndex = index
		n.applyCommittedLocked()
	} else {
		n.replicateLocked()
	}
	n.mu.Unlock()

	select {
	case <-ctx.Done():
		n.mu.Lock()
		delete(n.proposals, index)
		n.mu.Unlock()
		return ctx.Err()
	case err := <-waiter:
		return err
	}
}

// HandleRequestVote processes a RequestVote RPC.
func (n *Node) HandleRequestVote(req RequestVoteRequest) RequestVoteResponse {
	n.mu.Lock()

	if req.Term < n.currentTerm {
		resp := RequestVoteResponse{Term: n.currentTerm, VoteGranted: false}
		n.mu.Unlock()
		return resp
	}
	if req.Term > n.currentTerm {
		n.becomeFollowerLocked(req.Term, "")
	}

	lastIndex, lastTerm := n.lastLogIndexTermLocked()
	upToDate := req.LastLogTerm > lastTerm || (req.LastLogTerm == lastTerm && req.LastLogIndex >= lastIndex)
	canVote := (n.votedFor == "" || n.votedFor == req.CandidateID) && upToDate
	if canVote {
		n.votedFor = req.CandidateID
		n.resetElectionDeadlineLocked(time.Now())
	}
	resp := RequestVoteResponse{Term: n.currentTerm, VoteGranted: canVote}
	candidateID := req.CandidateID
	term := req.Term
	n.mu.Unlock()

	if canVote {
		n.logger.Info("granted vote", "candidate", candidateID, "term", term)
	}
	return resp
}

// HandleAppendEntries processes an AppendEntries RPC.
func (n *Node) HandleAppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.currentTerm {
		return AppendEntriesResponse{Term: n.currentTerm, Success: false}
	}
	if req.Term > n.currentTerm || n.role != RoleFollower {
		n.becomeFollowerLocked(req.Term, req.LeaderID)
	} else {
		n.leaderID = req.LeaderID
	}

	n.resetElectionDeadlineLocked(time.Now())

	if req.PrevLogIndex >= uint64(len(n.log)) {
		return AppendEntriesResponse{Term: n.currentTerm, Success: false}
	}
	if n.log[req.PrevLogIndex].Term != req.PrevLogTerm {
		return AppendEntriesResponse{Term: n.currentTerm, Success: false}
	}

	insertAt := req.PrevLogIndex + 1
	if insertAt < uint64(len(n.log)) {
		n.log = n.log[:insertAt]
	}
	for _, entry := range req.Entries {
		entry.Index = uint64(len(n.log))
		n.log = append(n.log, entry)
	}

	if req.LeaderCommit > n.commitIndex {
		lastNewIndex := uint64(len(n.log)) - 1
		n.commitIndex = min64(req.LeaderCommit, lastNewIndex)
		n.applyCommittedLocked()
	}

	return AppendEntriesResponse{Term: n.currentTerm, Success: true}
}

// IsLeader reports whether this node is the current leader.
func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role == RoleLeader
}

// LeaderID returns the known leader node id.
func (n *Node) LeaderID() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

// LeaderAddr returns the address of the current leader if known.
func (n *Node) LeaderAddr() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.leaderID == "" {
		return ""
	}
	if n.leaderID == n.id {
		return n.addr
	}
	return n.peers[n.leaderID]
}

// Status returns a snapshot of Raft state.
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{
		ID:          n.id,
		Role:        n.role,
		Term:        n.currentTerm,
		LeaderID:    n.leaderID,
		CommitIndex: n.commitIndex,
		LastApplied: n.lastApplied,
		LogLength:   len(n.log),
	}
}

// SetPeerAddr updates a peer address after cluster membership changes.
func (n *Node) SetPeerAddr(id, addr string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.peers[id] = addr
}

// SetAddr updates the public address for this node.
func (n *Node) SetAddr(addr string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.addr = addr
}

// AddPeer registers a new voting peer.
func (n *Node) AddPeer(id, addr string) {
	n.mu.Lock()
	n.peers[id] = addr
	replicate := n.role == RoleLeader
	if replicate {
		n.nextIndex[id] = uint64(len(n.log))
		n.matchIndex[id] = 0
	}
	n.mu.Unlock()
	if replicate {
		n.mu.Lock()
		n.replicateLocked()
		n.mu.Unlock()
	}
}

// RemovePeer removes a voting peer.
func (n *Node) RemovePeer(id string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.peers, id)
	delete(n.nextIndex, id)
	delete(n.matchIndex, id)
}

// PeerAddrs returns a copy of peer id to address mappings excluding self.
func (n *Node) PeerAddrs() map[string]string {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make(map[string]string, len(n.peers))
	for id, addr := range n.peers {
		if id != n.id {
			out[id] = addr
		}
	}
	return out
}

func (n *Node) startElectionLocked() {
	n.role = RoleCandidate
	n.currentTerm++
	term := n.currentTerm
	n.votedFor = n.id
	n.leaderID = ""
	n.resetElectionDeadlineLocked(time.Now())

	lastIndex, lastTerm := n.lastLogIndexTermLocked()
	majority := len(n.peers)/2 + 1
	peers := n.peerAddrsLocked()

	n.mu.Unlock()
	n.logger.Info("starting election", "term", term, "node_id", n.id)

	if len(peers) == 0 {
		n.mu.Lock()
		if n.role == RoleCandidate && n.currentTerm == term {
			n.becomeLeaderLocked()
			n.logger.Info("became leader", "term", n.currentTerm, "node_id", n.id)
		}
		n.mu.Unlock()
		return
	}

	votes := 1
	var voteMu sync.Mutex
	var becomeLeaderOnce sync.Once

	for peerID, addr := range peers {
		go func(peerID, addr string) {
			ctx, cancel := context.WithTimeout(context.Background(), n.cfg.RPCTimeout)
			defer cancel()
			resp, err := n.transport.RequestVote(ctx, addr, RequestVoteRequest{
				Term:         term,
				CandidateID:  n.id,
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
			})
			if err != nil {
				n.logger.Warn("request vote failed", "peer", peerID, "error", err)
			}

			n.mu.Lock()
			if resp.Term > n.currentTerm {
				n.becomeFollowerLocked(resp.Term, "")
				n.mu.Unlock()
				return
			}
			n.mu.Unlock()

			if err != nil || !resp.VoteGranted {
				return
			}

			voteMu.Lock()
			votes++
			currentVotes := votes
			voteMu.Unlock()

			if currentVotes >= majority {
				becomeLeaderOnce.Do(func() {
					n.mu.Lock()
					if n.role == RoleCandidate && n.currentTerm == term {
						n.becomeLeaderLocked()
						n.logger.Info("became leader", "term", n.currentTerm, "node_id", n.id)
					}
					n.mu.Unlock()
				})
			}
		}(peerID, addr)
	}
}

func (n *Node) becomeLeaderLocked() {
	n.role = RoleLeader
	n.leaderID = n.id
	n.nextIndex = make(map[string]uint64, len(n.peers))
	n.matchIndex = make(map[string]uint64, len(n.peers))
	next := uint64(len(n.log))
	for id := range n.peers {
		if id == n.id {
			continue
		}
		n.nextIndex[id] = next
		n.matchIndex[id] = 0
	}
	n.resetHeartbeatDeadlineLocked(time.Now())
	n.replicateLocked()
}

func (n *Node) becomeFollowerLocked(term uint64, leaderID string) {
	n.role = RoleFollower
	n.currentTerm = term
	n.votedFor = ""
	n.leaderID = leaderID
	n.resetElectionDeadlineLocked(time.Now())
}

func (n *Node) sendHeartbeatsLocked() {
	n.replicateLocked()
}

func (n *Node) replicateLocked() {
	if n.role != RoleLeader {
		return
	}
	for peerID, addr := range n.peerAddrsLocked() {
		next, ok := n.nextIndex[peerID]
		if !ok || next == 0 {
			next = uint64(len(n.log))
			n.nextIndex[peerID] = next
		}
		if next > uint64(len(n.log)) {
			next = uint64(len(n.log))
			n.nextIndex[peerID] = next
		}
		prevIndex := next - 1
		prevTerm := n.log[prevIndex].Term
		entries := append([]Entry(nil), n.log[next:]...)

		req := AppendEntriesRequest{
			Term:         n.currentTerm,
			LeaderID:     n.id,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: n.commitIndex,
		}

		go func(peerID, addr string, req AppendEntriesRequest, prevIndex uint64) {
			ctx, cancel := context.WithTimeout(context.Background(), n.cfg.RPCTimeout)
			defer cancel()
			resp, err := n.transport.AppendEntries(ctx, addr, req)

			n.mu.Lock()
			if err != nil {
				n.logger.Warn("append entries failed", "peer", peerID, "error", err)
				n.mu.Unlock()
				return
			}
			if resp.Term > n.currentTerm {
				n.becomeFollowerLocked(resp.Term, "")
				n.mu.Unlock()
				return
			}
			if n.role != RoleLeader || req.Term != n.currentTerm {
				n.mu.Unlock()
				return
			}
			if !resp.Success {
				if n.nextIndex[peerID] > 1 {
					n.nextIndex[peerID]--
				}
				n.replicateLocked()
				n.mu.Unlock()
				return
			}

			if len(req.Entries) > 0 {
				lastIndex := req.Entries[len(req.Entries)-1].Index
				n.matchIndex[peerID] = lastIndex
				n.nextIndex[peerID] = lastIndex + 1
			}

			for index := n.commitIndex + 1; index < uint64(len(n.log)); index++ {
				if n.log[index].Term != n.currentTerm {
					continue
				}
				count := 1
				for id := range n.peers {
					if id == n.id {
						continue
					}
					if n.matchIndex[id] >= index {
						count++
					}
				}
				if count >= len(n.peers)/2+1 {
					n.commitIndex = index
				}
			}
			n.applyCommittedLocked()
			n.replicateLocked()
			n.mu.Unlock()
		}(peerID, addr, req, prevIndex)
	}
}

func (n *Node) applyCommittedLocked() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.log[n.lastApplied]
		prop := n.proposals[n.lastApplied]

		n.mu.Unlock()
		n.apply(entry)
		n.mu.Lock()

		if prop != nil {
			select {
			case prop.notify <- nil:
			default:
			}
			delete(n.proposals, n.lastApplied)
		}
	}
}

func (n *Node) lastLogIndexTermLocked() (uint64, uint64) {
	lastIndex := uint64(len(n.log)) - 1
	return lastIndex, n.log[lastIndex].Term
}

func (n *Node) resetElectionDeadlineLocked(now time.Time) {
	jitter := n.cfg.ElectionTimeoutMax - n.cfg.ElectionTimeoutMin
	if jitter <= 0 {
		jitter = time.Millisecond
	}
	n.electionDeadline = now.Add(n.cfg.ElectionTimeoutMin + time.Duration(n.rng.Int63n(int64(jitter))))
}

func (n *Node) resetHeartbeatDeadlineLocked(now time.Time) {
	n.heartbeatDeadline = now.Add(n.cfg.HeartbeatInterval)
}

func (n *Node) peerAddrsLocked() map[string]string {
	out := make(map[string]string, len(n.peers))
	for id, addr := range n.peers {
		if id != n.id {
			out[id] = addr
		}
	}
	return out
}

func min64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

// SetTransport replaces the transport, primarily for tests.
func (n *Node) SetTransport(transport Transport) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.transport = transport
}

// InjectAppendEntries allows tests to deliver AppendEntries without network I/O.
func (n *Node) InjectAppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	return n.HandleAppendEntries(req)
}

// InjectRequestVote allows tests to deliver RequestVote without network I/O.
func (n *Node) InjectRequestVote(req RequestVoteRequest) RequestVoteResponse {
	return n.HandleRequestVote(req)
}

// ForceElectionTimeout triggers an election attempt immediately.
func (n *Node) ForceElectionTimeout() {
	n.mu.Lock()
	n.electionDeadline = time.Now().Add(-time.Millisecond)
	n.mu.Unlock()
	n.tick()
}

// AppendLocalEntry appends to the log while leader without replication, for tests only.
func (n *Node) AppendLocalEntry(cmd Command) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != RoleLeader {
		return ErrNotLeader
	}
	index := uint64(len(n.log))
	n.log = append(n.log, Entry{Term: n.currentTerm, Index: index, Command: cmd})
	n.commitIndex = index
	n.applyCommittedLocked()
	return nil
}
