package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/distributed-cache/distributed-cache/internal/cache"
	"github.com/distributed-cache/distributed-cache/internal/cluster"
	"github.com/distributed-cache/distributed-cache/internal/config"
	"github.com/distributed-cache/distributed-cache/internal/discovery"
	"github.com/distributed-cache/distributed-cache/internal/metrics"
	"github.com/distributed-cache/distributed-cache/internal/replication"
	"github.com/distributed-cache/distributed-cache/internal/raft"
	"github.com/distributed-cache/distributed-cache/internal/server"
)

func main() {
	addr := flag.String("addr", ":6379", "TCP listen address")
	advertiseAddr := flag.String("advertise-addr", "", "cluster address advertised to peers (defaults to -addr)")
	nodeID := flag.String("node-id", "", "unique cluster node id (enables cluster mode when set)")
	peers := flag.String("peers", "", "comma-separated peer list: id=host:port,id=host:port")
	vnodes := flag.Int("vnodes", 128, "virtual nodes per physical node on the hash ring")
	raftEnabled := flag.Bool("raft", false, "enable Raft consensus for membership changes")
	replicationFactor := flag.Int("replication-factor", 1, "number of replicas per key (1 disables replication)")
	writeQuorum := flag.Int("write-quorum", 0, "required write acknowledgements (default: RF/2+1)")
	readQuorum := flag.Int("read-quorum", 0, "required read responses (default: RF/2+1)")
	writeConsistency := flag.String("write-consistency", "quorum", "write consistency: one, quorum, all")
	readConsistency := flag.String("read-consistency", "quorum", "read consistency: primary, quorum, any")
	healthInterval := flag.Duration("health-interval", 5*time.Second, "peer health check interval")
	maxConns := flag.Int("max-conns", 1024, "maximum concurrent client connections")
	maxBytes := flag.Int64("max-bytes", 0, "maximum cache memory in bytes (0 = unlimited)")
	cleanupInterval := flag.Duration("cleanup-interval", time.Second, "background TTL cleanup interval")
	readTimeout := flag.Duration("read-timeout", 30*time.Second, "per-read timeout")
	writeTimeout := flag.Duration("write-timeout", 30*time.Second, "per-write timeout")
	debugAddr := flag.String("debug-addr", "", "HTTP address for pprof and JSON metrics (e.g. 127.0.0.1:6060)")
	flag.Parse()

	*nodeID = config.StringFromEnv(*nodeID, "CACHE_NODE_ID")
	*advertiseAddr = config.StringFromEnv(*advertiseAddr, "CACHE_ADVERTISE_ADDR")
	*peers = config.StringFromEnv(*peers, "CACHE_PEERS")
	*debugAddr = config.StringFromEnv(*debugAddr, "CACHE_DEBUG_ADDR")
	*raftEnabled = config.BoolFromEnv(*raftEnabled, "CACHE_RAFT")
	*replicationFactor = config.IntFromEnv(*replicationFactor, "CACHE_REPLICATION_FACTOR")
	*maxBytes = config.Int64FromEnv(*maxBytes, "CACHE_MAX_BYTES")

	logLevel := config.StringFromEnv("", "CACHE_LOG_LEVEL")
	loggerLevel := slog.LevelInfo
	if strings.EqualFold(logLevel, "debug") {
		loggerLevel = slog.LevelDebug
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: loggerLevel,
	}))

	storeCfg := cache.DefaultConfig()
	storeCfg.MaxBytes = *maxBytes
	storeCfg.CleanupInterval = *cleanupInterval

	store := cache.NewStore(storeCfg)

	var clusterView *cluster.Cluster
	var peerNodes []cluster.Node
	if *nodeID != "" {
		publicAddr := *advertiseAddr
		if publicAddr == "" {
			publicAddr = *addr
		}

		var err error
		peerNodes, err = cluster.ParsePeers(*peers)
		if err != nil {
			logger.Error("invalid peers", "error", err)
			os.Exit(1)
		}

		clusterView, err = cluster.NewCluster(
			cluster.Node{ID: *nodeID, Addr: publicAddr},
			peerNodes,
			cluster.RingConfig{VirtualNodes: *vnodes},
		)
		if err != nil {
			logger.Error("failed to create cluster", "error", err)
			os.Exit(1)
		}
	}

	var replicationManager *replication.Manager
	if *replicationFactor > 1 {
		if clusterView == nil {
			logger.Error("replication requires cluster mode (-node-id)")
			os.Exit(1)
		}

		replCfg := replication.Config{
			ReplicationFactor: *replicationFactor,
			WriteQuorum:       *writeQuorum,
			ReadQuorum:        *readQuorum,
			WriteConsistency:  replication.WriteConsistency(*writeConsistency),
			ReadConsistency:   replication.ReadConsistency(*readConsistency),
		}

		var err error
		replicationManager, err = replication.NewManager(replCfg, clusterView, store, logger)
		if err != nil {
			logger.Error("failed to create replication manager", "error", err)
			os.Exit(1)
		}
	}

	var raftNode *raft.Node
	if *raftEnabled {
		if clusterView == nil {
			logger.Error("raft requires cluster mode (-node-id)")
			os.Exit(1)
		}

		publicAddr := *advertiseAddr
		if publicAddr == "" {
			publicAddr = *addr
		}

		peerMap := make(map[string]string, len(peerNodes))
		for _, peer := range peerNodes {
			peerMap[peer.ID] = peer.Addr
		}

		transport := &raft.TCPTransport{Timeout: 500 * time.Millisecond}
		apply := func(entry raft.Entry) {
			switch entry.Command.Type {
			case raft.CommandAddMember:
				if err := clusterView.Join(cluster.Node{ID: entry.Command.NodeID, Addr: entry.Command.Addr}); err != nil {
					logger.Error("apply add member failed", "error", err)
					return
				}
				if raftNode != nil {
					raftNode.AddPeer(entry.Command.NodeID, entry.Command.Addr)
				}
				logger.Info("raft applied add member", "node_id", entry.Command.NodeID, "addr", entry.Command.Addr)
			case raft.CommandRemoveMember:
				if err := clusterView.Leave(entry.Command.NodeID); err != nil {
					logger.Error("apply remove member failed", "error", err)
					return
				}
				if raftNode != nil {
					raftNode.RemovePeer(entry.Command.NodeID)
				}
				logger.Info("raft applied remove member", "node_id", entry.Command.NodeID)
			}
		}

		raftNode = raft.NewNode(*nodeID, publicAddr, peerMap, raft.DefaultConfig(), transport, apply, logger)
	}

	var metricsRegistry *metrics.Registry
	var discoveryProvider discovery.Provider
	if *debugAddr != "" {
		metricsRegistry = metrics.NewRegistry()
		if clusterView != nil {
			discoveryProvider = discovery.NewClusterProvider(clusterView)
		}
	}

	srv, err := server.New(server.Config{
		Addr:           *addr,
		MaxConnections: *maxConns,
		ReadTimeout:    *readTimeout,
		WriteTimeout:   *writeTimeout,
		Logger:         logger,
		Cluster:        clusterView,
		Replication:    replicationManager,
		Raft:           raftNode,
		Metrics:        metricsRegistry,
	}, store)
	if err != nil {
		logger.Error("failed to create server", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go store.RunCleanup(ctx)
	if replicationManager != nil {
		go replicationManager.RunHealthChecks(ctx, *healthInterval)
	}
	if raftNode != nil {
		go raftNode.Start(ctx)
	}
	if *debugAddr != "" {
		go func() {
			bound, err := metrics.StartDebug(ctx, *debugAddr, metrics.DebugOptions{
				Registry:  metricsRegistry,
				Discovery: discoveryProvider,
			}, logger)
			if err != nil {
				logger.Error("debug server failed to start", "error", err)
				return
			}
			if bound != "" && bound != *debugAddr {
				logger.Info("debug server bound", "requested", *debugAddr, "actual", bound)
			}
		}()
	}

	logger.Info("starting cache node",
		"addr", *addr,
		"node_id", *nodeID,
		"max_bytes", *maxBytes,
		"cleanup_interval", storeCfg.CleanupInterval.String(),
		"vnodes", *vnodes,
		"replication_factor", *replicationFactor,
		"write_consistency", *writeConsistency,
		"read_consistency", *readConsistency,
		"raft", *raftEnabled,
		"debug_addr", *debugAddr,
	)
	if err := srv.ListenAndServe(ctx); err != nil {
		logger.Error("server stopped with error", "error", err)
		os.Exit(1)
	}

	logger.Info("server stopped")
}
