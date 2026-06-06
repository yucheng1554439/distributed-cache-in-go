package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/distributed-cache/distributed-cache/internal/cache"
	"github.com/distributed-cache/distributed-cache/internal/cluster"
	"github.com/distributed-cache/distributed-cache/internal/server"
)

func main() {
	addr := flag.String("addr", ":6379", "TCP listen address")
	advertiseAddr := flag.String("advertise-addr", "", "cluster address advertised to peers (defaults to -addr)")
	nodeID := flag.String("node-id", "", "unique cluster node id (enables cluster mode when set)")
	peers := flag.String("peers", "", "comma-separated peer list: id=host:port,id=host:port")
	vnodes := flag.Int("vnodes", 128, "virtual nodes per physical node on the hash ring")
	maxConns := flag.Int("max-conns", 1024, "maximum concurrent client connections")
	maxBytes := flag.Int64("max-bytes", 0, "maximum cache memory in bytes (0 = unlimited)")
	cleanupInterval := flag.Duration("cleanup-interval", time.Second, "background TTL cleanup interval")
	readTimeout := flag.Duration("read-timeout", 30*time.Second, "per-read timeout")
	writeTimeout := flag.Duration("write-timeout", 30*time.Second, "per-write timeout")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	storeCfg := cache.DefaultConfig()
	storeCfg.MaxBytes = *maxBytes
	storeCfg.CleanupInterval = *cleanupInterval

	store := cache.NewStore(storeCfg)

	var clusterView *cluster.Cluster
	if *nodeID != "" {
		publicAddr := *advertiseAddr
		if publicAddr == "" {
			publicAddr = *addr
		}

		peerNodes, err := cluster.ParsePeers(*peers)
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

	srv, err := server.New(server.Config{
		Addr:           *addr,
		MaxConnections: *maxConns,
		ReadTimeout:    *readTimeout,
		WriteTimeout:   *writeTimeout,
		Logger:         logger,
		Cluster:        clusterView,
	}, store)
	if err != nil {
		logger.Error("failed to create server", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go store.RunCleanup(ctx)

	logger.Info("starting cache node",
		"addr", *addr,
		"node_id", *nodeID,
		"max_bytes", *maxBytes,
		"cleanup_interval", storeCfg.CleanupInterval.String(),
		"vnodes", *vnodes,
	)
	if err := srv.ListenAndServe(ctx); err != nil {
		logger.Error("server stopped with error", "error", err)
		os.Exit(1)
	}

	logger.Info("server stopped")
}
