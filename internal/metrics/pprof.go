package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"

	"github.com/distributed-cache/distributed-cache/internal/discovery"
)

// DebugOptions configures the debug HTTP server.
type DebugOptions struct {
	Registry  *Registry
	Discovery discovery.Provider
}

func newDebugMux(opts DebugOptions) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	if opts.Registry != nil {
		mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(opts.Registry.Snapshot())
		})
	}
	if opts.Discovery != nil {
		mux.HandleFunc("/discovery", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(opts.Discovery.Snapshot())
		})
	}
	registerPProf(mux)
	return mux
}

// registerPProf attaches pprof handlers to mux.
// The blank import of net/http/pprof only registers routes on http.DefaultServeMux;
// a custom mux must register handlers explicitly.
func registerPProf(mux *http.ServeMux) {
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
}

// StartDebug listens on addr and serves health, metrics, discovery, and pprof endpoints.
// It returns the bound address and runs until ctx is canceled.
func StartDebug(ctx context.Context, addr string, opts DebugOptions, logger *slog.Logger) (string, error) {
	if addr == "" {
		return "", nil
	}
	if logger == nil {
		logger = slog.Default()
	}

	server := &http.Server{Handler: newDebugMux(opts)}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return "", fmt.Errorf("listen debug server on %s: %w", addr, err)
	}

	bound := listener.Addr().String()
	go func() {
		<-ctx.Done()
		_ = server.Shutdown(context.Background())
	}()
	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			logger.Error("debug server stopped with error", "error", err)
		}
	}()

	logger.Info("debug server listening",
		"addr", bound,
		"metrics", opts.Registry != nil,
		"discovery", opts.Discovery != nil,
	)
	return bound, nil
}

// ServeDebug is a blocking variant of StartDebug for simple main goroutine use.
func ServeDebug(ctx context.Context, addr string, opts DebugOptions, logger *slog.Logger) error {
	if addr == "" {
		return nil
	}
	if logger == nil {
		logger = slog.Default()
	}

	server := &http.Server{Handler: newDebugMux(opts)}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen debug server on %s: %w", addr, err)
	}

	go func() {
		<-ctx.Done()
		_ = server.Shutdown(context.Background())
	}()

	logger.Info("debug server listening",
		"addr", listener.Addr().String(),
		"metrics", opts.Registry != nil,
		"discovery", opts.Discovery != nil,
	)
	return server.Serve(listener)
}
