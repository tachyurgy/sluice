// Sluice is an exactly-once ingest gateway for high-volume edge device fleets.
//
// It accepts batched asset deliveries over HTTP, stores each asset exactly once
// no matter how many times a device redelivers it, and tracks a per-device
// contiguous delivery watermark so assets lost in transit are detectable rather
// than silently missing.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/tachyurgy/sluice/internal/api"
	"github.com/tachyurgy/sluice/internal/ingest"
	"github.com/tachyurgy/sluice/internal/store"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Error("DATABASE_URL is required")
		os.Exit(1)
	}
	addr := ":" + envOr("PORT", "8080")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := openWithRetry(ctx, dsn, log)
	if err != nil {
		log.Error("database unreachable", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	if err := st.Migrate(ctx); err != nil {
		log.Error("migrate failed", "err", err)
		os.Exit(1)
	}

	pipe := ingest.New(st, ingest.Config{
		QueueDepth:   envInt("QUEUE_DEPTH", 8192),
		Workers:      envInt("WORKERS", 4),
		BatchSize:    envInt("BATCH_SIZE", 128),
		FlushTimeout: time.Duration(envInt("FLUSH_MS", 50)) * time.Millisecond,
	})
	pipe.Start(ctx)

	srv := &http.Server{
		Addr:              addr,
		Handler:           api.New(st, pipe, log),
		ReadHeaderTimeout: 10 * time.Second,
		WriteTimeout:      30 * time.Second,
	}

	go func() {
		log.Info("sluice listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server failed", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")

	// Stop accepting, then let the pipeline drain what it already acknowledged.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("http shutdown", "err", err)
	}
	pipe.Stop()
	log.Info("stopped cleanly")
}

// openWithRetry tolerates the database not being ready yet, which happens on a
// cold start where the app container comes up before Postgres accepts
// connections. Without this the container just crash-loops.
func openWithRetry(ctx context.Context, dsn string, log *slog.Logger) (*store.Store, error) {
	var lastErr error
	for attempt := 1; attempt <= 10; attempt++ {
		st, err := store.Open(ctx, dsn)
		if err == nil {
			return st, nil
		}
		lastErr = err
		log.Warn("database not ready, retrying", "attempt", attempt, "err", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Duration(attempt) * time.Second):
		}
	}
	return nil, lastErr
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
