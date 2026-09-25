package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"delivery-fullcycle/services/media-service/internal/config"
	"delivery-fullcycle/services/media-service/internal/handler"
	"delivery-fullcycle/services/media-service/internal/metrics"
	"delivery-fullcycle/services/media-service/internal/storage"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	cfg := config.Load()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	poolConfig, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		logger.Error("invalid postgres url", slog.String("error", err.Error()))
		os.Exit(1)
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		logger.Error("connect postgres failed", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		logger.Error("ping postgres failed", slog.String("error", err.Error()))
		os.Exit(1)
	}
	logger.Info("connected to postgres")

	s3Client, err := storage.New(cfg.S3Endpoint, cfg.S3AccessKey, cfg.S3SecretKey, cfg.S3Bucket, cfg.S3SSL)
	if err != nil {
		logger.Error("init s3 failed", slog.String("error", err.Error()))
		os.Exit(1)
	}

	if err := s3Client.EnsureBucket(ctx, cfg.S3Bucket); err != nil {
		logger.Error("ensure s3 bucket failed", slog.String("error", err.Error()))
		os.Exit(1)
	}
	logger.Info("connected to rustfs s3", slog.String("bucket", cfg.S3Bucket), slog.String("endpoint", cfg.S3Endpoint))

	h := handler.NewMediaHandler(pool, s3Client, cfg.S3Bucket)

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(metrics.Middleware)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	r.Get("/healthz", h.Health)
	r.Get("/readyz", h.Health)
	r.Handle("/metrics", promhttp.Handler())

	r.Get("/media/{bucket}/*", h.ServeMedia)
	r.Head("/media/{bucket}/*", h.ServeMedia)

	r.Post("/api/media/upload", h.Upload)

	server := &http.Server{
		Addr:         cfg.Port,
		Handler:      r,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		logger.Info("media-service listening", slog.String("addr", cfg.Port))
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server failed", slog.String("error", err.Error()))
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down media-service...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("server forced to shutdown", slog.String("error", err.Error()))
	}

	logger.Info("media-service stopped cleanly")
}
