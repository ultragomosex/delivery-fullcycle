package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"
	"time"

	"delivery-fullcycle/internal/migrator"
	"delivery-fullcycle/internal/storage"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	var (
		port            string
		dbURL           string
		s3Endpoint      string
		s3AccessKey     string
		s3SecretKey     string
		s3Bucket        string
		s3SSL           bool
		migrateOnStart  bool
		runOnceMigrate  bool
		shutdownTimeout time.Duration
	)

	defaultPort := os.Getenv("PORT")
	if defaultPort == "" {
		defaultPort = ":8081"
	}
	if !strings.HasPrefix(defaultPort, ":") {
		defaultPort = ":" + defaultPort
	}

	defaultDB := os.Getenv("DATABASE_URL")
	if defaultDB == "" {
		defaultDB = "postgres://delivery:delivery123@localhost:5432/delivery?sslmode=disable"
	}

	defaultS3Endpoint := os.Getenv("S3_ENDPOINT")
	if defaultS3Endpoint == "" {
		defaultS3Endpoint = "127.0.0.1:9000"
	}

	defaultAccessKey := os.Getenv("S3_ACCESS_KEY")
	if defaultAccessKey == "" {
		defaultAccessKey = "ea5fc5464891940eeebaceb2"
	}

	defaultSecretKey := os.Getenv("S3_SECRET_KEY")
	if defaultSecretKey == "" {
		defaultSecretKey = "4b2d3855a3832263df61c340b1c0526cf1dde4c4b702c8a1"
	}

	defaultBucket := os.Getenv("S3_BUCKET")
	if defaultBucket == "" {
		defaultBucket = "delivery-media"
	}

	flag.StringVar(&port, "port", defaultPort, "")
	flag.StringVar(&dbURL, "db-url", defaultDB, "")
	flag.StringVar(&s3Endpoint, "s3-endpoint", defaultS3Endpoint, "")
	flag.StringVar(&s3AccessKey, "s3-access-key", defaultAccessKey, "")
	flag.StringVar(&s3SecretKey, "s3-secret-key", defaultSecretKey, "")
	flag.StringVar(&s3Bucket, "s3-bucket", defaultBucket, "")
	flag.BoolVar(&s3SSL, "s3-ssl", false, "")
	flag.BoolVar(&migrateOnStart, "migrate", false, "")
	flag.BoolVar(&runOnceMigrate, "run-once-migrate", false, "")
	flag.DurationVar(&shutdownTimeout, "shutdown-timeout", 10*time.Second, "")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	poolConfig, err := pgxpool.ParseConfig(dbURL)
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

	s3Client, err := storage.New(s3Endpoint, s3AccessKey, s3SecretKey, s3Bucket, s3SSL)
	if err != nil {
		logger.Error("init s3 failed", slog.String("error", err.Error()))
		os.Exit(1)
	}

	if err := s3Client.EnsureBucket(ctx, s3Bucket); err != nil {
		logger.Error("ensure s3 bucket failed", slog.String("error", err.Error()))
		os.Exit(1)
	}
	logger.Info("connected to rustfs s3", slog.String("bucket", s3Bucket), slog.String("endpoint", s3Endpoint))

	mig := migrator.New(pool, s3Client, s3Bucket, logger)

	if runOnceMigrate {
		logger.Info("running one-time media migration")
		uCount, pCount, err := mig.MigrateAll(ctx)
		if err != nil {
			logger.Error("migration failed", slog.String("error", err.Error()))
			os.Exit(1)
		}
		logger.Info("one-time media migration finished", slog.Int("users", uCount), slog.Int("products", pCount))
		return
	}

	if migrateOnStart {
		go func() {
			bgCtx, bgCancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer bgCancel()
			uCount, pCount, err := mig.MigrateAll(bgCtx)
			if err != nil {
				logger.Error("background migration failed", slog.String("error", err.Error()))
			} else {
				logger.Info("background migration finished", slog.Int("users", uCount), slog.Int("products", pCount))
			}
		}()
	}

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		hCtx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		dbErr := pool.Ping(hCtx)
		s3Err := s3Client.Ping(hCtx)

		status := "ok"
		code := http.StatusOK
		if dbErr != nil || s3Err != nil {
			status = "degraded"
			code = http.StatusServiceUnavailable
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":   status,
			"database": dbErr == nil,
			"s3":       s3Err == nil,
			"time":     time.Now().UTC().Format(time.RFC3339),
		})
	})

	mediaHandler := func(w http.ResponseWriter, r *http.Request) {
		bucket := chi.URLParam(r, "bucket")
		objectKey := chi.URLParam(r, "*")
		if objectKey == "" {
			http.Error(w, "object key required", http.StatusBadRequest)
			return
		}

		obj, contentType, size, err := s3Client.GetObject(r.Context(), bucket, objectKey)
		if err != nil {
			http.Error(w, "media not found", http.StatusNotFound)
			return
		}
		defer obj.Close()

		w.Header().Set("Content-Type", contentType)
		if size > 0 {
			w.Header().Set("Content-Length", fmt.Sprintf("%d", size))
		}
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")

		if r.Method == http.MethodGet {
			_, _ = io.Copy(w, obj)
		}
	}

	r.Get("/media/{bucket}/*", mediaHandler)
	r.Head("/media/{bucket}/*", mediaHandler)


	r.Post("/api/media/upload", func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseMultipartForm(32 << 20); err != nil {
			http.Error(w, "invalid multipart form: "+err.Error(), http.StatusBadRequest)
			return
		}

		file, header, err := r.FormFile("file")
		if err != nil {
			http.Error(w, "missing file field: "+err.Error(), http.StatusBadRequest)
			return
		}
		defer file.Close()

		targetBucket := r.FormValue("bucket")
		if targetBucket == "" {
			targetBucket = s3Bucket
		}

		folder := r.FormValue("folder")
		if folder == "" {
			folder = "uploads"
		}

		filename := header.Filename
		ext := path.Ext(filename)
		key := fmt.Sprintf("%s/%d%s", folder, time.Now().UnixNano(), ext)

		contentType := header.Header.Get("Content-Type")
		if contentType == "" {
			contentType = "application/octet-stream"
		}

		if err := s3Client.Upload(r.Context(), targetBucket, key, file, header.Size, contentType); err != nil {
			http.Error(w, "failed to upload to s3: "+err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"bucket": targetBucket,
			"key":    key,
			"url":    fmt.Sprintf("/media/%s/%s", targetBucket, key),
			"size":   header.Size,
		})
	})

	r.Post("/api/media/migrate", func(w http.ResponseWriter, r *http.Request) {
		wait := r.URL.Query().Get("wait") == "true"
		if wait {
			uCount, pCount, err := mig.MigrateAll(r.Context())
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status":   "completed",
				"users":    uCount,
				"products": pCount,
			})
			return
		}

		go func() {
			bgCtx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
			defer cancel()
			uCount, pCount, err := mig.MigrateAll(bgCtx)
			if err != nil {
				logger.Error("migration trigger failed", slog.String("error", err.Error()))
			} else {
				logger.Info("migration trigger finished", slog.Int("users", uCount), slog.Int("products", pCount))
			}
		}()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status": "migration_started",
		})
	})

	server := &http.Server{
		Addr:         port,
		Handler:      r,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		logger.Info("media-service listening", slog.String("addr", port))
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server failed", slog.String("error", err.Error()))
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down media-service...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("server forced to shutdown", slog.String("error", err.Error()))
	}

	logger.Info("media-service stopped cleanly")
}
