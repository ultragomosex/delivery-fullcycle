package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type MediaMigrator struct {
	dbPool     *pgxpool.Pool
	s3Client   *minio.Client
	httpClient *http.Client
	logger     *slog.Logger
	bucket     string
}

func NewMediaMigrator(dbPool *pgxpool.Pool, s3Client *minio.Client, bucket string, logger *slog.Logger) *MediaMigrator {
	return &MediaMigrator{
		dbPool:   dbPool,
		s3Client: s3Client,
		httpClient: &http.Client{
			Timeout: 15 * time.Second,
		},
		logger: logger,
		bucket: bucket,
	}
}

func (m *MediaMigrator) EnsureBucket(ctx context.Context) error {
	exists, err := m.s3Client.BucketExists(ctx, m.bucket)
	if err != nil {
		return fmt.Errorf("check bucket exists: %w", err)
	}
	if !exists {
		err = m.s3Client.MakeBucket(ctx, m.bucket, minio.MakeBucketOptions{})
		if err != nil {
			return fmt.Errorf("create bucket: %w", err)
		}
	}
	return nil
}

func (m *MediaMigrator) MigrateUsers(ctx context.Context) (int, error) {
	query := `
		SELECT id, avatar_url
		FROM users
		WHERE avatar_url IS NOT NULL 
		  AND avatar_url LIKE 'http%' 
		  AND avatar_url NOT LIKE '%/media/%'
		ORDER BY id
	`
	rows, err := m.dbPool.Query(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("query users: %w", err)
	}
	defer rows.Close()

	type userItem struct {
		id  int64
		url string
	}
	var items []userItem
	for rows.Next() {
		var it userItem
		if err := rows.Scan(&it.id, &it.url); err != nil {
			return 0, fmt.Errorf("scan user: %w", err)
		}
		items = append(items, it)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("rows user err: %w", err)
	}

	if len(items) == 0 {
		m.logger.Info("no users pending media migration")
		return 0, nil
	}

	m.logger.Info("starting users media migration", slog.Int("count", len(items)))

	jobs := make(chan userItem, len(items))
	for _, it := range items {
		jobs <- it
	}
	close(jobs)

	var wg sync.WaitGroup
	var successCount int64
	var mu sync.Mutex

	concurrency := 8
	if len(items) < concurrency {
		concurrency = len(items)
	}

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for it := range jobs {
				select {
				case <-ctx.Done():
					return
				default:
				}

				ext := extractFileExt(it.url, ".jpg")
				s3Key := fmt.Sprintf("avatars/%d%s", it.id, ext)

				data, contentType, err := m.download(ctx, it.url)
				if err != nil {
					m.logger.Warn("failed to download avatar", slog.Int64("user_id", it.id), slog.String("error", err.Error()))
					continue
				}

				_, err = m.s3Client.PutObject(ctx, m.bucket, s3Key, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{
					ContentType: contentType,
				})
				if err != nil {
					m.logger.Warn("failed to upload avatar to s3", slog.Int64("user_id", it.id), slog.String("error", err.Error()))
					continue
				}

				newURL := fmt.Sprintf("/media/%s/%s", m.bucket, s3Key)
				updateQuery := `UPDATE users SET avatar_url = $1, updated_at = NOW() WHERE id = $2`
				if _, err := m.dbPool.Exec(ctx, updateQuery, newURL, it.id); err != nil {
					m.logger.Warn("failed to update user avatar in db", slog.Int64("user_id", it.id), slog.String("error", err.Error()))
					continue
				}

				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	m.logger.Info("completed users media migration", slog.Int64("migrated", successCount), slog.Int("total", len(items)))
	return int(successCount), nil
}

func (m *MediaMigrator) MigrateProducts(ctx context.Context) (int, error) {
	query := `
		SELECT id, thumbnail, images
		FROM products
		WHERE thumbnail IS NOT NULL 
		  AND thumbnail LIKE 'http%' 
		  AND thumbnail NOT LIKE '%/media/%'
		ORDER BY id
	`
	rows, err := m.dbPool.Query(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("query products: %w", err)
	}
	defer rows.Close()

	type prodItem struct {
		id        int64
		thumbnail string
		rawImages []byte
	}
	var items []prodItem
	for rows.Next() {
		var it prodItem
		if err := rows.Scan(&it.id, &it.thumbnail, &it.rawImages); err != nil {
			return 0, fmt.Errorf("scan product: %w", err)
		}
		items = append(items, it)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("rows product err: %w", err)
	}

	if len(items) == 0 {
		m.logger.Info("no products pending media migration")
		return 0, nil
	}

	m.logger.Info("starting products media migration", slog.Int("count", len(items)))

	jobs := make(chan prodItem, len(items))
	for _, it := range items {
		jobs <- it
	}
	close(jobs)

	var wg sync.WaitGroup
	var successCount int64
	var mu sync.Mutex

	concurrency := 8
	if len(items) < concurrency {
		concurrency = len(items)
	}

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for it := range jobs {
				select {
				case <-ctx.Done():
					return
				default:
				}

				var newThumbnailURL string
				if it.thumbnail != "" && strings.HasPrefix(it.thumbnail, "http") {
					ext := extractFileExt(it.thumbnail, ".webp")
					s3Key := fmt.Sprintf("products/%d/thumbnail%s", it.id, ext)

					data, contentType, err := m.download(ctx, it.thumbnail)
					if err == nil {
						_, err := m.s3Client.PutObject(ctx, m.bucket, s3Key, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{
							ContentType: contentType,
						})
						if err == nil {
							newThumbnailURL = fmt.Sprintf("/media/%s/%s", m.bucket, s3Key)
						}
					}
				}

				var oldImageURLs []string
				if len(it.rawImages) > 0 {
					_ = json.Unmarshal(it.rawImages, &oldImageURLs)
				}

				newImageURLs := make([]string, 0, len(oldImageURLs))
				for idx, imgURL := range oldImageURLs {
					if strings.HasPrefix(imgURL, "http") {
						ext := extractFileExt(imgURL, ".webp")
						s3Key := fmt.Sprintf("products/%d/image_%d%s", it.id, idx+1, ext)

						data, contentType, err := m.download(ctx, imgURL)
						if err == nil {
							_, err := m.s3Client.PutObject(ctx, m.bucket, s3Key, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{
								ContentType: contentType,
							})
							if err == nil {
								newImageURLs = append(newImageURLs, fmt.Sprintf("/media/%s/%s", m.bucket, s3Key))
								continue
							}
						}
					}
					newImageURLs = append(newImageURLs, imgURL)
				}

				newImagesJSON, _ := json.Marshal(newImageURLs)
				if newThumbnailURL == "" {
					newThumbnailURL = it.thumbnail
				}

				updateQuery := `UPDATE products SET thumbnail = $1, images = $2, updated_at = NOW() WHERE id = $3`
				if _, err := m.dbPool.Exec(ctx, updateQuery, newThumbnailURL, newImagesJSON, it.id); err != nil {
					m.logger.Warn("failed to update product in db", slog.Int64("product_id", it.id), slog.String("error", err.Error()))
					continue
				}

				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	m.logger.Info("completed products media migration", slog.Int64("migrated", successCount), slog.Int("total", len(items)))
	return int(successCount), nil
}

func (m *MediaMigrator) download(ctx context.Context, rawURL string) ([]byte, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("User-Agent", "delivery-media-migrator/1.0")

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", fmt.Errorf("read body: %w", err)
	}

	contentType := resp.Header.Get("Content-Type")
	if contentType == "" {
		contentType = http.DetectContentType(data)
	}

	return data, contentType, nil
}

func extractFileExt(rawURL, fallback string) string {
	clean := strings.Split(rawURL, "?")[0]
	ext := path.Ext(clean)
	if ext == "" || len(ext) > 5 {
		return fallback
	}
	return ext
}

func main() {
	var (
		dbURL       string
		s3Endpoint  string
		s3AccessKey string
		s3SecretKey string
		s3Bucket    string
		s3SSL       bool
		timeout     time.Duration
	)

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

	flag.StringVar(&dbURL, "db-url", defaultDB, "")
	flag.StringVar(&s3Endpoint, "s3-endpoint", defaultS3Endpoint, "")
	flag.StringVar(&s3AccessKey, "s3-access-key", defaultAccessKey, "")
	flag.StringVar(&s3SecretKey, "s3-secret-key", defaultSecretKey, "")
	flag.StringVar(&s3Bucket, "s3-bucket", defaultBucket, "")
	flag.BoolVar(&s3SSL, "s3-ssl", false, "")
	flag.DurationVar(&timeout, "timeout", 15*time.Minute, "")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

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

	s3Client, err := minio.New(s3Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(s3AccessKey, s3SecretKey, ""),
		Secure: s3SSL,
	})
	if err != nil {
		logger.Error("init s3 failed", slog.String("error", err.Error()))
		os.Exit(1)
	}

	mig := NewMediaMigrator(pool, s3Client, s3Bucket, logger)
	if err := mig.EnsureBucket(ctx); err != nil {
		logger.Error("ensure bucket failed", slog.String("error", err.Error()))
		os.Exit(1)
	}
	logger.Info("connected to rustfs s3", slog.String("bucket", s3Bucket))

	usersMigrated, err := mig.MigrateUsers(ctx)
	if err != nil {
		logger.Error("users migration error", slog.String("error", err.Error()))
	}

	productsMigrated, err := mig.MigrateProducts(ctx)
	if err != nil {
		logger.Error("products migration error", slog.String("error", err.Error()))
	}

	logger.Info("migration completed", slog.Int("users", usersMigrated), slog.Int("products", productsMigrated))
}
