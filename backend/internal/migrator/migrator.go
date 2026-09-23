package migrator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"delivery-fullcycle/internal/storage"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Migrator struct {
	db         *pgxpool.Pool
	s3         *storage.Client
	httpClient *http.Client
	logger     *slog.Logger
	bucket     string
}

func New(db *pgxpool.Pool, s3 *storage.Client, bucket string, logger *slog.Logger) *Migrator {
	return &Migrator{
		db: db,
		s3: s3,
		httpClient: &http.Client{
			Timeout: 15 * time.Second,
		},
		logger: logger,
		bucket: bucket,
	}
}

func (m *Migrator) MigrateAll(ctx context.Context) (int, int, error) {
	if err := m.s3.EnsureBucket(ctx, m.bucket); err != nil {
		return 0, 0, fmt.Errorf("ensure s3 bucket: %w", err)
	}

	usersMigrated, err := m.MigrateUsers(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("migrate users: %w", err)
	}

	productsMigrated, err := m.MigrateProducts(ctx)
	if err != nil {
		return usersMigrated, 0, fmt.Errorf("migrate products: %w", err)
	}

	return usersMigrated, productsMigrated, nil
}

func (m *Migrator) MigrateUsers(ctx context.Context) (int, error) {
	query := `
		SELECT id, avatar_url
		FROM users
		WHERE avatar_url IS NOT NULL 
		  AND avatar_url LIKE 'http%' 
		  AND avatar_url NOT LIKE '%/media/%'
		ORDER BY id
	`
	rows, err := m.db.Query(ctx, query)
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

	type job struct {
		user userItem
	}
	jobs := make(chan job, len(items))
	for _, it := range items {
		jobs <- job{user: it}
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
			for j := range jobs {
				select {
				case <-ctx.Done():
					return
				default:
				}

				ext := extractExt(j.user.url, ".jpg")
				s3Key := fmt.Sprintf("avatars/%d%s", j.user.id, ext)

				data, contentType, err := m.download(ctx, j.user.url)
				if err != nil {
					m.logger.Warn("failed to download user avatar",
						slog.Int64("user_id", j.user.id),
						slog.String("url", j.user.url),
						slog.String("error", err.Error()),
					)
					continue
				}

				if err := m.s3.Upload(ctx, m.bucket, s3Key, bytes.NewReader(data), int64(len(data)), contentType); err != nil {
					m.logger.Warn("failed to upload avatar to s3",
						slog.Int64("user_id", j.user.id),
						slog.String("key", s3Key),
						slog.String("error", err.Error()),
					)
					continue
				}

				newURL := fmt.Sprintf("/media/%s/%s", m.bucket, s3Key)
				updateQuery := `UPDATE users SET avatar_url = $1, updated_at = NOW() WHERE id = $2`
				if _, err := m.db.Exec(ctx, updateQuery, newURL, j.user.id); err != nil {
					m.logger.Warn("failed to update user avatar link in db",
						slog.Int64("user_id", j.user.id),
						slog.String("error", err.Error()),
					)
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

func (m *Migrator) MigrateProducts(ctx context.Context) (int, error) {
	query := `
		SELECT id, thumbnail, images
		FROM products
		WHERE thumbnail IS NOT NULL 
		  AND thumbnail LIKE 'http%' 
		  AND thumbnail NOT LIKE '%/media/%'
		ORDER BY id
	`
	rows, err := m.db.Query(ctx, query)
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

	type job struct {
		prod prodItem
	}
	jobs := make(chan job, len(items))
	for _, it := range items {
		jobs <- job{prod: it}
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
			for j := range jobs {
				select {
				case <-ctx.Done():
					return
				default:
				}

				var newThumbnailURL string
				if j.prod.thumbnail != "" && strings.HasPrefix(j.prod.thumbnail, "http") {
					ext := extractExt(j.prod.thumbnail, ".webp")
					s3Key := fmt.Sprintf("products/%d/thumbnail%s", j.prod.id, ext)

					data, contentType, err := m.download(ctx, j.prod.thumbnail)
					if err == nil {
						if err := m.s3.Upload(ctx, m.bucket, s3Key, bytes.NewReader(data), int64(len(data)), contentType); err == nil {
							newThumbnailURL = fmt.Sprintf("/media/%s/%s", m.bucket, s3Key)
						}
					}
				}

				var oldImageURLs []string
				if len(j.prod.rawImages) > 0 {
					_ = json.Unmarshal(j.prod.rawImages, &oldImageURLs)
				}

				newImageURLs := make([]string, 0, len(oldImageURLs))
				for idx, imgURL := range oldImageURLs {
					if strings.HasPrefix(imgURL, "http") {
						ext := extractExt(imgURL, ".webp")
						s3Key := fmt.Sprintf("products/%d/image_%d%s", j.prod.id, idx+1, ext)

						data, contentType, err := m.download(ctx, imgURL)
						if err == nil {
							if err := m.s3.Upload(ctx, m.bucket, s3Key, bytes.NewReader(data), int64(len(data)), contentType); err == nil {
								newImageURLs = append(newImageURLs, fmt.Sprintf("/media/%s/%s", m.bucket, s3Key))
								continue
							}
						}
					}
					newImageURLs = append(newImageURLs, imgURL)
				}

				newImagesJSON, _ := json.Marshal(newImageURLs)
				if newThumbnailURL == "" {
					newThumbnailURL = j.prod.thumbnail
				}

				updateQuery := `UPDATE products SET thumbnail = $1, images = $2, updated_at = NOW() WHERE id = $3`
				if _, err := m.db.Exec(ctx, updateQuery, newThumbnailURL, newImagesJSON, j.prod.id); err != nil {
					m.logger.Warn("failed to update product media in db",
						slog.Int64("product_id", j.prod.id),
						slog.String("error", err.Error()),
					)
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

func (m *Migrator) download(ctx context.Context, rawURL string) ([]byte, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, "", fmt.Errorf("create req: %w", err)
	}
	req.Header.Set("User-Agent", "delivery-media-service/1.0")

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("do req: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("status %d", resp.StatusCode)
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

func extractExt(rawURL, fallback string) string {
	clean := strings.Split(rawURL, "?")[0]
	ext := path.Ext(clean)
	if ext == "" || len(ext) > 5 {
		return fallback
	}
	return ext
}
