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
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type DummyProductsResponse struct {
	Products []DummyProductItem `json:"products"`
	Total    int                `json:"total"`
	Skip     int                `json:"skip"`
	Limit    int                `json:"limit"`
}

type DummyProductItem struct {
	ID                 int      `json:"id"`
	Title              string   `json:"title"`
	Description        string   `json:"description"`
	Category           string   `json:"category"`
	Price              float64  `json:"price"`
	DiscountPercentage float64  `json:"discountPercentage"`
	Rating             float64  `json:"rating"`
	Stock              int      `json:"stock"`
	Tags               []string `json:"tags"`
	Brand              string   `json:"brand"`
	SKU                string   `json:"sku"`
	Weight             float64  `json:"weight"`
	AvailabilityStatus string   `json:"availabilityStatus"`
	Thumbnail          string   `json:"thumbnail"`
	Images             []string `json:"images"`
}

type ProductEntity struct {
	ExternalID         int
	Title              string
	Description        string
	Category           string
	Section            string
	Price              float64
	DiscountPercentage float64
	Rating             float64
	Stock              int
	Brand              string
	SKU                string
	Weight             float64
	AvailabilityStatus string
	Thumbnail          string
	ImagesJSON         []byte
	TagsJSON           []byte
}

func MapToSection(category string) string {
	cat := strings.ToLower(strings.TrimSpace(category))
	switch cat {
	case "groceries":
		return "food"
	case "smartphones", "laptops", "tablets", "mobile-accessories":
		return "electronics"
	case "kitchen-accessories", "home-decoration", "furniture", "beauty", "skin-care", "fragrances":
		return "household"
	default:
		return "general"
	}
}

type ProductsParser struct {
	client    *http.Client
	dbPool    *pgxpool.Pool
	s3Client  *minio.Client
	s3Bucket  string
	uploadS3  bool
	logger    *slog.Logger
	apiURL    string
	batchSize int
}

func NewProductsParser(dbPool *pgxpool.Pool, s3Client *minio.Client, s3Bucket string, uploadS3 bool, logger *slog.Logger, batchSize int) *ProductsParser {
	return &ProductsParser{
		client: &http.Client{
			Timeout: 20 * time.Second,
		},
		dbPool:    dbPool,
		s3Client:  s3Client,
		s3Bucket:  s3Bucket,
		uploadS3:  uploadS3,
		logger:    logger,
		apiURL:    "https://dummyjson.com/products",
		batchSize: batchSize,
	}
}

func (p *ProductsParser) FetchProducts(ctx context.Context, limit int, sectionFilter string) ([]ProductEntity, error) {
	u, err := url.Parse(p.apiURL)
	if err != nil {
		return nil, fmt.Errorf("parse api url: %w", err)
	}

	q := u.Query()
	q.Set("limit", strconv.Itoa(limit))
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "delivery-fullcycle-importer/1.0")

	p.logger.Info("fetching products from external API", slog.String("url", u.String()), slog.Int("limit", limit))

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code %d from %s", resp.StatusCode, u.String())
	}

	var payload DummyProductsResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode json response: %w", err)
	}

	entities := make([]ProductEntity, 0, len(payload.Products))
	sectionFilter = strings.ToLower(strings.TrimSpace(sectionFilter))

	for _, item := range payload.Products {
		section := MapToSection(item.Category)

		if sectionFilter != "" && sectionFilter != "all" && section != sectionFilter {
			continue
		}

		thumbnailURL := item.Thumbnail
		var s3Images []string

		if p.uploadS3 && p.s3Client != nil {
			if thumbnailURL != "" {
				s3URL, err := p.uploadImageToS3(ctx, fmt.Sprintf("products/%d/thumbnail.webp", item.ID), thumbnailURL)
				if err == nil {
					thumbnailURL = s3URL
				}
			}

			for idx, img := range item.Images {
				s3URL, err := p.uploadImageToS3(ctx, fmt.Sprintf("products/%d/image_%d.webp", item.ID, idx+1), img)
				if err == nil {
					s3Images = append(s3Images, s3URL)
				} else {
					s3Images = append(s3Images, img)
				}
			}
		} else {
			s3Images = item.Images
		}

		imagesJSON, _ := json.Marshal(s3Images)
		tagsJSON, _ := json.Marshal(item.Tags)

		brand := item.Brand
		if brand == "" {
			brand = "Generic"
		}

		status := item.AvailabilityStatus
		if status == "" {
			status = "In Stock"
		}

		entity := ProductEntity{
			ExternalID:         item.ID,
			Title:              strings.TrimSpace(item.Title),
			Description:        strings.TrimSpace(item.Description),
			Category:           item.Category,
			Section:            section,
			Price:              item.Price,
			DiscountPercentage: item.DiscountPercentage,
			Rating:             item.Rating,
			Stock:              item.Stock,
			Brand:              brand,
			SKU:                item.SKU,
			Weight:             item.Weight,
			AvailabilityStatus: status,
			Thumbnail:          thumbnailURL,
			ImagesJSON:         imagesJSON,
			TagsJSON:           tagsJSON,
		}
		entities = append(entities, entity)
	}

	p.logger.Info("successfully parsed products",
		slog.Int("total_received", payload.Total),
		slog.Int("filtered_matched", len(entities)),
	)
	return entities, nil
}

func (p *ProductsParser) uploadImageToS3(ctx context.Context, s3Key, imageURL string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, imageURL, nil)
	if err != nil {
		return "", err
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("status %d", resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	contentType := resp.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "image/webp"
	}

	_, err = p.s3Client.PutObject(ctx, p.s3Bucket, s3Key, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{
		ContentType: contentType,
	})
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("/media/%s/%s", p.s3Bucket, s3Key), nil
}

func (p *ProductsParser) Migrate(ctx context.Context) error {
	schema := `
	CREATE TABLE IF NOT EXISTS products (
		id BIGSERIAL PRIMARY KEY,
		external_id INT UNIQUE NOT NULL,
		title VARCHAR(255) NOT NULL,
		description TEXT,
		category VARCHAR(100) NOT NULL,
		section VARCHAR(50) NOT NULL DEFAULT 'general',
		price NUMERIC(12, 2) NOT NULL DEFAULT 0.00,
		discount_percentage NUMERIC(5, 2) DEFAULT 0.00,
		rating NUMERIC(3, 2) DEFAULT 0.00,
		stock INT DEFAULT 0,
		brand VARCHAR(100),
		sku VARCHAR(100),
		weight NUMERIC(8, 2),
		availability_status VARCHAR(50),
		thumbnail TEXT,
		images JSONB,
		tags JSONB,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);

	CREATE INDEX IF NOT EXISTS idx_products_external_id ON products (external_id);
	CREATE INDEX IF NOT EXISTS idx_products_category ON products (category);
	CREATE INDEX IF NOT EXISTS idx_products_section ON products (section);
	`
	p.logger.Info("ensuring products table schema and indexes exist")
	_, err := p.dbPool.Exec(ctx, schema)
	if err != nil {
		return fmt.Errorf("execute schema migration: %w", err)
	}
	return nil
}

func (p *ProductsParser) UpsertBatch(ctx context.Context, products []ProductEntity) (int, error) {
	if len(products) == 0 {
		return 0, nil
	}

	query := `
	INSERT INTO products (
		external_id, title, description, category, section, price,
		discount_percentage, rating, stock, brand, sku, weight,
		availability_status, thumbnail, images, tags
	) VALUES (
		$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
	)
	ON CONFLICT (external_id) DO UPDATE SET
		title = EXCLUDED.title,
		description = EXCLUDED.description,
		category = EXCLUDED.category,
		section = EXCLUDED.section,
		price = EXCLUDED.price,
		discount_percentage = EXCLUDED.discount_percentage,
		rating = EXCLUDED.rating,
		stock = EXCLUDED.stock,
		brand = EXCLUDED.brand,
		sku = EXCLUDED.sku,
		weight = EXCLUDED.weight,
		availability_status = EXCLUDED.availability_status,
		thumbnail = EXCLUDED.thumbnail,
		images = EXCLUDED.images,
		tags = EXCLUDED.tags,
		updated_at = NOW();
	`

	totalInserted := 0
	for i := 0; i < len(products); i += p.batchSize {
		end := i + p.batchSize
		if end > len(products) {
			end = len(products)
		}
		chunk := products[i:end]

		batch := &pgx.Batch{}
		for _, prod := range chunk {
			batch.Queue(query,
				prod.ExternalID,
				prod.Title,
				prod.Description,
				prod.Category,
				prod.Section,
				prod.Price,
				prod.DiscountPercentage,
				prod.Rating,
				prod.Stock,
				prod.Brand,
				prod.SKU,
				prod.Weight,
				prod.AvailabilityStatus,
				prod.Thumbnail,
				prod.ImagesJSON,
				prod.TagsJSON,
			)
		}

		br := p.dbPool.SendBatch(ctx, batch)
		for j := 0; j < len(chunk); j++ {
			if _, err := br.Exec(); err != nil {
				_ = br.Close()
				return totalInserted, fmt.Errorf("exec batch item at index %d: %w", i+j, err)
			}
			totalInserted++
		}
		if err := br.Close(); err != nil {
			return totalInserted, fmt.Errorf("close batch results: %w", err)
		}

		p.logger.Info("batch upserted", slog.Int("processed", totalInserted), slog.Int("total", len(products)))
	}

	return totalInserted, nil
}

func main() {
	var (
		dbURL       string
		s3Endpoint  string
		s3AccessKey string
		s3SecretKey string
		s3Bucket    string
		uploadS3    bool
		limit       int
		section     string
		batchSize   int
		dryRun      bool
		timeout     time.Duration
	)

	defaultDB := os.Getenv("DATABASE_URL")
	if defaultDB == "" {
		defaultDB = "postgres://postgres:postgres@localhost:5432/delivery?sslmode=disable"
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
	flag.BoolVar(&uploadS3, "upload-s3", true, "")
	flag.IntVar(&limit, "limit", 0, "")
	flag.StringVar(&section, "section", "all", "")
	flag.IntVar(&batchSize, "batch-size", 50, "")
	flag.BoolVar(&dryRun, "dry-run", false, "")
	flag.DurationVar(&timeout, "timeout", 60*time.Second, "")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var pool *pgxpool.Pool
	if !dryRun {
		poolConfig, err := pgxpool.ParseConfig(dbURL)
		if err != nil {
			logger.Error("invalid postgres database url", slog.String("error", err.Error()))
			os.Exit(1)
		}
		poolConfig.MaxConns = 10
		poolConfig.MinConns = 2

		var connErr error
		pool, connErr = pgxpool.NewWithConfig(ctx, poolConfig)
		if connErr != nil {
			logger.Error("failed to create connection pool", slog.String("error", connErr.Error()))
			os.Exit(1)
		}
		defer pool.Close()

		if err := pool.Ping(ctx); err != nil {
			logger.Error("database ping failed", slog.String("error", err.Error()), slog.String("db_url", dbURL))
			os.Exit(1)
		}
		logger.Info("successfully connected to PostgreSQL")
	}

	var s3Client *minio.Client
	if uploadS3 && !dryRun {
		var err error
		s3Client, err = minio.New(s3Endpoint, &minio.Options{
			Creds:  credentials.NewStaticV4(s3AccessKey, s3SecretKey, ""),
			Secure: false,
		})
		if err != nil {
			logger.Warn("s3 client init failed, disabling s3 upload", slog.String("error", err.Error()))
			uploadS3 = false
		} else {
			exists, err := s3Client.BucketExists(ctx, s3Bucket)
			if err == nil && !exists {
				_ = s3Client.MakeBucket(ctx, s3Bucket, minio.MakeBucketOptions{})
			}
			logger.Info("connected to s3 for direct uploads", slog.String("bucket", s3Bucket))
		}
	}

	parser := NewProductsParser(pool, s3Client, s3Bucket, uploadS3, logger, batchSize)

	products, err := parser.FetchProducts(ctx, limit, section)
	if err != nil {
		logger.Error("failed to fetch products", slog.String("error", err.Error()))
		os.Exit(1)
	}

	sectionCounts := make(map[string]int)
	for _, p := range products {
		sectionCounts[p.Section]++
	}

	if dryRun {
		logger.Info("DRY-RUN mode enabled: skipping database insert")
		fmt.Printf("\n--- Products Section Breakdown ---\n")
		for sec, cnt := range sectionCounts {
			fmt.Printf("  • %-12s: %d items\n", sec, cnt)
		}
		fmt.Printf("  Total: %d\n", len(products))

		if len(products) > 0 {
			sample := products[0]
			fmt.Printf("\n--- Sample Parsed Product ---\nID:          #%d\nTitle:       %s\nCategory:    %s (section: %s)\nPrice:       $%.2f (discount: %.1f%%)\nRating:      %.2f / 5.0 (stock: %d)\nBrand:       %s (SKU: %s)\nThumbnail:   %s\n-----------------------------\n\n",
				sample.ExternalID, sample.Title, sample.Category, sample.Section, sample.Price, sample.DiscountPercentage, sample.Rating, sample.Stock, sample.Brand, sample.SKU, sample.Thumbnail)
		}
		logger.Info("dry-run finished successfully", slog.Int("total_parsed", len(products)))
		return
	}

	if err := parser.Migrate(ctx); err != nil {
		logger.Error("database migration failed", slog.String("error", err.Error()))
		os.Exit(1)
	}

	start := time.Now()
	inserted, err := parser.UpsertBatch(ctx, products)
	if err != nil {
		logger.Error("upsert products failed", slog.String("error", err.Error()))
		os.Exit(1)
	}

	logger.Info("import completed successfully",
		slog.Int("imported_products", inserted),
		slog.Duration("elapsed", time.Since(start)),
	)
}
