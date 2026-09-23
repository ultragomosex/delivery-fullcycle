package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
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
)

// RandomUserResponse represents the root response from randomuser.me API.
type RandomUserResponse struct {
	Results []RandomUserItem `json:"results"`
	Info    struct {
		Seed    string `json:"seed"`
		Results int    `json:"results"`
		Page    int    `json:"page"`
		Version string `json:"version"`
	} `json:"info"`
}

// RandomUserItem maps the individual user payload from randomuser.me.
type RandomUserItem struct {
	Gender string `json:"gender"`
	Name   struct {
		Title string `json:"title"`
		First string `json:"first"`
		Last  string `json:"last"`
	} `json:"name"`
	Location struct {
		Street struct {
			Number int    `json:"number"`
			Name   string `json:"name"`
		} `json:"street"`
		City        string `json:"city"`
		State       string `json:"state"`
		Country     string `json:"country"`
		Postcode    any    `json:"postcode"`
		Coordinates struct {
			Latitude  string `json:"latitude"`
			Longitude string `json:"longitude"`
		} `json:"coordinates"`
	} `json:"location"`
	Email string `json:"email"`
	Login struct {
		UUID     string `json:"uuid"`
		Username string `json:"username"`
		Password string `json:"password"`
	} `json:"login"`
	Registered struct {
		Date time.Time `json:"date"`
		Age  int       `json:"age"`
	} `json:"registered"`
	Phone   string `json:"phone"`
	Cell    string `json:"cell"`
	Picture struct {
		Large     string `json:"large"`
		Medium    string `json:"medium"`
		Thumbnail string `json:"thumbnail"`
	} `json:"picture"`
	Nat string `json:"nat"`
}

// UserEntity is the domain model matching the PostgreSQL users table.
type UserEntity struct {
	UUID         string
	FirstName    string
	LastName     string
	Email        string
	Gender       string
	Phone        string
	Username     string
	PasswordHash string
	AvatarURL    string
	Street       string
	City         string
	Country      string
	Postcode     string
	RegisteredAt time.Time
}

// UsersParser handles HTTP fetching, data transformation, and PostgreSQL persistence.
type UsersParser struct {
	client    *http.Client
	dbPool    *pgxpool.Pool
	logger    *slog.Logger
	apiURL    string
	batchSize int
}

func NewUsersParser(dbPool *pgxpool.Pool, logger *slog.Logger, batchSize int) *UsersParser {
	return &UsersParser{
		client: &http.Client{
			Timeout: 20 * time.Second,
		},
		dbPool:    dbPool,
		logger:    logger,
		apiURL:    "https://randomuser.me/api/",
		batchSize: batchSize,
	}
}

// FetchUsers retrieves N users from randomuser.me with optional nationality filtering.
func (p *UsersParser) FetchUsers(ctx context.Context, count int, nat string) ([]UserEntity, error) {
	if count <= 0 {
		return nil, fmt.Errorf("count must be > 0, got %d", count)
	}

	u, err := url.Parse(p.apiURL)
	if err != nil {
		return nil, fmt.Errorf("parse api url: %w", err)
	}

	q := u.Query()
	q.Set("results", strconv.Itoa(count))
	if nat != "" {
		q.Set("nat", nat)
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "delivery-fullcycle-importer/1.0")

	p.logger.Info("fetching users from external API", slog.String("url", u.String()), slog.Int("requested", count))

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code %d from %s", resp.StatusCode, u.String())
	}

	var payload RandomUserResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode json response: %w", err)
	}

	users := make([]UserEntity, 0, len(payload.Results))
	for _, item := range payload.Results {
		postcodeStr := fmt.Sprintf("%v", item.Location.Postcode)
		streetStr := fmt.Sprintf("%d %s", item.Location.Street.Number, item.Location.Street.Name)

		user := UserEntity{
			UUID:         item.Login.UUID,
			FirstName:    item.Name.First,
			LastName:     item.Name.Last,
			Email:        strings.ToLower(strings.TrimSpace(item.Email)),
			Gender:       item.Gender,
			Phone:        item.Phone,
			Username:     item.Login.Username,
			PasswordHash: item.Login.Password,
			AvatarURL:    item.Picture.Large,
			Street:       streetStr,
			City:         item.Location.City,
			Country:      item.Location.Country,
			Postcode:     postcodeStr,
			RegisteredAt: item.Registered.Date,
		}
		users = append(users, user)
	}

	p.logger.Info("successfully fetched and parsed users", slog.Int("parsed_count", len(users)))
	return users, nil
}

// Migrate ensures the PostgreSQL table and required indexes exist.
func (p *UsersParser) Migrate(ctx context.Context) error {
	schema := `
	CREATE TABLE IF NOT EXISTS users (
		id BIGSERIAL PRIMARY KEY,
		uuid UUID UNIQUE,
		first_name VARCHAR(100) NOT NULL,
		last_name VARCHAR(100) NOT NULL,
		email VARCHAR(255) UNIQUE NOT NULL,
		gender VARCHAR(20),
		phone VARCHAR(50),
		username VARCHAR(100),
		password_hash TEXT,
		avatar_url TEXT,
		street VARCHAR(255),
		city VARCHAR(100),
		country VARCHAR(100),
		postcode VARCHAR(30),
		registered_at TIMESTAMPTZ,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);

	CREATE INDEX IF NOT EXISTS idx_users_email ON users (email);
	CREATE INDEX IF NOT EXISTS idx_users_uuid ON users (uuid);
	`
	p.logger.Info("ensuring users table schema and indexes exist")
	_, err := p.dbPool.Exec(ctx, schema)
	if err != nil {
		return fmt.Errorf("execute schema migration: %w", err)
	}
	return nil
}

// UpsertBatch saves users in batches using PostgreSQL ON CONFLICT for idempotency.
func (p *UsersParser) UpsertBatch(ctx context.Context, users []UserEntity) (int, error) {
	if len(users) == 0 {
		return 0, nil
	}

	query := `
	INSERT INTO users (
		uuid, first_name, last_name, email, gender, phone, username,
		password_hash, avatar_url, street, city, country, postcode, registered_at
	) VALUES (
		NULLIF($1, '')::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
	)
	ON CONFLICT (email) DO UPDATE SET
		first_name = EXCLUDED.first_name,
		last_name = EXCLUDED.last_name,
		phone = EXCLUDED.phone,
		username = EXCLUDED.username,
		avatar_url = EXCLUDED.avatar_url,
		street = EXCLUDED.street,
		city = EXCLUDED.city,
		country = EXCLUDED.country,
		postcode = EXCLUDED.postcode,
		updated_at = NOW();
	`

	totalInserted := 0
	for i := 0; i < len(users); i += p.batchSize {
		end := i + p.batchSize
		if end > len(users) {
			end = len(users)
		}
		chunk := users[i:end]

		batch := &pgx.Batch{}
		for _, u := range chunk {
			batch.Queue(query,
				u.UUID,
				u.FirstName,
				u.LastName,
				u.Email,
				u.Gender,
				u.Phone,
				u.Username,
				u.PasswordHash,
				u.AvatarURL,
				u.Street,
				u.City,
				u.Country,
				u.Postcode,
				u.RegisteredAt,
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

		p.logger.Info("batch upserted", slog.Int("processed", totalInserted), slog.Int("total", len(users)))
	}

	return totalInserted, nil
}

func main() {
	var (
		dbURL     string
		count     int
		batchSize int
		nat       string
		dryRun    bool
		timeout   time.Duration
	)

	defaultDB := os.Getenv("DATABASE_URL")
	if defaultDB == "" {
		defaultDB = "postgres://postgres:postgres@localhost:5432/delivery?sslmode=disable"
	}

	flag.StringVar(&dbURL, "db-url", defaultDB, "PostgreSQL connection string (or DATABASE_URL env)")
	flag.IntVar(&count, "count", 100, "Number of users to fetch from randomuser.me")
	flag.IntVar(&batchSize, "batch-size", 50, "Batch size for database upserts")
	flag.StringVar(&nat, "nat", "", "Nationalities filter (e.g. 'us,gb,de', empty for all)")
	flag.BoolVar(&dryRun, "dry-run", false, "Fetch and parse only without connecting to database")
	flag.DurationVar(&timeout, "timeout", 60*time.Second, "Execution timeout")
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

	parser := NewUsersParser(pool, logger, batchSize)

	users, err := parser.FetchUsers(ctx, count, nat)
	if err != nil {
		logger.Error("failed to fetch users", slog.String("error", err.Error()))
		os.Exit(1)
	}

	if dryRun {
		logger.Info("DRY-RUN mode enabled: skipping database insert")
		if len(users) > 0 {
			sample := users[0]
			fmt.Printf("\n--- Sample Parsed User ---\nName:     %s %s\nEmail:    %s\nGender:   %s\nPhone:    %s\nAddress:  %s, %s, %s\nUUID:     %s\nAvatar:   %s\n--------------------------\n\n",
				sample.FirstName, sample.LastName, sample.Email, sample.Gender, sample.Phone, sample.Street, sample.City, sample.Country, sample.UUID, sample.AvatarURL)
		}
		logger.Info("dry-run finished successfully", slog.Int("total_parsed", len(users)))
		return
	}

	if err := parser.Migrate(ctx); err != nil {
		logger.Error("database migration failed", slog.String("error", err.Error()))
		os.Exit(1)
	}

	start := time.Now()
	inserted, err := parser.UpsertBatch(ctx, users)
	if err != nil {
		logger.Error("upsert users failed", slog.String("error", err.Error()))
		os.Exit(1)
	}

	logger.Info("import completed successfully",
		slog.Int("imported_users", inserted),
		slog.Duration("elapsed", time.Since(start)),
	)
}
