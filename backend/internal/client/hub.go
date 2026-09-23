package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"delivery-fullcycle/internal/model"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Config struct {
	ClientCount int
	OrdersCount int
	OrderURL    string
	DatabaseURL string
}

type Hub struct {
	config Config
	logger *slog.Logger
	pool   *pgxpool.Pool
}

func NewHub(cfg Config, pool *pgxpool.Pool, logger *slog.Logger) *Hub {
	return &Hub{
		config: cfg,
		logger: logger,
		pool:   pool,
	}
}

func (h *Hub) LoadUsers(ctx context.Context, limit int) ([]*Client, error) {
	query := `
		SELECT id, first_name || ' ' || last_name, email
		FROM users
		ORDER BY id
		LIMIT $1
	`
	rows, err := h.pool.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("query users: %w", err)
	}
	defer rows.Close()

	var clients []*Client
	for rows.Next() {
		var id int64
		var name, email string
		if err := rows.Scan(&id, &name, &email); err != nil {
			return nil, fmt.Errorf("scan user: %w", err)
		}
		clients = append(clients, NewClient(id, name, email))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows err: %w", err)
	}
	return clients, nil
}

func (h *Hub) LoadProducts(ctx context.Context) ([]model.Product, error) {
	query := `
		SELECT id, title, price, category, section, stock
		FROM products
		WHERE stock > 0
		ORDER BY id
	`
	rows, err := h.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query products: %w", err)
	}
	defer rows.Close()

	var products []model.Product
	for rows.Next() {
		var p model.Product
		if err := rows.Scan(&p.ID, &p.Title, &p.Price, &p.Category, &p.Section, &p.Stock); err != nil {
			return nil, fmt.Errorf("scan product: %w", err)
		}
		products = append(products, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows err: %w", err)
	}
	return products, nil
}

func (h *Hub) Run(ctx context.Context) error {
	clients, err := h.LoadUsers(ctx, h.config.ClientCount)
	if err != nil {
		return fmt.Errorf("load users: %w", err)
	}
	if len(clients) == 0 {
		return fmt.Errorf("no users found in database")
	}

	products, err := h.LoadProducts(ctx)
	if err != nil {
		return fmt.Errorf("load products: %w", err)
	}
	if len(products) == 0 {
		return fmt.Errorf("no products found in database")
	}

	h.logger.Info("loaded seed data from PostgreSQL",
		slog.Int("clients_loaded", len(clients)),
		slog.Int("products_loaded", len(products)),
	)

	httpClient := &http.Client{Timeout: 5 * time.Second}

	var wg sync.WaitGroup
	var successCount, failureCount int64
	var mu sync.Mutex

	for _, c := range clients {
		wg.Add(1)
		go func(cl *Client) {
			defer wg.Done()
			for j := 0; j < h.config.OrdersCount; j++ {
				select {
				case <-ctx.Done():
					return
				default:
				}

				cart := cl.MakeCart(products, 4)
				status, err := h.sendOrder(httpClient, cart)

				mu.Lock()
				if err != nil {
					failureCount++
					h.logger.Warn("order dispatch failed",
						slog.Int64("user_id", cl.ID),
						slog.String("email", cl.Email),
						slog.String("error", err.Error()),
					)
				} else {
					successCount++
					h.logger.Info("order dispatched",
						slog.Int64("user_id", cl.ID),
						slog.String("email", cl.Email),
						slog.Int("items", len(cart.Items)),
						slog.Float64("total", cart.TotalPrice),
						slog.Int("status_code", status),
					)
				}
				mu.Unlock()

				time.Sleep(50 * time.Millisecond)
			}
		}(c)
	}

	wg.Wait()

	h.logger.Info("clienthub execution finished",
		slog.Int64("successful_orders", successCount),
		slog.Int64("failed_orders", failureCount),
	)

	return nil
}

func (h *Hub) sendOrder(httpClient *http.Client, cart *model.Cart) (int, error) {
	body, err := json.Marshal(cart)
	if err != nil {
		return 0, fmt.Errorf("marshal cart: %w", err)
	}

	resp, err := httpClient.Post(h.config.OrderURL, "application/json", bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("post order: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return resp.StatusCode, fmt.Errorf("server responded with status %d", resp.StatusCode)
	}

	return resp.StatusCode, nil
}
