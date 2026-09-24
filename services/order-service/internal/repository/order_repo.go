package repository

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"delivery-fullcycle/services/order-service/internal/model"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrOrderNotFound = errors.New("order not found")
	ErrEmptyCart     = errors.New("cannot create order with empty cart")
	ErrProductStock  = errors.New("insufficient stock for product")
	ErrProductNotFound = errors.New("product not found")
)

type OrderRepository struct {
	pool *pgxpool.Pool
}

func NewOrderRepository(pool *pgxpool.Pool) *OrderRepository {
	return &OrderRepository{pool: pool}
}

func (r *OrderRepository) Migrate(ctx context.Context) error {
	schema := `
	CREATE TABLE IF NOT EXISTS orders (
		id BIGSERIAL PRIMARY KEY,
		user_id BIGINT NOT NULL,
		user_email VARCHAR(255),
		status VARCHAR(50) NOT NULL DEFAULT 'confirmed',
		total_price NUMERIC(12, 2) NOT NULL DEFAULT 0.00,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);

	CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id);
	CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
	CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders(created_at DESC);

	CREATE TABLE IF NOT EXISTS order_items (
		id BIGSERIAL PRIMARY KEY,
		order_id BIGINT NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
		product_id BIGINT NOT NULL,
		product_title VARCHAR(255),
		unit_price NUMERIC(12, 2) NOT NULL DEFAULT 0.00,
		quantity INT NOT NULL DEFAULT 1,
		total_price NUMERIC(12, 2) NOT NULL DEFAULT 0.00,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);

	CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id);
	CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON order_items(product_id);
	`
	_, err := r.pool.Exec(ctx, schema)
	if err != nil {
		return fmt.Errorf("execute order schema migration: %w", err)
	}
	return nil
}

func (r *OrderRepository) Create(ctx context.Context, req *model.CreateOrderRequest) (*model.Order, error) {
	if len(req.Items) == 0 {
		return nil, ErrEmptyCart
	}

	userID := req.UserID
	if userID == 0 && req.ClientID != "" {
		if parsed, err := strconv.ParseInt(req.ClientID, 10, 64); err == nil {
			userID = parsed
		}
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	var calculatedTotal float64
	orderItems := make([]model.OrderItem, 0, len(req.Items))

	for _, item := range req.Items {
		prodID := item.ResolveProductID()
		if prodID == 0 {
			continue
		}
		qty := item.Quantity
		if qty <= 0 {
			qty = 1
		}

		var (
			currentStock int
			title        string
			price        float64
		)

		query := `SELECT title, price, stock FROM products WHERE id = $1 FOR UPDATE`
		err := tx.QueryRow(ctx, query, prodID).Scan(&title, &price, &currentStock)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, fmt.Errorf("%w (id=%d)", ErrProductNotFound, prodID)
			}
			return nil, fmt.Errorf("query product %d: %w", prodID, err)
		}

		if currentStock < qty {
			return nil, fmt.Errorf("%w (id=%d, requested=%d, available=%d)", ErrProductStock, prodID, qty, currentStock)
		}

		updateStockQuery := `UPDATE products SET stock = stock - $1, updated_at = NOW() WHERE id = $2`
		if _, err := tx.Exec(ctx, updateStockQuery, qty, prodID); err != nil {
			return nil, fmt.Errorf("update stock for product %d: %w", prodID, err)
		}

		itemTotal := price * float64(qty)
		calculatedTotal += itemTotal

		orderItems = append(orderItems, model.OrderItem{
			ProductID:    prodID,
			ProductTitle: title,
			UnitPrice:    price,
			Quantity:     qty,
			TotalPrice:   itemTotal,
		})
	}

	if len(orderItems) == 0 {
		return nil, ErrEmptyCart
	}

	var order model.Order
	insertOrderQuery := `
		INSERT INTO orders (user_id, user_email, status, total_price, created_at, updated_at)
		VALUES ($1, $2, 'confirmed', $3, NOW(), NOW())
		RETURNING id, user_id, user_email, status, total_price, created_at, updated_at
	`
	err = tx.QueryRow(ctx, insertOrderQuery, userID, req.UserEmail, calculatedTotal).Scan(
		&order.ID,
		&order.UserID,
		&order.UserEmail,
		&order.Status,
		&order.TotalPrice,
		&order.CreatedAt,
		&order.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("insert order: %w", err)
	}

	batch := &pgx.Batch{}
	insertItemQuery := `
		INSERT INTO order_items (order_id, product_id, product_title, unit_price, quantity, total_price, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, NOW())
	`
	for i := range orderItems {
		orderItems[i].OrderID = order.ID
		batch.Queue(insertItemQuery,
			order.ID,
			orderItems[i].ProductID,
			orderItems[i].ProductTitle,
			orderItems[i].UnitPrice,
			orderItems[i].Quantity,
			orderItems[i].TotalPrice,
		)
	}

	br := tx.SendBatch(ctx, batch)
	for i := 0; i < len(orderItems); i++ {
		if _, err := br.Exec(); err != nil {
			_ = br.Close()
			return nil, fmt.Errorf("insert order item at index %d: %w", i, err)
		}
	}
	if err := br.Close(); err != nil {
		return nil, fmt.Errorf("close batch: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit transaction: %w", err)
	}

	order.Items = orderItems
	return &order, nil
}

func (r *OrderRepository) GetByID(ctx context.Context, orderID int64) (*model.Order, error) {
	var order model.Order
	query := `
		SELECT id, user_id, user_email, status, total_price, created_at, updated_at
		FROM orders
		WHERE id = $1
	`
	err := r.pool.QueryRow(ctx, query, orderID).Scan(
		&order.ID,
		&order.UserID,
		&order.UserEmail,
		&order.Status,
		&order.TotalPrice,
		&order.CreatedAt,
		&order.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrOrderNotFound
		}
		return nil, fmt.Errorf("query order: %w", err)
	}

	itemsQuery := `
		SELECT id, order_id, product_id, product_title, unit_price, quantity, total_price
		FROM order_items
		WHERE order_id = $1
		ORDER BY id
	`
	rows, err := r.pool.Query(ctx, itemsQuery, orderID)
	if err != nil {
		return nil, fmt.Errorf("query order items: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var item model.OrderItem
		if err := rows.Scan(
			&item.ID,
			&item.OrderID,
			&item.ProductID,
			&item.ProductTitle,
			&item.UnitPrice,
			&item.Quantity,
			&item.TotalPrice,
		); err != nil {
			return nil, fmt.Errorf("scan order item: %w", err)
		}
		order.Items = append(order.Items, item)
	}

	return &order, nil
}

func (r *OrderRepository) List(ctx context.Context, userID int64, limit, offset int) ([]model.Order, int64, error) {
	if limit <= 0 {
		limit = 20
	}
	if offset < 0 {
		offset = 0
	}

	countQuery := `SELECT count(*) FROM orders WHERE ($1 = 0 OR user_id = $1)`
	var total int64
	if err := r.pool.QueryRow(ctx, countQuery, userID).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count orders: %w", err)
	}

	query := `
		SELECT id, user_id, user_email, status, total_price, created_at, updated_at
		FROM orders
		WHERE ($1 = 0 OR user_id = $1)
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3
	`
	rows, err := r.pool.Query(ctx, query, userID, limit, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("query orders: %w", err)
	}
	defer rows.Close()

	orders := make([]model.Order, 0)
	for rows.Next() {
		var o model.Order
		if err := rows.Scan(
			&o.ID,
			&o.UserID,
			&o.UserEmail,
			&o.Status,
			&o.TotalPrice,
			&o.CreatedAt,
			&o.UpdatedAt,
		); err != nil {
			return nil, 0, fmt.Errorf("scan order: %w", err)
		}
		orders = append(orders, o)
	}

	return orders, total, nil
}
