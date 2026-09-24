package model

import "time"

type OrderItem struct {
	ID           int64   `json:"id,omitempty"`
	OrderID      int64   `json:"order_id,omitempty"`
	ProductID    int64   `json:"product_id"`
	ProductTitle string  `json:"product_title"`
	UnitPrice    float64 `json:"unit_price"`
	Quantity     int     `json:"quantity"`
	TotalPrice   float64 `json:"total_price"`
}

type Order struct {
	ID         int64       `json:"id"`
	UserID     int64       `json:"user_id"`
	UserEmail  string      `json:"user_email,omitempty"`
	Status     string      `json:"status"`
	TotalPrice float64     `json:"total_price"`
	Items      []OrderItem `json:"items"`
	CreatedAt  time.Time   `json:"created_at"`
	UpdatedAt  time.Time   `json:"updated_at"`
}

type CartItemInput struct {
	ProductID int64 `json:"product_id"`
	Product   struct {
		ID int64 `json:"id"`
	} `json:"product"`
	Quantity int `json:"quantity"`
}

func (c *CartItemInput) ResolveProductID() int64 {
	if c.ProductID > 0 {
		return c.ProductID
	}
	return c.Product.ID
}

type CreateOrderRequest struct {
	UserID     int64           `json:"user_id"`
	ClientID   string          `json:"client_id,omitempty"`
	UserEmail  string          `json:"user_email"`
	UserName   string          `json:"user_name,omitempty"`
	Items      []CartItemInput `json:"items"`
	TotalPrice float64         `json:"total_price,omitempty"`
}
