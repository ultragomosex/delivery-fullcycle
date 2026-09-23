package client

import (
	"delivery-fullcycle/internal/model"
	"math/rand"
)

type Client struct {
	ID    int64
	Name  string
	Email string
}

func NewClient(id int64, name, email string) *Client {
	return &Client{
		ID:    id,
		Name:  name,
		Email: email,
	}
}

func (c *Client) MakeCart(availableProducts []model.Product, maxItems int) *model.Cart {
	if len(availableProducts) == 0 {
		return &model.Cart{
			UserID:    c.ID,
			UserName:  c.Name,
			UserEmail: c.Email,
			Items:     nil,
		}
	}

	targetCount := rand.Intn(maxItems) + 1
	if targetCount > len(availableProducts) {
		targetCount = len(availableProducts)
	}

	perm := rand.Perm(len(availableProducts))
	items := make([]model.CartItem, 0, targetCount)
	var totalPrice float64

	for i := 0; i < targetCount; i++ {
		p := availableProducts[perm[i]]
		maxQty := 3
		if p.Stock < maxQty && p.Stock > 0 {
			maxQty = p.Stock
		}
		qty := rand.Intn(maxQty) + 1

		items = append(items, model.CartItem{
			Product:  p,
			Quantity: qty,
		})
		totalPrice += p.Price * float64(qty)
	}

	return &model.Cart{
		UserID:     c.ID,
		UserName:   c.Name,
		UserEmail:  c.Email,
		Items:      items,
		TotalPrice: totalPrice,
	}
}
