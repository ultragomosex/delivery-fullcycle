package client

import (
	"math/rand"
	"delivery-fullcycle/internal/catalog"
	"delivery-fullcycle/internal/model"
	"fmt"
)

type Client struct {
	ID   int
	Name string
}

func NewClient(id int, name string) *Client {
	return &Client{
		ID:   id,
		Name: name,
	}
}

func (c *Client) MakeCart() *model.Cart {
	products := catalog.GetAll()

	used := make(map[int]bool)
	var items []model.CartItem

	for len(items) < 3 {
		p := products[rand.Intn(len(products))]
		if !used[p.ID] {
			used[p.ID] = true
			items = append(items, model.CartItem{
				Product:  p,
				Quantity: rand.Intn(5) + 1,
			})
		}
	}

	return &model.Cart{
		ClientID: fmt.Sprintf("%d", c.ID),
		Items:    items,
	}
}
