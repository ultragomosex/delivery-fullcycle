package client

import (
	"bytes"
	"delivery-fullcycle/internal/model"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

type Config struct {
	ClientCount int
	OrdersCount int
	OrderURL    string
}

type Hub struct {
	clients []*Client
	config  Config
}

func NewHub(config Config) *Hub {
	clients := make([]*Client, 0, config.ClientCount)
	for i := 0; i < config.ClientCount; i++ {
		clients = append(clients, NewClient(
			i+1,
			fmt.Sprintf("Client %d", i+1),
		))
	}
	return &Hub{clients: clients, config: config}
}

func (h *Hub) Run() {
	httpClient := &http.Client{Timeout: 5 * time.Second}

	for _, c := range h.clients {
		for j := 0; j < h.config.OrdersCount; j++ {
			cart := c.MakeCart()
			h.sendOrder(httpClient, cart)
		}
	}
}

func (h *Hub) sendOrder(httpClient *http.Client, cart *model.Cart) error {
	body, err := json.Marshal(cart)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	resp, err := httpClient.Post(h.config.OrderURL, "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("post: %w", err)
	}
	defer resp.Body.Close()

	log.Printf("[%s] заказ отправлен: %d позиций — статус %d", cart.ClientID, len(cart.Items), resp.StatusCode)
	return nil
}
