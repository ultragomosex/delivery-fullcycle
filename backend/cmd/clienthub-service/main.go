package main

import (
	"delivery-fullcycle/internal/client"
)

func main() {
	cfg := client.Config{
		ClientCount: 10,
		OrdersCount: 5,
		OrderURL:    "http://localhost:8080/api/orders",
	}

	hub := client.NewHub(cfg)
	hub.Run()
}
