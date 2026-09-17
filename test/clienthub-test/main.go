package main

import (
	"encoding/json"
	"log"
	"net/http"

	"delivery-fullcycle/internal/model"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

func main() {
	r := chi.NewRouter()
	r.Use(middleware.Logger)

	r.Post("/api/orders", func(w http.ResponseWriter, r *http.Request) {
		var cart model.Cart
		if err := json.NewDecoder(r.Body).Decode(&cart); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		log.Printf("Получен заказ от %s: %d позиций", cart.ClientID, len(cart.Items))
		w.WriteHeader(http.StatusCreated)
	})

	log.Println("Сервер запущен на :8080")
	http.ListenAndServe(":8080", r)
}
