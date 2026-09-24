package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"delivery-fullcycle/services/order-service/internal/model"
	"delivery-fullcycle/services/order-service/internal/repository"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type OrderHandler struct {
	repo   *repository.OrderRepository
	dbPool *pgxpool.Pool
}

func NewOrderHandler(repo *repository.OrderRepository, dbPool *pgxpool.Pool) *OrderHandler {
	return &OrderHandler{
		repo:   repo,
		dbPool: dbPool,
	}
}

func (h *OrderHandler) Health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	err := h.dbPool.Ping(ctx)
	status := "ok"
	code := http.StatusOK
	if err != nil {
		status = "degraded"
		code = http.StatusServiceUnavailable
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":   status,
		"database": err == nil,
		"time":     time.Now().UTC().Format(time.RFC3339),
	})
}

func (h *OrderHandler) Create(w http.ResponseWriter, r *http.Request) {
	var req model.CreateOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request payload: "+err.Error(), http.StatusBadRequest)
		return
	}

	order, err := h.repo.Create(r.Context(), &req)
	if err != nil {
		if errors.Is(err, repository.ErrEmptyCart) || errors.Is(err, repository.ErrProductNotFound) || errors.Is(err, repository.ErrProductStock) {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		http.Error(w, "failed to create order: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(order)
}

func (h *OrderHandler) Get(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid order id", http.StatusBadRequest)
		return
	}

	order, err := h.repo.GetByID(r.Context(), id)
	if err != nil {
		if errors.Is(err, repository.ErrOrderNotFound) {
			http.Error(w, "order not found", http.StatusNotFound)
			return
		}
		http.Error(w, "failed to query order: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(order)
}

func (h *OrderHandler) List(w http.ResponseWriter, r *http.Request) {
	var userID int64
	if uStr := r.URL.Query().Get("user_id"); uStr != "" {
		userID, _ = strconv.ParseInt(uStr, 10, 64)
	}

	limit := 20
	if lStr := r.URL.Query().Get("limit"); lStr != "" {
		if parsed, err := strconv.Atoi(lStr); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	offset := 0
	if oStr := r.URL.Query().Get("offset"); oStr != "" {
		if parsed, err := strconv.Atoi(oStr); err == nil && parsed >= 0 {
			offset = parsed
		}
	}

	orders, total, err := h.repo.List(r.Context(), userID, limit, offset)
	if err != nil {
		http.Error(w, "failed to list orders: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"orders": orders,
		"total":  total,
		"limit":  limit,
		"offset": offset,
	})
}
