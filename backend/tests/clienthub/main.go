package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"delivery-fullcycle/internal/client"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	var (
		dbURL       string
		orderURL    string
		clientCount int
		ordersCount int
		timeout     time.Duration
	)

	defaultDB := os.Getenv("DATABASE_URL")
	if defaultDB == "" {
		defaultDB = "postgres://delivery:delivery123@localhost:5432/delivery?sslmode=disable"
	}

	defaultOrderURL := os.Getenv("ORDER_URL")
	if defaultOrderURL == "" {
		defaultOrderURL = "http://localhost:8080/api/orders"
	}

	flag.StringVar(&dbURL, "db-url", defaultDB, "")
	flag.StringVar(&orderURL, "order-url", defaultOrderURL, "")
	flag.IntVar(&clientCount, "clients", 10, "")
	flag.IntVar(&ordersCount, "orders-per-client", 3, "")
	flag.DurationVar(&timeout, "timeout", 60*time.Second, "")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	poolConfig, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		logger.Error("invalid postgres url", slog.String("error", err.Error()))
		os.Exit(1)
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		logger.Error("unable to connect to postgres", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		logger.Error("ping postgres failed", slog.String("error", err.Error()))
		os.Exit(1)
	}

	cfg := client.Config{
		ClientCount: clientCount,
		OrdersCount: ordersCount,
		OrderURL:    orderURL,
		DatabaseURL: dbURL,
	}

	hub := client.NewHub(cfg, pool, logger)
	if err := hub.Run(ctx); err != nil {
		logger.Error("clienthub failed", slog.String("error", err.Error()))
		os.Exit(1)
	}
}
