package config

import (
	"flag"
	"os"
	"strings"
	"time"
)

type Config struct {
	Port             string
	DatabaseURL      string
	KafkaBrokers     string
	KafkaTopicOrders string
	ShutdownTimeout  time.Duration
}

func Load() *Config {
	var cfg Config

	defaultPort := os.Getenv("PORT")
	if defaultPort == "" {
		defaultPort = ":8082"
	}
	if !strings.HasPrefix(defaultPort, ":") {
		defaultPort = ":" + defaultPort
	}

	defaultDB := os.Getenv("DATABASE_URL")
	if defaultDB == "" {
		defaultDB = "postgres://delivery:delivery123@localhost:5432/delivery?sslmode=disable"
	}

	defaultKafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if defaultKafkaBrokers == "" {
		defaultKafkaBrokers = "kafka.kafka.svc.cluster.local:9092"
	}

	defaultKafkaTopic := os.Getenv("KAFKA_TOPIC_ORDERS")
	if defaultKafkaTopic == "" {
		defaultKafkaTopic = "orders.events"
	}

	flag.StringVar(&cfg.Port, "port", defaultPort, "")
	flag.StringVar(&cfg.DatabaseURL, "db-url", defaultDB, "")
	flag.StringVar(&cfg.KafkaBrokers, "kafka-brokers", defaultKafkaBrokers, "")
	flag.StringVar(&cfg.KafkaTopicOrders, "kafka-topic-orders", defaultKafkaTopic, "")
	flag.DurationVar(&cfg.ShutdownTimeout, "shutdown-timeout", 10*time.Second, "")
	flag.Parse()

	return &cfg
}
