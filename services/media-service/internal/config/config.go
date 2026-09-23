package config

import (
	"flag"
	"os"
	"strings"
	"time"
)

type Config struct {
	Port            string
	DatabaseURL     string
	S3Endpoint      string
	S3AccessKey     string
	S3SecretKey     string
	S3Bucket        string
	S3SSL           bool
	ShutdownTimeout time.Duration
}

func Load() *Config {
	var cfg Config

	defaultPort := os.Getenv("PORT")
	if defaultPort == "" {
		defaultPort = ":8081"
	}
	if !strings.HasPrefix(defaultPort, ":") {
		defaultPort = ":" + defaultPort
	}

	defaultDB := os.Getenv("DATABASE_URL")
	if defaultDB == "" {
		defaultDB = "postgres://delivery:delivery123@localhost:5432/delivery?sslmode=disable"
	}

	defaultS3Endpoint := os.Getenv("S3_ENDPOINT")
	if defaultS3Endpoint == "" {
		defaultS3Endpoint = "127.0.0.1:9000"
	}

	defaultAccessKey := os.Getenv("S3_ACCESS_KEY")
	if defaultAccessKey == "" {
		defaultAccessKey = "ea5fc5464891940eeebaceb2"
	}

	defaultSecretKey := os.Getenv("S3_SECRET_KEY")
	if defaultSecretKey == "" {
		defaultSecretKey = "4b2d3855a3832263df61c340b1c0526cf1dde4c4b702c8a1"
	}

	defaultBucket := os.Getenv("S3_BUCKET")
	if defaultBucket == "" {
		defaultBucket = "delivery-media"
	}

	flag.StringVar(&cfg.Port, "port", defaultPort, "")
	flag.StringVar(&cfg.DatabaseURL, "db-url", defaultDB, "")
	flag.StringVar(&cfg.S3Endpoint, "s3-endpoint", defaultS3Endpoint, "")
	flag.StringVar(&cfg.S3AccessKey, "s3-access-key", defaultAccessKey, "")
	flag.StringVar(&cfg.S3SecretKey, "s3-secret-key", defaultSecretKey, "")
	flag.StringVar(&cfg.S3Bucket, "s3-bucket", defaultBucket, "")
	flag.BoolVar(&cfg.S3SSL, "s3-ssl", false, "")
	flag.DurationVar(&cfg.ShutdownTimeout, "shutdown-timeout", 10*time.Second, "")
	flag.Parse()

	return &cfg
}
