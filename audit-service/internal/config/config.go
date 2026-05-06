package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	ServiceName       string
	Environment       string
	Port              int
	DatabaseURL       string
	KafkaBrokers      string
	ConsumerGroup     string
	InputTopic        string
	OutputTopic       string
	Workers           int
	JobBuffer         int
	WriterBatchSize   int
	WriterBatchTimeout time.Duration
}

func Load() (Config, error) {
	port := 8003
	if raw := os.Getenv("SERVICE_PORT"); raw != "" {
		p, err := strconv.Atoi(raw)
		if err != nil {
			return Config{}, fmt.Errorf("invalid SERVICE_PORT: %w", err)
		}
		port = p
	}
	workers := getEnvInt("AUDIT_WORKERS", 8)
	if workers < 1 {
		return Config{}, fmt.Errorf("AUDIT_WORKERS must be >= 1, got %d", workers)
	}
	jobBuffer := getEnvInt("AUDIT_JOB_BUFFER", 256)
	if jobBuffer < 1 {
		return Config{}, fmt.Errorf("AUDIT_JOB_BUFFER must be >= 1, got %d", jobBuffer)
	}
	writerBatchSize := getEnvInt("AUDIT_WRITER_BATCH_SIZE", 100)
	if writerBatchSize < 1 {
		return Config{}, fmt.Errorf("AUDIT_WRITER_BATCH_SIZE must be >= 1, got %d", writerBatchSize)
	}
	writerBatchMs := getEnvInt("AUDIT_WRITER_BATCH_MS", 10)
	if writerBatchMs < 0 {
		return Config{}, fmt.Errorf("AUDIT_WRITER_BATCH_MS must be >= 0, got %d", writerBatchMs)
	}
	cfg := Config{
		ServiceName:   getEnv("SERVICE_NAME", "audit-service"),
		Environment:   getEnv("ENVIRONMENT", "development"),
		Port:          port,
		DatabaseURL:   os.Getenv("DATABASE_URL"),
		KafkaBrokers:  os.Getenv("KAFKA_BROKERS"),
		ConsumerGroup: getEnv("KAFKA_CONSUMER_GROUP", "audit-service-group"),
		InputTopic:    getEnv("INPUT_TOPIC", "declaration.created"),
		OutputTopic:   getEnv("AUDIT_LOGGED_TOPIC", "audit.logged"),
		Workers:           workers,
		JobBuffer:         jobBuffer,
		WriterBatchSize:   writerBatchSize,
		WriterBatchTimeout: time.Duration(writerBatchMs) * time.Millisecond,
	}
	if cfg.DatabaseURL == "" || cfg.KafkaBrokers == "" {
		return Config{}, fmt.Errorf("DATABASE_URL and KAFKA_BROKERS are required")
	}
	return cfg, nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return v
}
