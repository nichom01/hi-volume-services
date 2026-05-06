package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	ServiceName           string
	Environment           string
	Port                  int
	DatabaseURL           string
	KafkaBrokers          string
	KafkaConsumerGroup    string
	InputTopic            string
	ValidationPassedTopic string
	ValidationFailedTopic string
	Workers               int
	JobBuffer             int
	WriterBatchSize       int
	WriterBatchTimeout    time.Duration
}

func Load() (Config, error) {
	port := 8002
	if raw := os.Getenv("SERVICE_PORT"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			return Config{}, fmt.Errorf("invalid SERVICE_PORT %q: %w", raw, err)
		}
		port = parsed
	}

	workers := getEnvInt("VALIDATE_WORKERS", 8)
	if workers < 1 {
		return Config{}, fmt.Errorf("VALIDATE_WORKERS must be >= 1, got %d", workers)
	}
	jobBuffer := getEnvInt("VALIDATE_JOB_BUFFER", 256)
	if jobBuffer < 1 {
		return Config{}, fmt.Errorf("VALIDATE_JOB_BUFFER must be >= 1, got %d", jobBuffer)
	}
	writerBatchSize := getEnvInt("VALIDATE_WRITER_BATCH_SIZE", 100)
	if writerBatchSize < 1 {
		return Config{}, fmt.Errorf("VALIDATE_WRITER_BATCH_SIZE must be >= 1, got %d", writerBatchSize)
	}
	writerBatchMs := getEnvInt("VALIDATE_WRITER_BATCH_MS", 10)
	if writerBatchMs < 0 {
		return Config{}, fmt.Errorf("VALIDATE_WRITER_BATCH_MS must be >= 0, got %d", writerBatchMs)
	}

	cfg := Config{
		ServiceName:           getEnv("SERVICE_NAME", "validate-service"),
		Environment:           getEnv("ENVIRONMENT", "development"),
		Port:                  port,
		DatabaseURL:           os.Getenv("DATABASE_URL"),
		KafkaBrokers:          os.Getenv("KAFKA_BROKERS"),
		KafkaConsumerGroup:    getEnv("KAFKA_CONSUMER_GROUP", "validate-service-group"),
		InputTopic:            getEnv("INPUT_TOPIC", "declaration.created"),
		ValidationPassedTopic: getEnv("VALIDATION_PASSED_TOPIC", "validation.passed"),
		ValidationFailedTopic: getEnv("VALIDATION_FAILED_TOPIC", "validation.failed"),
		Workers:               workers,
		JobBuffer:             jobBuffer,
		WriterBatchSize:       writerBatchSize,
		WriterBatchTimeout:    time.Duration(writerBatchMs) * time.Millisecond,
	}
	if cfg.DatabaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL is required")
	}
	if cfg.KafkaBrokers == "" {
		return Config{}, fmt.Errorf("KAFKA_BROKERS is required")
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
