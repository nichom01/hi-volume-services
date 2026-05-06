package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	ServiceName             string
	Environment             string
	Port                    int
	DatabaseURL             string
	KafkaBrokers            string
	KafkaConsumerGroup      string
	InboundTopic            string
	DeclarationCreatedTopic string
	Workers                 int
	JobBuffer               int
	WriterBatchSize         int
	WriterBatchTimeout      time.Duration
}

func Load() (Config, error) {
	port := 8001
	if raw := os.Getenv("SERVICE_PORT"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			return Config{}, fmt.Errorf("invalid SERVICE_PORT %q: %w", raw, err)
		}
		port = parsed
	}

	workers := getEnvInt("DECLARATION_WORKERS", 8)
	if workers < 1 {
		return Config{}, fmt.Errorf("DECLARATION_WORKERS must be >= 1, got %d", workers)
	}
	jobBuffer := getEnvInt("DECLARATION_JOB_BUFFER", 256)
	if jobBuffer < 1 {
		return Config{}, fmt.Errorf("DECLARATION_JOB_BUFFER must be >= 1, got %d", jobBuffer)
	}
	writerBatchSize := getEnvInt("DECLARATION_WRITER_BATCH_SIZE", 100)
	if writerBatchSize < 1 {
		return Config{}, fmt.Errorf("DECLARATION_WRITER_BATCH_SIZE must be >= 1, got %d", writerBatchSize)
	}
	writerBatchMs := getEnvInt("DECLARATION_WRITER_BATCH_MS", 10)
	if writerBatchMs < 0 {
		return Config{}, fmt.Errorf("DECLARATION_WRITER_BATCH_MS must be >= 0, got %d", writerBatchMs)
	}

	cfg := Config{
		ServiceName:             getEnv("SERVICE_NAME", "declaration-service"),
		Environment:             getEnv("ENVIRONMENT", "development"),
		Port:                    port,
		DatabaseURL:             os.Getenv("DATABASE_URL"),
		KafkaBrokers:            os.Getenv("KAFKA_BROKERS"),
		KafkaConsumerGroup:      getEnv("KAFKA_CONSUMER_GROUP", "declaration-service-group"),
		InboundTopic:            getEnv("INBOUND_TOPIC", "inbound"),
		DeclarationCreatedTopic: getEnv("DECLARATION_CREATED_TOPIC", "declaration.created"),
		Workers:                 workers,
		JobBuffer:               jobBuffer,
		WriterBatchSize:         writerBatchSize,
		WriterBatchTimeout:      time.Duration(writerBatchMs) * time.Millisecond,
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
