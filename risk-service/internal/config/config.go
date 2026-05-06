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
	RiskAssessedTopic string
	RiskFlaggedTopic  string
	Workers           int
	JobBuffer         int
	WriterBatchSize   int
	WriterBatchTimeout time.Duration
}

func Load() (Config, error) {
	port := 8004
	if raw := os.Getenv("SERVICE_PORT"); raw != "" {
		p, err := strconv.Atoi(raw)
		if err != nil {
			return Config{}, fmt.Errorf("invalid SERVICE_PORT: %w", err)
		}
		port = p
	}
	workers := getEnvInt("RISK_WORKERS", 8)
	if workers < 1 {
		return Config{}, fmt.Errorf("RISK_WORKERS must be >= 1, got %d", workers)
	}
	jobBuffer := getEnvInt("RISK_JOB_BUFFER", 256)
	if jobBuffer < 1 {
		return Config{}, fmt.Errorf("RISK_JOB_BUFFER must be >= 1, got %d", jobBuffer)
	}
	writerBatchSize := getEnvInt("RISK_WRITER_BATCH_SIZE", 100)
	if writerBatchSize < 1 {
		return Config{}, fmt.Errorf("RISK_WRITER_BATCH_SIZE must be >= 1, got %d", writerBatchSize)
	}
	writerBatchMs := getEnvInt("RISK_WRITER_BATCH_MS", 10)
	if writerBatchMs < 0 {
		return Config{}, fmt.Errorf("RISK_WRITER_BATCH_MS must be >= 0, got %d", writerBatchMs)
	}
	cfg := Config{
		ServiceName:       getEnv("SERVICE_NAME", "risk-service"),
		Environment:       getEnv("ENVIRONMENT", "development"),
		Port:              port,
		DatabaseURL:       os.Getenv("DATABASE_URL"),
		KafkaBrokers:      os.Getenv("KAFKA_BROKERS"),
		ConsumerGroup:     getEnv("KAFKA_CONSUMER_GROUP", "risk-service-group"),
		InputTopic:        getEnv("INPUT_TOPIC", "declaration.created"),
		RiskAssessedTopic: getEnv("RISK_ASSESSED_TOPIC", "risk.assessed"),
		RiskFlaggedTopic:  getEnv("RISK_FLAGGED_TOPIC", "risk.flagged"),
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
