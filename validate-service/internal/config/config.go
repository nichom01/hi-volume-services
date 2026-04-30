package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	ServiceName          string
	Environment          string
	Port                 int
	DatabaseURL          string
	KafkaBrokers         string
	KafkaConsumerGroup   string
	InputTopic           string
	ValidationPassedTopic string
	ValidationFailedTopic string
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
