package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	ServiceName    string
	Environment    string
	Port           int
	DatabaseURL    string
	KafkaBrokers   string
	ConsumerGroup  string
	InputTopic     string
	OutputTopic    string
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
	cfg := Config{
		ServiceName:   getEnv("SERVICE_NAME", "audit-service"),
		Environment:   getEnv("ENVIRONMENT", "development"),
		Port:          port,
		DatabaseURL:   os.Getenv("DATABASE_URL"),
		KafkaBrokers:  os.Getenv("KAFKA_BROKERS"),
		ConsumerGroup: getEnv("KAFKA_CONSUMER_GROUP", "audit-service-group"),
		InputTopic:    getEnv("INPUT_TOPIC", "declaration.created"),
		OutputTopic:   getEnv("AUDIT_LOGGED_TOPIC", "audit.logged"),
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
