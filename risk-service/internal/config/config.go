package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	ServiceName      string
	Environment      string
	Port             int
	DatabaseURL      string
	KafkaBrokers     string
	ConsumerGroup    string
	InputTopic       string
	RiskAssessedTopic string
	RiskFlaggedTopic  string
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
