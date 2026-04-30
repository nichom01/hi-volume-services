package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	ServiceName            string
	Environment            string
	Port                   int
	DatabaseURL            string
	KafkaBrokers           string
	KafkaConsumerGroup     string
	InboundTopic           string
	DeclarationCreatedTopic string
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

	cfg := Config{
		ServiceName:             getEnv("SERVICE_NAME", "declaration-service"),
		Environment:             getEnv("ENVIRONMENT", "development"),
		Port:                    port,
		DatabaseURL:             os.Getenv("DATABASE_URL"),
		KafkaBrokers:            os.Getenv("KAFKA_BROKERS"),
		KafkaConsumerGroup:      getEnv("KAFKA_CONSUMER_GROUP", "declaration-service-group"),
		InboundTopic:            getEnv("INBOUND_TOPIC", "inbound"),
		DeclarationCreatedTopic: getEnv("DECLARATION_CREATED_TOPIC", "declaration.created"),
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
