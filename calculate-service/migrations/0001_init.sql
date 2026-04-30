CREATE SCHEMA IF NOT EXISTS calculation;

CREATE TABLE IF NOT EXISTS calculation.state (
  correlation_id UUID PRIMARY KEY,
  has_validation BOOLEAN NOT NULL DEFAULT FALSE,
  has_risk BOOLEAN NOT NULL DEFAULT FALSE,
  calculated BOOLEAN NOT NULL DEFAULT FALSE,
  updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS calculation.results (
  id UUID PRIMARY KEY,
  correlation_id UUID NOT NULL UNIQUE,
  payload JSONB NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS calculation.outbox (
  id BIGSERIAL PRIMARY KEY,
  aggregate_id UUID NOT NULL,
  aggregate_type VARCHAR(255) NOT NULL,
  event_type VARCHAR(255) NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  processed BOOLEAN DEFAULT FALSE,
  processed_at TIMESTAMP
);
