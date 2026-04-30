package processor

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

type Config struct {
	ServiceName           string
	KafkaBrokers          string
	KafkaConsumerGroup    string
	InputTopic            string
	ValidationPassedTopic string
	ValidationFailedTopic string
}

type Processor struct {
	db           *sql.DB
	reader       *kafka.Reader
	passedWriter *kafka.Writer
	failedWriter *kafka.Writer
	cfg          Config
}

func New(db *sql.DB, cfg Config) *Processor {
	brokers := splitCSV(cfg.KafkaBrokers)
	return &Processor{
		db: db,
		cfg: cfg,
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers, GroupID: cfg.KafkaConsumerGroup, Topic: cfg.InputTopic, MaxWait: 2 * time.Second,
		}),
		passedWriter: &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: cfg.ValidationPassedTopic, AllowAutoTopicCreation: true, RequiredAcks: kafka.RequireOne},
		failedWriter: &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: cfg.ValidationFailedTopic, AllowAutoTopicCreation: true, RequiredAcks: kafka.RequireOne},
	}
}

func (p *Processor) Run(ctx context.Context) error {
	log.Printf("{\"service\":\"%s\",\"msg\":\"event processor started\",\"input\":\"%s\"}", p.cfg.ServiceName, p.cfg.InputTopic)
	for {
		msg, err := p.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("fetch input message: %w", err)
		}
		if err := p.handleMessage(ctx, msg); err != nil {
			log.Printf("{\"service\":\"%s\",\"level\":\"error\",\"msg\":\"processing failed\",\"error\":%q}", p.cfg.ServiceName, err.Error())
			continue
		}
		if err := p.reader.CommitMessages(ctx, msg); err != nil {
			return fmt.Errorf("commit input message: %w", err)
		}
	}
}

func (p *Processor) Close() error {
	var err error
	_ = p.reader.Close()
	_ = p.passedWriter.Close()
	err = p.failedWriter.Close()
	return err
}

func (p *Processor) handleMessage(ctx context.Context, msg kafka.Message) error {
	sourceEventID := extractSourceEventID(msg)
	if sourceEventID == "" {
		sourceEventID = uuid.NewString()
	}

	status := "passed"
	reason := ""
	if !passesBasicChecks(msg.Value) {
		status = "failed"
		reason = "invalid declaration payload"
	}

	validationID := uuid.NewString()
	now := time.Now().UTC()
	eventType := "validation.passed"
	if status == "failed" {
		eventType = "validation.failed"
	}

	event := map[string]any{
		"id":            uuid.NewString(),
		"timestamp":     now.Format(time.RFC3339Nano),
		"aggregateId":   validationID,
		"aggregateType": "validation",
		"eventType":     eventType,
		"version":       1,
		"payload": map[string]any{
			"validationId": validationID,
			"status":       status,
			"reason":       reason,
		},
		"metadata": map[string]any{
			"correlationId": sourceEventID,
			"causationId":   sourceEventID,
			"traceId":       sourceEventID,
			"source":        p.cfg.ServiceName,
		},
	}
	eventPayload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal validation event: %w", err)
	}

	tx, err := p.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	res, err := tx.ExecContext(ctx, `
		INSERT INTO validation.validations (id, source_event_id, status, reason, payload)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (source_event_id) DO NOTHING
	`, validationID, sourceEventID, status, reason, msg.Value)
	if err != nil {
		return fmt.Errorf("insert validation: %w", err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return nil
	}

	var outboxID int64
	if err := tx.QueryRowContext(ctx, `
		INSERT INTO validation.outbox (aggregate_id, aggregate_type, event_type, payload)
		VALUES ($1, $2, $3, $4)
		RETURNING id
	`, validationID, "validation", eventType, eventPayload).Scan(&outboxID); err != nil {
		return fmt.Errorf("insert outbox: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}

	writer := p.passedWriter
	if status == "failed" {
		writer = p.failedWriter
	}
	if err := writer.WriteMessages(ctx, kafka.Message{Key: []byte(validationID), Value: eventPayload, Time: now}); err != nil {
		return fmt.Errorf("publish validation event: %w", err)
	}
	if _, err := p.db.ExecContext(ctx, `UPDATE validation.outbox SET processed = TRUE, processed_at = NOW() WHERE id = $1`, outboxID); err != nil {
		return fmt.Errorf("mark outbox processed: %w", err)
	}
	log.Printf("{\"service\":\"%s\",\"msg\":\"validation processed\",\"status\":\"%s\",\"sourceEventId\":\"%s\"}", p.cfg.ServiceName, status, sourceEventID)
	return nil
}

func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if v := strings.TrimSpace(p); v != "" {
			out = append(out, v)
		}
	}
	return out
}

func extractSourceEventID(msg kafka.Message) string {
	var payload map[string]any
	if err := json.Unmarshal(msg.Value, &payload); err == nil {
		if rawID, ok := payload["id"].(string); ok && rawID != "" {
			return rawID
		}
	}
	return ""
}

func passesBasicChecks(raw []byte) bool {
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return false
	}
	return payload["eventType"] == "declaration.created"
}
