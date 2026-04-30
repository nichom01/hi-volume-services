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
	ServiceName   string
	KafkaBrokers  string
	ConsumerGroup string
	InputTopic    string
	OutputTopic   string
}

type Processor struct {
	db     *sql.DB
	reader *kafka.Reader
	writer *kafka.Writer
	cfg    Config
}

func New(db *sql.DB, cfg Config) *Processor {
	brokers := splitCSV(cfg.KafkaBrokers)
	return &Processor{
		db: db,
		cfg: cfg,
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers, GroupID: cfg.ConsumerGroup, Topic: cfg.InputTopic, MaxWait: 2 * time.Second,
		}),
		writer: &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: cfg.OutputTopic, AllowAutoTopicCreation: true, RequiredAcks: kafka.RequireOne},
	}
}

func (p *Processor) Run(ctx context.Context) error {
	for {
		msg, err := p.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if err := p.handleMessage(ctx, msg); err != nil {
			log.Printf("{\"service\":\"%s\",\"level\":\"error\",\"error\":%q}", p.cfg.ServiceName, err.Error())
			continue
		}
		if err := p.reader.CommitMessages(ctx, msg); err != nil {
			return err
		}
	}
}

func (p *Processor) Close() error {
	_ = p.reader.Close()
	return p.writer.Close()
}

func (p *Processor) handleMessage(ctx context.Context, msg kafka.Message) error {
	sourceEventID := extractID(msg.Value)
	if sourceEventID == "" {
		sourceEventID = uuid.NewString()
	}
	auditID := uuid.NewString()
	now := time.Now().UTC()
	event := map[string]any{
		"id":            uuid.NewString(),
		"timestamp":     now.Format(time.RFC3339Nano),
		"aggregateId":   auditID,
		"aggregateType": "audit",
		"eventType":     "audit.logged",
		"version":       1,
		"payload": map[string]any{
			"auditId": auditID,
			"action":  "declaration.created.received",
		},
		"metadata": map[string]any{
			"correlationId": sourceEventID,
			"causationId":   sourceEventID,
			"traceId":       sourceEventID,
			"source":        p.cfg.ServiceName,
		},
	}
	encoded, _ := json.Marshal(event)

	tx, err := p.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	res, err := tx.ExecContext(ctx, `
		INSERT INTO audit.logs (id, source_event_id, payload)
		VALUES ($1,$2,$3)
		ON CONFLICT (source_event_id) DO NOTHING
	`, auditID, sourceEventID, msg.Value)
	if err != nil {
		return err
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return nil
	}
	var outboxID int64
	if err := tx.QueryRowContext(ctx, `
		INSERT INTO audit.outbox (aggregate_id, aggregate_type, event_type, payload)
		VALUES ($1,$2,$3,$4) RETURNING id
	`, auditID, "audit", "audit.logged", encoded).Scan(&outboxID); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if err := p.writer.WriteMessages(ctx, kafka.Message{Key: []byte(auditID), Value: encoded, Time: now}); err != nil {
		return fmt.Errorf("publish audit.logged: %w", err)
	}
	_, _ = p.db.ExecContext(ctx, `UPDATE audit.outbox SET processed=TRUE, processed_at=NOW() WHERE id=$1`, outboxID)
	return nil
}

func splitCSV(s string) []string {
	out := []string{}
	for _, p := range strings.Split(s, ",") {
		if v := strings.TrimSpace(p); v != "" {
			out = append(out, v)
		}
	}
	return out
}

func extractID(raw []byte) string {
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err == nil {
		if id, ok := payload["id"].(string); ok {
			return id
		}
	}
	return ""
}
