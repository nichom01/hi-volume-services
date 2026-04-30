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
	ServiceName             string
	KafkaBrokers            string
	KafkaConsumerGroup      string
	InboundTopic            string
	DeclarationCreatedTopic string
}

type Processor struct {
	db     *sql.DB
	reader *kafka.Reader
	writer *kafka.Writer
	cfg    Config
}

type Event struct {
	ID            string         `json:"id"`
	Timestamp     string         `json:"timestamp"`
	AggregateID   string         `json:"aggregateId"`
	AggregateType string         `json:"aggregateType"`
	EventType     string         `json:"eventType"`
	Version       int            `json:"version"`
	Payload       map[string]any `json:"payload"`
	Metadata      Metadata       `json:"metadata"`
}

type Metadata struct {
	CorrelationID string `json:"correlationId"`
	CausationID   string `json:"causationId"`
	TraceID       string `json:"traceId"`
	Source        string `json:"source"`
}

func New(db *sql.DB, cfg Config) *Processor {
	brokers := splitCSV(cfg.KafkaBrokers)
	return &Processor{
		db: db,
		cfg: cfg,
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers,
			GroupID: cfg.KafkaConsumerGroup,
			Topic:   cfg.InboundTopic,
			MaxWait: 2 * time.Second,
		}),
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Topic:                  cfg.DeclarationCreatedTopic,
			AllowAutoTopicCreation: true,
			RequiredAcks:           kafka.RequireOne,
		},
	}
}

func (p *Processor) Run(ctx context.Context) error {
	log.Printf("event processor started: inbound=%s outbound=%s", p.cfg.InboundTopic, p.cfg.DeclarationCreatedTopic)

	for {
		msg, err := p.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("fetch inbound message: %w", err)
		}

		if err := p.handleMessage(ctx, msg); err != nil {
			log.Printf("failed processing message key=%s partition=%d offset=%d: %v", string(msg.Key), msg.Partition, msg.Offset, err)
			continue
		}

		if err := p.reader.CommitMessages(ctx, msg); err != nil {
			return fmt.Errorf("commit message offset %d: %w", msg.Offset, err)
		}
	}
}

func (p *Processor) Close() error {
	var err1, err2 error
	err1 = p.reader.Close()
	err2 = p.writer.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

func (p *Processor) handleMessage(ctx context.Context, msg kafka.Message) error {
	sourceEventID := extractSourceEventID(msg)
	if sourceEventID == "" {
		sourceEventID = uuid.NewString()
	}

	declarationID := uuid.NewString()
	now := time.Now().UTC()

	event := Event{
		ID:            uuid.NewString(),
		Timestamp:     now.Format(time.RFC3339Nano),
		AggregateID:   declarationID,
		AggregateType: "declaration",
		EventType:     "declaration.created",
		Version:       1,
		Payload: map[string]any{
			"declarationId": declarationID,
			"inboundTopic":  msg.Topic,
			"inboundOffset": msg.Offset,
			"inboundPayload": json.RawMessage(msg.Value),
		},
		Metadata: Metadata{
			CorrelationID: sourceEventID,
			CausationID:   sourceEventID,
			TraceID:       sourceEventID,
			Source:        p.cfg.ServiceName,
		},
	}

	eventPayload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal declaration event: %w", err)
	}

	tx, err := p.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	res, err := tx.ExecContext(ctx, `
		INSERT INTO declaration.declarations (id, source_event_id, payload)
		VALUES ($1, $2, $3)
		ON CONFLICT (source_event_id) DO NOTHING
	`, declarationID, sourceEventID, msg.Value)
	if err != nil {
		return fmt.Errorf("insert declaration: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected: %w", err)
	}
	if rows == 0 {
		// Idempotent replay: record was already handled previously.
		return nil
	}

	var outboxID int64
	if err := tx.QueryRowContext(ctx, `
		INSERT INTO declaration.outbox (aggregate_id, aggregate_type, event_type, payload)
		VALUES ($1, $2, $3, $4)
		RETURNING id
	`, declarationID, "declaration", "declaration.created", eventPayload).Scan(&outboxID); err != nil {
		return fmt.Errorf("insert outbox: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}

	if err := p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(declarationID),
		Value: eventPayload,
		Time:  now,
	}); err != nil {
		return fmt.Errorf("publish declaration.created: %w", err)
	}

	if _, err := p.db.ExecContext(ctx, `
		UPDATE declaration.outbox
		SET processed = TRUE, processed_at = NOW()
		WHERE id = $1
	`, outboxID); err != nil {
		return fmt.Errorf("mark outbox processed: %w", err)
	}

	log.Printf("declaration created id=%s source_event_id=%s", declarationID, sourceEventID)
	return nil
}

func splitCSV(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func extractSourceEventID(msg kafka.Message) string {
	for _, h := range msg.Headers {
		if strings.EqualFold(h.Key, "event-id") || strings.EqualFold(h.Key, "id") {
			return string(h.Value)
		}
	}
	var payload map[string]any
	if err := json.Unmarshal(msg.Value, &payload); err == nil {
		if rawID, ok := payload["id"].(string); ok && rawID != "" {
			return rawID
		}
	}
	return ""
}
