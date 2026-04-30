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
	ServiceName      string
	KafkaBrokers     string
	ConsumerGroup    string
	InputTopic       string
	RiskAssessedTopic string
	RiskFlaggedTopic  string
}

type Processor struct {
	db             *sql.DB
	reader         *kafka.Reader
	assessedWriter *kafka.Writer
	flaggedWriter  *kafka.Writer
	cfg            Config
}

func New(db *sql.DB, cfg Config) *Processor {
	brokers := splitCSV(cfg.KafkaBrokers)
	return &Processor{
		db: db,
		cfg: cfg,
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers, GroupID: cfg.ConsumerGroup, Topic: cfg.InputTopic, MaxWait: 2 * time.Second,
		}),
		assessedWriter: &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: cfg.RiskAssessedTopic, AllowAutoTopicCreation: true, RequiredAcks: kafka.RequireOne},
		flaggedWriter:  &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: cfg.RiskFlaggedTopic, AllowAutoTopicCreation: true, RequiredAcks: kafka.RequireOne},
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
	_ = p.assessedWriter.Close()
	return p.flaggedWriter.Close()
}

func (p *Processor) handleMessage(ctx context.Context, msg kafka.Message) error {
	sourceEventID := extractID(msg.Value)
	if sourceEventID == "" {
		sourceEventID = uuid.NewString()
	}

	score := calculateScore(msg.Value)
	flagged := score >= 75
	riskID := uuid.NewString()
	now := time.Now().UTC()

	assessedEvent := map[string]any{
		"id":            uuid.NewString(),
		"timestamp":     now.Format(time.RFC3339Nano),
		"aggregateId":   riskID,
		"aggregateType": "risk",
		"eventType":     "risk.assessed",
		"version":       1,
		"payload": map[string]any{
			"riskId":  riskID,
			"score":   score,
			"flagged": flagged,
		},
		"metadata": map[string]any{
			"correlationId": sourceEventID,
			"causationId":   sourceEventID,
			"traceId":       sourceEventID,
			"source":        p.cfg.ServiceName,
		},
	}
	assessedPayload, _ := json.Marshal(assessedEvent)

	tx, err := p.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	res, err := tx.ExecContext(ctx, `
		INSERT INTO risk.assessments (id, source_event_id, score, flagged, payload)
		VALUES ($1,$2,$3,$4,$5)
		ON CONFLICT (source_event_id) DO NOTHING
	`, riskID, sourceEventID, score, flagged, msg.Value)
	if err != nil {
		return err
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return nil
	}
	var assessedOutboxID int64
	if err := tx.QueryRowContext(ctx, `
		INSERT INTO risk.outbox (aggregate_id, aggregate_type, event_type, payload)
		VALUES ($1,$2,$3,$4) RETURNING id
	`, riskID, "risk", "risk.assessed", assessedPayload).Scan(&assessedOutboxID); err != nil {
		return err
	}

	var flaggedPayload []byte
	var flaggedOutboxID int64
	if flagged {
		flaggedEvent := map[string]any{
			"id":            uuid.NewString(),
			"timestamp":     now.Format(time.RFC3339Nano),
			"aggregateId":   riskID,
			"aggregateType": "risk",
			"eventType":     "risk.flagged",
			"version":       1,
			"payload": map[string]any{
				"riskId": riskID,
				"score":  score,
				"reason": "score-above-threshold",
			},
			"metadata": map[string]any{
				"correlationId": sourceEventID,
				"causationId":   sourceEventID,
				"traceId":       sourceEventID,
				"source":        p.cfg.ServiceName,
			},
		}
		flaggedPayload, _ = json.Marshal(flaggedEvent)
		if err := tx.QueryRowContext(ctx, `
			INSERT INTO risk.outbox (aggregate_id, aggregate_type, event_type, payload)
			VALUES ($1,$2,$3,$4) RETURNING id
		`, riskID, "risk", "risk.flagged", flaggedPayload).Scan(&flaggedOutboxID); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	if err := p.assessedWriter.WriteMessages(ctx, kafka.Message{Key: []byte(riskID), Value: assessedPayload, Time: now}); err != nil {
		return fmt.Errorf("publish risk.assessed: %w", err)
	}
	if _, err := p.db.ExecContext(ctx, `UPDATE risk.outbox SET processed=TRUE, processed_at=NOW() WHERE id=$1`, assessedOutboxID); err != nil {
		return err
	}
	if flagged {
		if err := p.flaggedWriter.WriteMessages(ctx, kafka.Message{Key: []byte(riskID), Value: flaggedPayload, Time: now}); err != nil {
			return fmt.Errorf("publish risk.flagged: %w", err)
		}
		_, _ = p.db.ExecContext(ctx, `UPDATE risk.outbox SET processed=TRUE, processed_at=NOW() WHERE id=$1`, flaggedOutboxID)
	}
	log.Printf("{\"service\":\"%s\",\"msg\":\"risk assessed\",\"score\":%d,\"flagged\":%t}", p.cfg.ServiceName, score, flagged)
	return nil
}

func calculateScore(raw []byte) int {
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return 10
	}
	evPayload, _ := payload["payload"].(map[string]any)
	inbound, _ := evPayload["inboundPayload"].(map[string]any)
	inner, _ := inbound["payload"].(map[string]any)
	amount, _ := inner["amount"].(float64)
	switch {
	case amount >= 5000:
		return 90
	case amount >= 1000:
		return 80
	case amount > 0:
		return 40
	default:
		return 20
	}
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
