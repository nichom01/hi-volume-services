package processor

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
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
	Workers          int
	JobBuffer        int
	WriterBatchSize  int
	WriterBatchTimeout time.Duration
}

type Processor struct {
	db             *sql.DB
	reader         *kafka.Reader
	assessedWriter *kafka.Writer
	flaggedWriter  *kafka.Writer
	cfg            Config
}

type workerResult struct {
	msg kafka.Message
	err error
}

type commitCoordinator struct {
	mu       sync.Mutex
	expected map[int]int64
	pending  map[int]map[int64]kafka.Message
}

func newCommitCoordinator() *commitCoordinator {
	return &commitCoordinator{
		expected: make(map[int]int64),
		pending:  make(map[int]map[int64]kafka.Message),
	}
}

func (c *commitCoordinator) noteFetched(partition int, offset int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.pending[partition]; !ok {
		c.pending[partition] = make(map[int64]kafka.Message)
		c.expected[partition] = offset
	}
}

func (c *commitCoordinator) onSuccess(partition int, offset int64, msg kafka.Message) []kafka.Message {
	c.mu.Lock()
	defer c.mu.Unlock()
	pend, ok := c.pending[partition]
	if !ok {
		pend = make(map[int64]kafka.Message)
		c.pending[partition] = pend
		c.expected[partition] = offset
	}
	pend[offset] = msg
	exp := c.expected[partition]
	var batch []kafka.Message
	for {
		m, ok := pend[exp]
		if !ok {
			break
		}
		delete(pend, exp)
		batch = append(batch, m)
		exp++
	}
	c.expected[partition] = exp
	return batch
}

func New(db *sql.DB, cfg Config) *Processor {
	brokers := splitCSV(cfg.KafkaBrokers)
	wbatch := cfg.WriterBatchSize
	if wbatch < 1 {
		wbatch = 100
	}
	wtimeout := cfg.WriterBatchTimeout
	if wtimeout <= 0 {
		wtimeout = 10 * time.Millisecond
	}
	return &Processor{
		db: db,
		cfg: cfg,
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers, GroupID: cfg.ConsumerGroup, Topic: cfg.InputTopic, MaxWait: 2 * time.Second,
		}),
		assessedWriter: &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: cfg.RiskAssessedTopic, AllowAutoTopicCreation: true, RequiredAcks: kafka.RequireOne, BatchSize: wbatch, BatchTimeout: wtimeout},
		flaggedWriter:  &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: cfg.RiskFlaggedTopic, AllowAutoTopicCreation: true, RequiredAcks: kafka.RequireOne, BatchSize: wbatch, BatchTimeout: wtimeout},
	}
}

func (p *Processor) Run(ctx context.Context) error {
	workers := p.cfg.Workers
	if workers < 1 {
		workers = 1
	}
	buf := p.cfg.JobBuffer
	if buf < 1 {
		buf = 1
	}
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	log.Printf("{\"service\":\"%s\",\"msg\":\"event processor started\",\"input\":\"%s\",\"workers\":%d,\"jobBuffer\":%d}", p.cfg.ServiceName, p.cfg.InputTopic, workers, buf)
	coord := newCommitCoordinator()
	jobs := make(chan kafka.Message, buf)
	results := make(chan workerResult, buf+workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for msg := range jobs {
				err := p.handleMessage(runCtx, msg)
				select {
				case results <- workerResult{msg: msg, err: err}:
				case <-runCtx.Done():
					return
				}
			}
		}()
	}
	var commitErrMu sync.Mutex
	var commitErr error
	commitDone := make(chan struct{})
	go func() {
		defer close(commitDone)
		for res := range results {
			commitErrMu.Lock()
			fatal := commitErr != nil
			commitErrMu.Unlock()
			if fatal {
				continue
			}
			if res.err != nil {
				log.Printf("{\"service\":\"%s\",\"level\":\"error\",\"error\":%q}", p.cfg.ServiceName, res.err.Error())
				continue
			}
			batch := coord.onSuccess(res.msg.Partition, res.msg.Offset, res.msg)
			if len(batch) == 0 {
				continue
			}
			if err := p.reader.CommitMessages(runCtx, batch...); err != nil {
				if runCtx.Err() != nil {
					return
				}
				commitErrMu.Lock()
				commitErr = fmt.Errorf("commit messages: %w", err)
				commitErrMu.Unlock()
				cancelRun()
			}
		}
	}()
fetchLoop:
	for {
		msg, err := p.reader.FetchMessage(runCtx)
		if err != nil {
			if runCtx.Err() != nil || ctx.Err() != nil {
				break fetchLoop
			}
			close(jobs)
			wg.Wait()
			close(results)
			<-commitDone
			commitErrMu.Lock()
			errOut := commitErr
			commitErrMu.Unlock()
			if errOut != nil {
				return errOut
			}
			return err
		}
		coord.noteFetched(msg.Partition, msg.Offset)
		select {
		case jobs <- msg:
		case <-runCtx.Done():
			break fetchLoop
		}
	}
	close(jobs)
	wg.Wait()
	close(results)
	<-commitDone
	commitErrMu.Lock()
	errOut := commitErr
	commitErrMu.Unlock()
	return errOut
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
