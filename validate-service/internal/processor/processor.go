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
	ServiceName           string
	KafkaBrokers          string
	KafkaConsumerGroup    string
	InputTopic            string
	ValidationPassedTopic string
	ValidationFailedTopic string
	Workers               int
	JobBuffer             int
	WriterBatchSize       int
	WriterBatchTimeout    time.Duration
}

type Processor struct {
	db           *sql.DB
	reader       *kafka.Reader
	passedWriter *kafka.Writer
	failedWriter *kafka.Writer
	cfg          Config
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
			Brokers: brokers, GroupID: cfg.KafkaConsumerGroup, Topic: cfg.InputTopic, MaxWait: 2 * time.Second,
		}),
		passedWriter: &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: cfg.ValidationPassedTopic, AllowAutoTopicCreation: true, RequiredAcks: kafka.RequireOne, BatchSize: wbatch, BatchTimeout: wtimeout},
		failedWriter: &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: cfg.ValidationFailedTopic, AllowAutoTopicCreation: true, RequiredAcks: kafka.RequireOne, BatchSize: wbatch, BatchTimeout: wtimeout},
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
				log.Printf("{\"service\":\"%s\",\"level\":\"error\",\"msg\":\"processing failed\",\"error\":%q}", p.cfg.ServiceName, res.err.Error())
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
				commitErr = fmt.Errorf("commit input messages: %w", err)
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
			return fmt.Errorf("fetch input message: %w", err)
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
