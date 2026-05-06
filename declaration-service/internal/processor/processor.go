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
	"github.com/nichom01/hi-volume-services/declaration-service/internal/metrics"
	"github.com/segmentio/kafka-go"
)

type Config struct {
	ServiceName             string
	KafkaBrokers            string
	KafkaConsumerGroup      string
	InboundTopic            string
	DeclarationCreatedTopic string
	Workers                 int
	JobBuffer               int
	WriterBatchSize         int
	WriterBatchTimeout      time.Duration
}

type Processor struct {
	db     *sql.DB
	reader *kafka.Reader
	writer *kafka.Writer
	cfg    Config
}

type workerResult struct {
	msg kafka.Message
	err error
}

// commitCoordinator commits Kafka offsets in partition offset order when workers finish out of order.
type commitCoordinator struct {
	mu       sync.Mutex
	expected map[int]int64                  // next offset to commit per partition
	pending  map[int]map[int64]kafka.Message // buffered successful completions
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
			Brokers: brokers,
			GroupID: cfg.KafkaConsumerGroup,
			Topic:   cfg.InboundTopic,
			// Tighter poll improves latency under sustained load; workers still batch via job queue.
			MaxWait: 250 * time.Millisecond,
		}),
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Topic:                  cfg.DeclarationCreatedTopic,
			AllowAutoTopicCreation: true,
			RequiredAcks:           kafka.RequireOne,
			BatchSize:              wbatch,
			BatchTimeout:           wtimeout,
		},
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

	log.Printf("event processor started: inbound=%s outbound=%s workers=%d job_buffer=%d",
		p.cfg.InboundTopic, p.cfg.DeclarationCreatedTopic, workers, buf)

	coord := newCommitCoordinator()
	jobs := make(chan kafka.Message, buf)
	resultsBuf := buf + workers
	if resultsBuf < workers*2 {
		resultsBuf = workers * 2
	}
	results := make(chan workerResult, resultsBuf)

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
				log.Printf("failed processing message key=%s partition=%d offset=%d: %v",
					string(res.msg.Key), res.msg.Partition, res.msg.Offset, res.err)
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
				continue
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
			return fmt.Errorf("fetch inbound message: %w", err)
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
	if errOut != nil {
		return errOut
	}
	return nil
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

func (p *Processor) handleMessage(ctx context.Context, msg kafka.Message) (err error) {
	start := time.Now()
	defer func() {
		if err != nil {
			metrics.DeclarationsFailedTotal.Inc()
			return
		}
		metrics.ProcessingSeconds.Observe(time.Since(start).Seconds())
	}()

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
			"declarationId":  declarationID,
			"inboundTopic":     msg.Topic,
			"inboundOffset":    msg.Offset,
			"inboundPayload":   json.RawMessage(msg.Value),
		},
		Metadata: Metadata{
			CorrelationID: sourceEventID,
			CausationID:   sourceEventID,
			TraceID:       sourceEventID,
			Source:        p.cfg.ServiceName,
		},
	}

	eventPayload, marshalErr := json.Marshal(event)
	if marshalErr != nil {
		err = fmt.Errorf("marshal declaration event: %w", marshalErr)
		return err
	}

	tx, beginErr := p.db.BeginTx(ctx, &sql.TxOptions{})
	if beginErr != nil {
		err = fmt.Errorf("begin tx: %w", beginErr)
		return err
	}
	defer func() { _ = tx.Rollback() }()

	res, execErr := tx.ExecContext(ctx, `
		INSERT INTO declaration.declarations (id, source_event_id, payload)
		VALUES ($1, $2, $3)
		ON CONFLICT (source_event_id) DO NOTHING
	`, declarationID, sourceEventID, msg.Value)
	if execErr != nil {
		err = fmt.Errorf("insert declaration: %w", execErr)
		return err
	}
	rows, rowsErr := res.RowsAffected()
	if rowsErr != nil {
		err = fmt.Errorf("rows affected: %w", rowsErr)
		return err
	}
	if rows == 0 {
		// Idempotent replay: record was already handled previously.
		metrics.DeclarationsIdempotentTotal.Inc()
		return nil
	}

	var outboxID int64
	if scanErr := tx.QueryRowContext(ctx, `
		INSERT INTO declaration.outbox (aggregate_id, aggregate_type, event_type, payload)
		VALUES ($1, $2, $3, $4)
		RETURNING id
	`, declarationID, "declaration", "declaration.created", eventPayload).Scan(&outboxID); scanErr != nil {
		err = fmt.Errorf("insert outbox: %w", scanErr)
		return err
	}

	if commitErr := tx.Commit(); commitErr != nil {
		err = fmt.Errorf("commit tx: %w", commitErr)
		return err
	}

	if writeErr := p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(declarationID),
		Value: eventPayload,
		Time:  now,
	}); writeErr != nil {
		err = fmt.Errorf("publish declaration.created: %w", writeErr)
		return err
	}

	if _, upErr := p.db.ExecContext(ctx, `
		UPDATE declaration.outbox
		SET processed = TRUE, processed_at = NOW()
		WHERE id = $1
	`, outboxID); upErr != nil {
		err = fmt.Errorf("mark outbox processed: %w", upErr)
		return err
	}

	metrics.DeclarationsProcessedTotal.Inc()
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
