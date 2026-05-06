package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
)

func main() {
	dbURL := mustEnv("DATABASE_URL")
	brokers := splitCSV(mustEnv("KAFKA_BROKERS"))
	group := env("KAFKA_CONSUMER_GROUP", "calculate-service-group")
	outTopic := env("CALCULATION_COMPLETED_TOPIC", "calculation.completed")
	port := env("SERVICE_PORT", "8005")

	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	if err := waitForDB(db, 30*time.Second); err != nil {
		log.Fatal(err)
	}
	if err := runMigrations(db, "migrations"); err != nil {
		log.Fatal(err)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		GroupID:     group,
		GroupTopics: []string{"validation.passed", "risk.assessed"},
		MaxWait:     2 * time.Second,
	})
	defer reader.Close()
	writer := &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: outTopic, AllowAutoTopicCreation: true, RequiredAcks: kafka.RequireOne}
	writer.BatchSize = envInt("CALCULATE_WRITER_BATCH_SIZE", 100)
	writer.BatchTimeout = time.Duration(envInt("CALCULATE_WRITER_BATCH_MS", 10)) * time.Millisecond
	defer writer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go process(ctx, db, reader, writer, envInt("CALCULATE_WORKERS", 4), envInt("CALCULATE_JOB_BUFFER", 128))

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte(`{"status":"ok"}`)) })
	mux.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) {
		if db.Ping() != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(`{"ready":false}`))
			return
		}
		w.Write([]byte(`{"ready":true}`))
	})
	srv := &http.Server{Addr: ":" + port, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sigCh:
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	_ = srv.Shutdown(shutdownCtx)
}

type calculateResult struct {
	msg kafka.Message
	err error
}

type calculateCommitCoordinator struct {
	mu       sync.Mutex
	expected map[int]int64
	pending  map[int]map[int64]kafka.Message
}

func newCalculateCommitCoordinator() *calculateCommitCoordinator {
	return &calculateCommitCoordinator{expected: map[int]int64{}, pending: map[int]map[int64]kafka.Message{}}
}

func (c *calculateCommitCoordinator) noteFetched(partition int, offset int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.pending[partition]; !ok {
		c.pending[partition] = map[int64]kafka.Message{}
		c.expected[partition] = offset
	}
}

func (c *calculateCommitCoordinator) onSuccess(msg kafka.Message) []kafka.Message {
	c.mu.Lock()
	defer c.mu.Unlock()
	partition := msg.Partition
	pend, ok := c.pending[partition]
	if !ok {
		pend = map[int64]kafka.Message{}
		c.pending[partition] = pend
		c.expected[partition] = msg.Offset
	}
	pend[msg.Offset] = msg
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

func process(ctx context.Context, db *sql.DB, reader *kafka.Reader, writer *kafka.Writer, workers, jobBuffer int) {
	if workers < 1 {
		workers = 1
	}
	if jobBuffer < 1 {
		jobBuffer = 1
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	coord := newCalculateCommitCoordinator()
	jobs := make(chan kafka.Message, jobBuffer)
	results := make(chan calculateResult, jobBuffer+workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for msg := range jobs {
				err := handle(runCtx, db, writer, msg)
				select {
				case results <- calculateResult{msg: msg, err: err}:
				case <-runCtx.Done():
					return
				}
			}
		}()
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for res := range results {
			if res.err != nil {
				log.Printf("calculate handle err: %v", res.err)
				continue
			}
			batch := coord.onSuccess(res.msg)
			if len(batch) == 0 {
				continue
			}
			if err := reader.CommitMessages(runCtx, batch...); err != nil {
				log.Printf("calculate commit err: %v", err)
				cancel()
				return
			}
		}
	}()
	for {
		msg, err := reader.FetchMessage(runCtx)
		if err != nil {
			if runCtx.Err() != nil || ctx.Err() != nil {
				break
			}
			log.Printf("calculate fetch err: %v", err)
			break
		}
		coord.noteFetched(msg.Partition, msg.Offset)
		select {
		case jobs <- msg:
		case <-runCtx.Done():
			break
		}
	}
	close(jobs)
	wg.Wait()
	close(results)
	<-done
}

func handle(ctx context.Context, db *sql.DB, writer *kafka.Writer, msg kafka.Message) error {
	cid, err := correlationID(msg.Value)
	if err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	_, err = tx.ExecContext(ctx, `INSERT INTO calculation.state (correlation_id) VALUES ($1) ON CONFLICT (correlation_id) DO NOTHING`, cid)
	if err != nil {
		return err
	}
	if msg.Topic == "validation.passed" {
		_, err = tx.ExecContext(ctx, `UPDATE calculation.state SET has_validation=TRUE, updated_at=NOW() WHERE correlation_id=$1`, cid)
	} else if msg.Topic == "risk.assessed" {
		_, err = tx.ExecContext(ctx, `UPDATE calculation.state SET has_risk=TRUE, updated_at=NOW() WHERE correlation_id=$1`, cid)
	}
	if err != nil {
		return err
	}

	var hasValidation, hasRisk, calculated bool
	if err := tx.QueryRowContext(ctx, `SELECT has_validation, has_risk, calculated FROM calculation.state WHERE correlation_id=$1`, cid).Scan(&hasValidation, &hasRisk, &calculated); err != nil {
		return err
	}
	if !(hasValidation && hasRisk) || calculated {
		return tx.Commit()
	}

	calculationID := uuid.NewString()
	event := map[string]any{
		"id":            uuid.NewString(),
		"timestamp":     time.Now().UTC().Format(time.RFC3339Nano),
		"aggregateId":   calculationID,
		"aggregateType": "calculation",
		"eventType":     "calculation.completed",
		"version":       1,
		"payload": map[string]any{
			"calculationId": calculationID,
			"result":        "ok",
		},
		"metadata": map[string]any{
			"correlationId": cid,
			"causationId":   cid,
			"traceId":       cid,
			"source":        "calculate-service",
		},
	}
	encoded, _ := json.Marshal(event)
	if _, err := tx.ExecContext(ctx, `INSERT INTO calculation.results (id, correlation_id, payload) VALUES ($1,$2,$3)`, calculationID, cid, encoded); err != nil {
		return err
	}
	var outboxID int64
	if err := tx.QueryRowContext(ctx, `INSERT INTO calculation.outbox (aggregate_id, aggregate_type, event_type, payload) VALUES ($1,$2,$3,$4) RETURNING id`,
		calculationID, "calculation", "calculation.completed", encoded).Scan(&outboxID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE calculation.state SET calculated=TRUE, updated_at=NOW() WHERE correlation_id=$1`, cid); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}

	if err := writer.WriteMessages(ctx, kafka.Message{Key: []byte(calculationID), Value: encoded, Time: time.Now().UTC()}); err != nil {
		return err
	}
	_, _ = db.ExecContext(ctx, `UPDATE calculation.outbox SET processed=TRUE, processed_at=NOW() WHERE id=$1`, outboxID)
	return nil
}

func correlationID(raw []byte) (string, error) {
	var event map[string]any
	if err := json.Unmarshal(raw, &event); err != nil {
		return "", err
	}
	md, ok := event["metadata"].(map[string]any)
	if !ok {
		return "", fmt.Errorf("missing metadata")
	}
	cid, _ := md["correlationId"].(string)
	if cid == "" {
		return "", fmt.Errorf("missing correlationId")
	}
	return cid, nil
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("missing required env var: %s", key)
	}
	return v
}
func env(k, fallback string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return fallback
}
func envInt(k string, fb int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fb
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
func waitForDB(db *sql.DB, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if err := db.Ping(); err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("database not ready in time")
		}
		time.Sleep(time.Second)
	}
}
func runMigrations(db *sql.DB, dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	var files []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			files = append(files, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(files)
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return err
		}
		q := strings.TrimSpace(string(b))
		if q == "" {
			continue
		}
		if _, err := db.Exec(q); err != nil {
			return err
		}
	}
	return nil
}
