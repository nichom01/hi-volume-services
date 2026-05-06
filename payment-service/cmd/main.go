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
	port := env("SERVICE_PORT", "8006")

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
		Brokers: brokers, GroupID: env("KAFKA_CONSUMER_GROUP", "payment-service-group"), Topic: env("INPUT_TOPIC", "calculation.completed"), MaxWait: 2 * time.Second,
	})
	defer reader.Close()
	writer := &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: env("PAYMENT_COMPLETED_TOPIC", "payment.completed"), AllowAutoTopicCreation: true, RequiredAcks: kafka.RequireOne}
	writer.BatchSize = envInt("PAYMENT_WRITER_BATCH_SIZE", 100)
	writer.BatchTimeout = time.Duration(envInt("PAYMENT_WRITER_BATCH_MS", 10)) * time.Millisecond
	defer writer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go consumePayments(ctx, db, reader, writer, envInt("PAYMENT_WORKERS", 8), envInt("PAYMENT_JOB_BUFFER", 256))

	srv := httpServer(port, db)
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

type paymentResult struct {
	msg kafka.Message
	err error
}

type paymentCommitCoordinator struct {
	mu       sync.Mutex
	expected map[int]int64
	pending  map[int]map[int64]kafka.Message
}

func newPaymentCommitCoordinator() *paymentCommitCoordinator {
	return &paymentCommitCoordinator{expected: map[int]int64{}, pending: map[int]map[int64]kafka.Message{}}
}

func (c *paymentCommitCoordinator) noteFetched(partition int, offset int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.pending[partition]; !ok {
		c.pending[partition] = map[int64]kafka.Message{}
		c.expected[partition] = offset
	}
}

func (c *paymentCommitCoordinator) onSuccess(msg kafka.Message) []kafka.Message {
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

func consumePayments(ctx context.Context, db *sql.DB, reader *kafka.Reader, writer *kafka.Writer, workers, jobBuffer int) {
	if workers < 1 {
		workers = 1
	}
	if jobBuffer < 1 {
		jobBuffer = 1
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	coord := newPaymentCommitCoordinator()
	jobs := make(chan kafka.Message, jobBuffer)
	results := make(chan paymentResult, jobBuffer+workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for msg := range jobs {
				err := handlePayment(runCtx, db, writer, msg)
				select {
				case results <- paymentResult{msg: msg, err: err}:
				case <-runCtx.Done():
					return
				}
			}
		}()
	}
	var commitErr error
	var commitMu sync.Mutex
	done := make(chan struct{})
	go func() {
		defer close(done)
		for res := range results {
			commitMu.Lock()
			fatal := commitErr != nil
			commitMu.Unlock()
			if fatal {
				continue
			}
			if res.err != nil {
				log.Printf("payment handle err partition=%d offset=%d: %v", res.msg.Partition, res.msg.Offset, res.err)
				continue
			}
			batch := coord.onSuccess(res.msg)
			if len(batch) == 0 {
				continue
			}
			if err := reader.CommitMessages(runCtx, batch...); err != nil {
				commitMu.Lock()
				commitErr = err
				commitMu.Unlock()
				cancel()
			}
		}
	}()
	for {
		msg, err := reader.FetchMessage(runCtx)
		if err != nil {
			if runCtx.Err() != nil || ctx.Err() != nil {
				break
			}
			log.Printf("payment fetch err: %v", err)
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

func handlePayment(ctx context.Context, db *sql.DB, writer *kafka.Writer, msg kafka.Message) error {
	sourceEventID, cid, err := extractIDs(msg.Value)
	if err != nil {
		return err
	}
	paymentID := uuid.NewString()
	event := map[string]any{
		"id":            uuid.NewString(),
		"timestamp":     time.Now().UTC().Format(time.RFC3339Nano),
		"aggregateId":   paymentID,
		"aggregateType": "payment",
		"eventType":     "payment.completed",
		"version":       1,
		"payload": map[string]any{
			"paymentId": paymentID,
			"status":    "completed",
		},
		"metadata": map[string]any{
			"correlationId": cid,
			"causationId":   sourceEventID,
			"traceId":       cid,
			"source":        "payment-service",
		},
	}
	encoded, _ := json.Marshal(event)

	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	res, err := tx.ExecContext(ctx, `INSERT INTO payment.transactions (id, source_event_id, status, payload) VALUES ($1,$2,$3,$4) ON CONFLICT (source_event_id) DO NOTHING`,
		paymentID, sourceEventID, "completed", msg.Value)
	if err != nil {
		return err
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return tx.Commit()
	}
	var outboxID int64
	if err := tx.QueryRowContext(ctx, `INSERT INTO payment.outbox (aggregate_id, aggregate_type, event_type, payload) VALUES ($1,$2,$3,$4) RETURNING id`,
		paymentID, "payment", "payment.completed", encoded).Scan(&outboxID); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if err := writer.WriteMessages(ctx, kafka.Message{Key: []byte(paymentID), Value: encoded, Time: time.Now().UTC()}); err != nil {
		return err
	}
	_, _ = db.ExecContext(ctx, `UPDATE payment.outbox SET processed=TRUE, processed_at=NOW() WHERE id=$1`, outboxID)
	return nil
}

func extractIDs(raw []byte) (string, string, error) {
	var event map[string]any
	if err := json.Unmarshal(raw, &event); err != nil {
		return "", "", err
	}
	id, _ := event["id"].(string)
	md, ok := event["metadata"].(map[string]any)
	if !ok {
		return "", "", fmt.Errorf("missing metadata")
	}
	cid, _ := md["correlationId"].(string)
	if id == "" || cid == "" {
		return "", "", fmt.Errorf("missing IDs")
	}
	return id, cid, nil
}

func httpServer(port string, db *sql.DB) *http.Server {
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
	return &http.Server{Addr: ":" + port, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
}
func mustEnv(k string) string { v := os.Getenv(k); if v == "" { log.Fatalf("missing env %s", k) }; return v }
func env(k, fb string) string { if v := os.Getenv(k); v != "" { return v }; return fb }
func envInt(k string, fb int) int { if v := os.Getenv(k); v != "" { if n, err := strconv.Atoi(v); err == nil { return n } }; return fb }
func splitCSV(s string) []string { out := []string{}; for _, p := range strings.Split(s, ",") { if v := strings.TrimSpace(p); v != "" { out = append(out, v) } }; return out }
func waitForDB(db *sql.DB, timeout time.Duration) error { dl := time.Now().Add(timeout); for { if db.Ping() == nil { return nil }; if time.Now().After(dl) { return fmt.Errorf("db not ready") }; time.Sleep(time.Second) } }
func runMigrations(db *sql.DB, dir string) error {
	entries, err := os.ReadDir(dir); if err != nil { return err }
	var files []string
	for _, e := range entries { if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") { files = append(files, filepath.Join(dir, e.Name())) } }
	sort.Strings(files)
	for _, f := range files { b, err := os.ReadFile(f); if err != nil { return err }; q := strings.TrimSpace(string(b)); if q != "" { if _, err := db.Exec(q); err != nil { return err } } }
	return nil
}
