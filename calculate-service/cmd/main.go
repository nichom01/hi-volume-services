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
	"strings"
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
	defer writer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go process(ctx, db, reader, writer)

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

func process(ctx context.Context, db *sql.DB, reader *kafka.Reader, writer *kafka.Writer) {
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("calculate fetch err: %v", err)
			continue
		}
		if err := handle(ctx, db, writer, msg); err != nil {
			log.Printf("calculate handle err: %v", err)
			continue
		}
		_ = reader.CommitMessages(ctx, msg)
	}
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
