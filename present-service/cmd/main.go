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
	port := env("SERVICE_PORT", "8008")

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
		Brokers: brokers, GroupID: env("KAFKA_CONSUMER_GROUP", "present-service-group"),
		GroupTopics: []string{"payment.completed", "notification.sent"}, MaxWait: 2 * time.Second,
	})
	defer reader.Close()
	writer := &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: env("TRANSACTION_COMPLETED_TOPIC", "transaction.completed"), AllowAutoTopicCreation: true, RequiredAcks: kafka.RequireOne}
	defer writer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		for {
			msg, err := reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				continue
			}
			if err := handlePresent(ctx, db, writer, msg); err == nil {
				_ = reader.CommitMessages(ctx, msg)
			}
		}
	}()

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

func handlePresent(ctx context.Context, db *sql.DB, writer *kafka.Writer, msg kafka.Message) error {
	sourceEventID, cid, err := extractIDs(msg.Value)
	if err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	_, err = tx.ExecContext(ctx, `INSERT INTO presentation.state (correlation_id) VALUES ($1) ON CONFLICT (correlation_id) DO NOTHING`, cid)
	if err != nil {
		return err
	}
	if msg.Topic == "payment.completed" {
		_, err = tx.ExecContext(ctx, `UPDATE presentation.state SET has_payment=TRUE, updated_at=NOW() WHERE correlation_id=$1`, cid)
	} else if msg.Topic == "notification.sent" {
		_, err = tx.ExecContext(ctx, `UPDATE presentation.state SET has_notification=TRUE, updated_at=NOW() WHERE correlation_id=$1`, cid)
	}
	if err != nil {
		return err
	}

	var hasPayment, hasNotification, completed bool
	if err := tx.QueryRowContext(ctx, `SELECT has_payment, has_notification, completed FROM presentation.state WHERE correlation_id=$1`, cid).Scan(&hasPayment, &hasNotification, &completed); err != nil {
		return err
	}
	if !(hasPayment && hasNotification) || completed {
		return tx.Commit()
	}

	transactionID := uuid.NewString()
	event := map[string]any{
		"id":            uuid.NewString(),
		"timestamp":     time.Now().UTC().Format(time.RFC3339Nano),
		"aggregateId":   transactionID,
		"aggregateType": "transaction",
		"eventType":     "transaction.completed",
		"version":       1,
		"payload": map[string]any{
			"transactionId": transactionID,
			"status":        "completed",
		},
		"metadata": map[string]any{
			"correlationId": cid,
			"causationId":   sourceEventID,
			"traceId":       cid,
			"source":        "present-service",
		},
	}
	encoded, _ := json.Marshal(event)
	if _, err := tx.ExecContext(ctx, `INSERT INTO presentation.transactions (id, correlation_id, payload) VALUES ($1,$2,$3)`, transactionID, cid, encoded); err != nil {
		return err
	}
	var outboxID int64
	if err := tx.QueryRowContext(ctx, `INSERT INTO presentation.outbox (aggregate_id, aggregate_type, event_type, payload) VALUES ($1,$2,$3,$4) RETURNING id`,
		transactionID, "transaction", "transaction.completed", encoded).Scan(&outboxID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE presentation.state SET completed=TRUE, updated_at=NOW() WHERE correlation_id=$1`, cid); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if err := writer.WriteMessages(ctx, kafka.Message{Key: []byte(transactionID), Value: encoded, Time: time.Now().UTC()}); err != nil {
		return err
	}
	_, _ = db.ExecContext(ctx, `UPDATE presentation.outbox SET processed=TRUE, processed_at=NOW() WHERE id=$1`, outboxID)
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
