package main

import (
	"context"
	"database/sql"
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

	_ "github.com/lib/pq"
	"github.com/nichom01/hi-volume-services/audit-service/internal/config"
	"github.com/nichom01/hi-volume-services/audit-service/internal/health"
	"github.com/nichom01/hi-volume-services/audit-service/internal/processor"
	"github.com/nichom01/hi-volume-services/audit-service/internal/server"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}
	db, err := sql.Open("postgres", cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("database open error: %v", err)
	}
	defer db.Close()
	if err := waitForDatabase(db, 30*time.Second); err != nil {
		log.Fatalf("database unavailable: %v", err)
	}
	if err := runMigrations(db, "migrations"); err != nil {
		log.Fatalf("migration error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := processor.New(db, processor.Config{
		ServiceName:   cfg.ServiceName,
		KafkaBrokers:  cfg.KafkaBrokers,
		ConsumerGroup: cfg.ConsumerGroup,
		InputTopic:    cfg.InputTopic,
		OutputTopic:        cfg.OutputTopic,
		Workers:            cfg.Workers,
		JobBuffer:          cfg.JobBuffer,
		WriterBatchSize:    cfg.WriterBatchSize,
		WriterBatchTimeout: cfg.WriterBatchTimeout,
	})
	defer p.Close()
	perr := make(chan error, 1)
	go func() { perr <- p.Run(ctx) }()

	srv := server.New(cfg.Port, health.New(db))
	serr := make(chan error, 1)
	go func() { serr <- srv.Start() }()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sigCh:
	case err := <-serr:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server error: %v", err)
		}
	case err := <-perr:
		if err != nil {
			log.Fatalf("processor error: %v", err)
		}
	}
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()
	_ = srv.Shutdown(shutdownCtx)
}

func waitForDatabase(db *sql.DB, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := db.PingContext(ctx)
		cancel()
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return err
		}
		time.Sleep(1 * time.Second)
	}
}

func runMigrations(db *sql.DB, dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("read migrations directory: %w", err)
	}
	var files []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".sql") {
			files = append(files, filepath.Join(dir, entry.Name()))
		}
	}
	sort.Strings(files)
	for _, file := range files {
		content, err := os.ReadFile(file)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", file, err)
		}
		query := strings.TrimSpace(string(content))
		if query == "" {
			continue
		}
		if _, err := db.Exec(query); err != nil {
			return fmt.Errorf("exec migration %s: %w", file, err)
		}
	}
	return nil
}
