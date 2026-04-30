package server

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/nichom01/hi-volume-services/validate-service/internal/health"
)

type Server struct {
	httpServer *http.Server
}

func New(port int, healthHandler *health.Handler) *Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler.Health)
	mux.HandleFunc("/ready", healthHandler.Ready)
	return &Server{
		httpServer: &http.Server{
			Addr:              fmt.Sprintf(":%d", port),
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		},
	}
}

func (s *Server) Start() error { return s.httpServer.ListenAndServe() }

func (s *Server) Shutdown(ctx context.Context) error { return s.httpServer.Shutdown(ctx) }
