package server

import (
	"context"
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/sathish/bigquery-emulator/pkg/connection"
	"github.com/sathish/bigquery-emulator/pkg/job"
	"github.com/sathish/bigquery-emulator/pkg/metadata"
	"github.com/sathish/bigquery-emulator/pkg/query"
	"go.uber.org/zap"
)

// Config holds server configuration.
type Config struct {
	Host      string
	Port      int
	GRPCPort  int
	ProjectID string
	Database  string // ":memory:" or file path
	LogLevel  string
}

// Server is the BigQuery emulator HTTP server.
type Server struct {
	config     Config
	router     chi.Router
	httpServer *http.Server
	connMgr    *connection.Manager
	repo       *metadata.Repository
	executor   *query.Executor
	translator *query.Translator
	jobMgr     *job.Manager
	logger     *zap.Logger
}

// New creates a new Server with all dependencies initialized.
func New(cfg Config) (*Server, error) {
	// Build logger
	var logger *zap.Logger
	var err error
	switch cfg.LogLevel {
	case "debug":
		logger, err = zap.NewDevelopment()
	default:
		logger, err = zap.NewProduction()
	}
	if err != nil {
		return nil, fmt.Errorf("create logger: %w", err)
	}

	// Database DSN
	dsn := cfg.Database
	if dsn == "" {
		dsn = ":memory:"
	}

	// Connection manager
	connMgr, err := connection.NewManager(dsn, logger)
	if err != nil {
		return nil, fmt.Errorf("create connection manager: %w", err)
	}

	// Metadata repository
	repo, err := metadata.NewRepository(connMgr, logger)
	if err != nil {
		connMgr.Close()
		return nil, fmt.Errorf("create metadata repository: %w", err)
	}

	// Seed the default project if specified
	if cfg.ProjectID != "" {
		ctx := context.Background()
		_ = repo.CreateProject(ctx, metadata.Project{ID: cfg.ProjectID})
	}

	// Query executor and translator
	executor := query.NewExecutor(connMgr, logger)
	translator := query.NewTranslator()

	// Job manager
	jobMgr := job.NewManager(repo, executor, translator, logger)

	s := &Server{
		config:     cfg,
		router:     chi.NewRouter(),
		connMgr:    connMgr,
		repo:       repo,
		executor:   executor,
		translator: translator,
		jobMgr:     jobMgr,
		logger:     logger,
	}

	s.setupRoutes()

	logger.Info("server created",
		zap.String("project", cfg.ProjectID),
		zap.String("database", dsn),
		zap.Int("port", cfg.Port),
	)

	return s, nil
}

// Router returns the chi.Router for testing.
func (s *Server) Router() chi.Router {
	return s.router
}

// Handler returns the router as an http.Handler.
func (s *Server) Handler() http.Handler {
	return s.router
}

// Start starts the HTTP server.
func (s *Server) Start(ctx context.Context) error {
	addr := fmt.Sprintf("%s:%d", s.config.Host, s.config.Port)
	s.httpServer = &http.Server{
		Addr:    addr,
		Handler: s.router,
	}

	s.logger.Info("starting HTTP server", zap.String("addr", addr))

	errCh := make(chan error, 1)
	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case err := <-errCh:
		return fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
		return s.Stop(context.Background())
	}
}

// Stop gracefully shuts down the server.
func (s *Server) Stop(ctx context.Context) error {
	s.logger.Info("stopping server")

	if s.httpServer != nil {
		if ctx == nil {
			ctx = context.Background()
		}
		if err := s.httpServer.Shutdown(ctx); err != nil {
			s.logger.Error("http server shutdown error", zap.Error(err))
		}
	}

	if s.connMgr != nil {
		if err := s.connMgr.Close(); err != nil {
			s.logger.Error("connection manager close error", zap.Error(err))
		}
	}

	return nil
}
