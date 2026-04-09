package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/sathish/bigquery-emulator/server"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var version = "dev"

func main() {
	// Define flags
	project := flag.String("project", "", "Google Cloud project ID (required)")
	port := flag.Int("port", 9050, "HTTP server port")
	grpcPort := flag.Int("grpc-port", 9060, "gRPC server port")
	database := flag.String("database", ":memory:", "DuckDB database path (:memory: for in-memory)")
	logLevel := flag.String("log-level", "info", "Log level (debug, info, warn, error)")
	showVersion := flag.Bool("version", false, "Show version and exit")

	flag.Parse()

	if *showVersion {
		fmt.Printf("bigquery-emulator %s\n", version)
		os.Exit(0)
	}

	// Environment variable overrides
	if envProject := os.Getenv("BIGQUERY_EMULATOR_PROJECT"); envProject != "" && *project == "" {
		*project = envProject
	}
	if envHost := os.Getenv("BIGQUERY_EMULATOR_HOST"); envHost != "" {
		fmt.Fprintf(os.Stderr, "BIGQUERY_EMULATOR_HOST=%s (informational)\n", envHost)
	}

	if *project == "" {
		fmt.Fprintln(os.Stderr, "error: --project flag or BIGQUERY_EMULATOR_PROJECT env var is required")
		flag.Usage()
		os.Exit(1)
	}

	// Configure logger
	var level zapcore.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		fmt.Fprintf(os.Stderr, "error: invalid log level %q: %v\n", *logLevel, err)
		os.Exit(1)
	}

	zapCfg := zap.NewProductionConfig()
	zapCfg.Level = zap.NewAtomicLevelAt(level)
	logger, err := zapCfg.Build()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: failed to create logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	// Log configuration
	logger.Info("BigQuery Emulator starting",
		zap.String("version", version),
		zap.String("project", *project),
		zap.Int("port", *port),
		zap.Int("grpc_port", *grpcPort),
		zap.String("database", *database),
		zap.String("log_level", *logLevel),
	)

	// Create server
	cfg := server.Config{
		Host:      "0.0.0.0",
		Port:      *port,
		GRPCPort:  *grpcPort,
		ProjectID: *project,
		Database:  *database,
		LogLevel:  *logLevel,
	}

	srv, err := server.New(cfg)
	if err != nil {
		logger.Fatal("failed to create server", zap.Error(err))
	}

	logger.Info("Storage API (read/write) available on same HTTP port",
		zap.Int("port", *port),
		zap.String("base_path", "/v1"),
	)

	// Listen for shutdown signals
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	logger.Info("BigQuery Emulator ready",
		zap.String("address", fmt.Sprintf("http://0.0.0.0:%d", *port)),
		zap.String("env_hint", fmt.Sprintf("export BIGQUERY_EMULATOR_HOST=localhost:%d", *port)),
	)

	if err := srv.Start(ctx); err != nil {
		logger.Fatal("server error", zap.Error(err))
	}

	logger.Info("server stopped")
}
