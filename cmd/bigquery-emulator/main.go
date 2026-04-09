package main

import (
	"flag"
	"fmt"
	"os"

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

	cfg := zap.NewProductionConfig()
	cfg.Level = zap.NewAtomicLevelAt(level)
	logger, err := cfg.Build()
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

	// TODO: Initialize connection manager, server, and start serving
	logger.Info("Storage API (read/write) available on same HTTP port",
		zap.Int("port", *port),
		zap.String("base_path", "/v1"),
	)
	logger.Info("server startup not yet implemented - exiting")
}
