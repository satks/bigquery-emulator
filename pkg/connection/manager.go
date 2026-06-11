package connection

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	_ "github.com/marcboeker/go-duckdb"
	"go.uber.org/zap"
)

// Manager manages a DuckDB database connection with concurrent read access
// and serialized write access via sync.RWMutex.
type Manager struct {
	db     *sql.DB
	mu     sync.RWMutex
	logger *zap.Logger
}

// NewManager creates a new DuckDB connection manager.
// dsn can be ":memory:" for in-memory or a file path for persistent storage.
func NewManager(dsn string, logger *zap.Logger) (*Manager, error) {
	db, err := sql.Open("duckdb", dsn)
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}

	// Verify connection is valid
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping duckdb: %w", err)
	}

	m := &Manager{
		db:     db,
		logger: logger,
	}

	// Auto-load commonly used extensions
	ctx := context.Background()
	for _, ext := range []string{"json", "parquet"} {
		if err := m.LoadExtension(ctx, ext); err != nil {
			logger.Warn("failed to load extension", zap.String("extension", ext), zap.Error(err))
		}
	}

	// Register BQ-compatibility macros. Macros live in the database catalog,
	// so registering once covers all pooled connections.
	// farm_fingerprint: values do NOT match real FarmHash64 — DuckDB's hash()
	// is used instead. Stability and distribution hold, which is what
	// deterministic bucketing (MOD(ABS(FARM_FINGERPRINT(x)), N)) relies on.
	// The CASE guard matches BQ's NULL-in/NULL-out behavior. hash() returns
	// UBIGINT and CAST raises on overflow instead of wrapping, so values
	// above 2^63-1 are wrapped to negative via HUGEINT arithmetic
	// (two's complement), matching BQ's signed INT64 output domain.
	const farmFingerprintMacro = `CREATE OR REPLACE MACRO farm_fingerprint(s) AS
		CASE WHEN s IS NULL THEN NULL ELSE CAST(
			CAST(hash(s) AS HUGEINT) -
			CASE WHEN hash(s) > 9223372036854775807 THEN 18446744073709551616 ELSE 0 END
		AS BIGINT) END`
	if _, err := m.Exec(ctx, farmFingerprintMacro); err != nil {
		logger.Warn("failed to register farm_fingerprint macro", zap.Error(err))
	}

	logger.Info("DuckDB connection manager initialized", zap.String("dsn", dsn))
	return m, nil
}

// Close closes the underlying database connection.
func (m *Manager) Close() error {
	m.logger.Info("closing DuckDB connection")
	return m.db.Close()
}

// Query executes a read query (acquires read lock).
func (m *Manager) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.db.QueryContext(ctx, query, args...)
}

// QueryRow executes a read query returning a single row (acquires read lock).
func (m *Manager) QueryRow(ctx context.Context, query string, args ...any) *sql.Row {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.db.QueryRowContext(ctx, query, args...)
}

// Exec executes a write statement (acquires write lock).
func (m *Manager) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.db.ExecContext(ctx, query, args...)
}

// ExecTx executes a function within a write transaction.
// The write lock is held for the entire duration of the transaction.
// If fn returns an error, the transaction is rolled back and the error is returned.
// Otherwise the transaction is committed.
func (m *Manager) ExecTx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	if err := fn(tx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			m.logger.Error("rollback failed", zap.Error(rbErr))
		}
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}

// DB returns the underlying *sql.DB for advanced use cases.
func (m *Manager) DB() *sql.DB {
	return m.db
}

// LoadExtension loads a DuckDB extension (json, parquet, spatial, etc.).
func (m *Manager) LoadExtension(ctx context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, err := m.db.ExecContext(ctx, fmt.Sprintf("INSTALL '%s'; LOAD '%s';", name, name))
	if err != nil {
		return fmt.Errorf("load extension %s: %w", name, err)
	}
	m.logger.Debug("loaded DuckDB extension", zap.String("extension", name))
	return nil
}
