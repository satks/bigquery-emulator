package query

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/sathish/bigquery-emulator/pkg/connection"
	"github.com/sathish/bigquery-emulator/pkg/types"
	"go.uber.org/zap"
)

// Executor runs SQL queries against DuckDB and returns BigQuery-compatible results.
type Executor struct {
	connMgr    *connection.Manager
	typeMapper *types.TypeMapper
	logger     *zap.Logger
}

// NewExecutor creates a new query executor.
func NewExecutor(connMgr *connection.Manager, logger *zap.Logger) *Executor {
	return &Executor{
		connMgr:    connMgr,
		typeMapper: types.NewTypeMapper(),
		logger:     logger,
	}
}

// Query executes a read query and returns results.
// The sql parameter should already be translated to DuckDB SQL.
func (e *Executor) Query(ctx context.Context, query string, args ...any) (*QueryResult, error) {
	rows, err := e.connMgr.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}
	defer rows.Close()

	return e.scanRows(rows)
}

// Execute runs a DDL or DML statement.
// Returns the number of rows affected (0 for DDL).
func (e *Executor) Execute(ctx context.Context, stmt string, args ...any) (*ExecResult, error) {
	result, err := e.connMgr.Exec(ctx, stmt, args...)
	if err != nil {
		return nil, fmt.Errorf("statement execution failed: %w", err)
	}

	affected, _ := result.RowsAffected()
	return &ExecResult{RowsAffected: affected}, nil
}

// QueryPage executes a query with pagination support.
// offset and limit control which rows to return.
func (e *Executor) QueryPage(ctx context.Context, query string, offset, limit int, args ...any) (*QueryResult, error) {
	paged := fmt.Sprintf("SELECT * FROM (%s) AS _subq LIMIT %d OFFSET %d", query, limit, offset)
	return e.Query(ctx, paged, args...)
}

// scanRows reads all rows from a sql.Rows and builds a QueryResult.
func (e *Executor) scanRows(rows *sql.Rows) (*QueryResult, error) {
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("failed to get column types: %w", err)
	}

	schema := make([]ColumnMeta, len(colTypes))
	for i, ct := range colTypes {
		duckType := ct.DatabaseTypeName()
		bqType := e.typeMapper.DuckDBToBQ(duckType)
		schema[i] = ColumnMeta{
			Name: ct.Name(),
			Type: bqType,
		}
	}

	var allRows [][]interface{}
	numCols := len(colTypes)

	for rows.Next() {
		vals := make([]interface{}, numCols)
		ptrs := make([]interface{}, numCols)
		for i := range vals {
			ptrs[i] = &vals[i]
		}

		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("row scan failed: %w", err)
		}

		allRows = append(allRows, vals)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %w", err)
	}

	return &QueryResult{
		Schema:      schema,
		Rows:        allRows,
		TotalRows:   uint64(len(allRows)),
		JobComplete: true,
	}, nil
}
