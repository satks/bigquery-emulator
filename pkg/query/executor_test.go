package query

import (
	"context"
	"sync"
	"testing"

	"github.com/sathish/bigquery-emulator/pkg/connection"
	_ "github.com/marcboeker/go-duckdb"
	"go.uber.org/zap"
)

// newTestExecutor creates an Executor backed by an in-memory DuckDB.
func newTestExecutor(t *testing.T) (*Executor, func()) {
	t.Helper()
	logger, _ := zap.NewDevelopment()
	mgr, err := connection.NewManager(":memory:", logger)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}
	exec := NewExecutor(mgr, logger)
	return exec, func() { mgr.Close() }
}

func TestNewExecutor(t *testing.T) {
	exec, cleanup := newTestExecutor(t)
	defer cleanup()

	if exec == nil {
		t.Fatal("NewExecutor returned nil")
	}
	if exec.connMgr == nil {
		t.Error("connMgr is nil")
	}
	if exec.typeMapper == nil {
		t.Error("typeMapper is nil")
	}
	if exec.logger == nil {
		t.Error("logger is nil")
	}
}

func TestExecutor_Query_SimpleSelect(t *testing.T) {
	exec, cleanup := newTestExecutor(t)
	defer cleanup()

	result, err := exec.Query(context.Background(), "SELECT 1 AS n")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if result == nil {
		t.Fatal("result is nil")
	}
	if !result.JobComplete {
		t.Error("expected JobComplete to be true")
	}
	if result.TotalRows != 1 {
		t.Errorf("expected 1 row, got %d", result.TotalRows)
	}
	if len(result.Schema) != 1 {
		t.Fatalf("expected 1 column in schema, got %d", len(result.Schema))
	}
	if result.Schema[0].Name != "n" {
		t.Errorf("expected column name 'n', got %q", result.Schema[0].Name)
	}
	// DuckDB returns INTEGER for literal 1; BigQuery type should map accordingly
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if len(result.Rows[0]) != 1 {
		t.Fatalf("expected 1 column in row, got %d", len(result.Rows[0]))
	}
	// The value should be a numeric type (int32, int64, etc.)
	val := result.Rows[0][0]
	if val == nil {
		t.Fatal("expected non-nil value for SELECT 1")
	}
}

func TestExecutor_Query_MultipleColumns(t *testing.T) {
	exec, cleanup := newTestExecutor(t)
	defer cleanup()

	result, err := exec.Query(context.Background(), "SELECT 1 AS a, 'hello' AS b, true AS c")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(result.Schema) != 3 {
		t.Fatalf("expected 3 columns in schema, got %d", len(result.Schema))
	}

	// Check column names
	expectedNames := []string{"a", "b", "c"}
	for i, name := range expectedNames {
		if result.Schema[i].Name != name {
			t.Errorf("schema[%d].Name: expected %q, got %q", i, name, result.Schema[i].Name)
		}
	}

	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	row := result.Rows[0]
	if len(row) != 3 {
		t.Fatalf("expected 3 columns in row, got %d", len(row))
	}

	// b should be a string
	if s, ok := row[1].(string); !ok || s != "hello" {
		t.Errorf("expected row[1]='hello' (string), got %v (%T)", row[1], row[1])
	}

	// c should be a bool
	if b, ok := row[2].(bool); !ok || !b {
		t.Errorf("expected row[2]=true (bool), got %v (%T)", row[2], row[2])
	}
}

func TestExecutor_Query_MultipleRows(t *testing.T) {
	exec, cleanup := newTestExecutor(t)
	defer cleanup()

	result, err := exec.Query(context.Background(), "SELECT * FROM generate_series(1, 5)")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if result.TotalRows != 5 {
		t.Errorf("expected 5 rows, got %d", result.TotalRows)
	}
	if len(result.Rows) != 5 {
		t.Errorf("expected 5 rows in data, got %d", len(result.Rows))
	}
}

func TestExecutor_Query_EmptyResult(t *testing.T) {
	exec, cleanup := newTestExecutor(t)
	defer cleanup()

	result, err := exec.Query(context.Background(), "SELECT 1 AS n WHERE false")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if result.TotalRows != 0 {
		t.Errorf("expected 0 rows, got %d", result.TotalRows)
	}
	if len(result.Rows) != 0 {
		t.Errorf("expected 0 rows in data, got %d", len(result.Rows))
	}
	// Schema should still be present even with empty result
	if len(result.Schema) == 0 {
		t.Error("expected schema to be present even with empty result")
	}
	if result.Schema[0].Name != "n" {
		t.Errorf("expected column name 'n', got %q", result.Schema[0].Name)
	}
}

func TestExecutor_Query_NullValues(t *testing.T) {
	exec, cleanup := newTestExecutor(t)
	defer cleanup()

	result, err := exec.Query(context.Background(), "SELECT NULL AS n")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != nil {
		t.Errorf("expected nil for NULL, got %v (%T)", result.Rows[0][0], result.Rows[0][0])
	}
}

func TestExecutor_Query_InvalidSQL(t *testing.T) {
	exec, cleanup := newTestExecutor(t)
	defer cleanup()

	_, err := exec.Query(context.Background(), "SELEKT INVALID GARBAGE")
	if err == nil {
		t.Fatal("expected error for invalid SQL, got nil")
	}
}

func TestExecutor_Execute_CreateTable(t *testing.T) {
	exec, cleanup := newTestExecutor(t)
	defer cleanup()

	result, err := exec.Execute(context.Background(), "CREATE TABLE test_tbl (id BIGINT, name VARCHAR)")
	if err != nil {
		t.Fatalf("Execute CREATE TABLE failed: %v", err)
	}
	if result == nil {
		t.Fatal("result is nil")
	}
	// DDL typically returns 0 rows affected
}

func TestExecutor_Execute_InsertAndQuery(t *testing.T) {
	exec, cleanup := newTestExecutor(t)
	defer cleanup()

	// Create table
	_, err := exec.Execute(context.Background(), "CREATE TABLE roundtrip (id BIGINT, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Insert data
	_, err = exec.Execute(context.Background(), "INSERT INTO roundtrip VALUES (1, 'alice'), (2, 'bob')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Query back
	result, err := exec.Query(context.Background(), "SELECT id, name FROM roundtrip ORDER BY id")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if result.TotalRows != 2 {
		t.Fatalf("expected 2 rows, got %d", result.TotalRows)
	}

	// Check first row
	if len(result.Rows[0]) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(result.Rows[0]))
	}

	// Check column names
	if result.Schema[0].Name != "id" || result.Schema[1].Name != "name" {
		t.Errorf("unexpected schema: %+v", result.Schema)
	}

	// Verify data
	// id=1
	row0 := result.Rows[0]
	switch v := row0[0].(type) {
	case int32:
		if v != 1 {
			t.Errorf("expected id=1, got %d", v)
		}
	case int64:
		if v != 1 {
			t.Errorf("expected id=1, got %d", v)
		}
	default:
		t.Errorf("unexpected type for id: %T (%v)", row0[0], row0[0])
	}

	if row0[1] != "alice" {
		t.Errorf("expected name='alice', got %v", row0[1])
	}
}

func TestExecutor_Execute_CreateSchemaAndTable(t *testing.T) {
	exec, cleanup := newTestExecutor(t)
	defer cleanup()

	// Create schema (DuckDB SCHEMA = BigQuery dataset)
	_, err := exec.Execute(context.Background(), "CREATE SCHEMA my_dataset")
	if err != nil {
		t.Fatalf("CREATE SCHEMA failed: %v", err)
	}

	// Create table in schema
	_, err = exec.Execute(context.Background(), "CREATE TABLE my_dataset.my_table (id BIGINT)")
	if err != nil {
		t.Fatalf("CREATE TABLE in schema failed: %v", err)
	}

	// Insert and query
	_, err = exec.Execute(context.Background(), "INSERT INTO my_dataset.my_table VALUES (42)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	result, err := exec.Query(context.Background(), "SELECT id FROM my_dataset.my_table")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if result.TotalRows != 1 {
		t.Errorf("expected 1 row, got %d", result.TotalRows)
	}
}

func TestExecutor_Execute_RowsAffected(t *testing.T) {
	exec, cleanup := newTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute(context.Background(), "CREATE TABLE affected_test (id BIGINT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	result, err := exec.Execute(context.Background(), "INSERT INTO affected_test VALUES (1), (2), (3)")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	if result.RowsAffected != 3 {
		t.Errorf("expected RowsAffected=3, got %d", result.RowsAffected)
	}
}

func TestExecutor_Execute_InvalidSQL(t *testing.T) {
	exec, cleanup := newTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute(context.Background(), "DROPTABLE nonexistent")
	if err == nil {
		t.Fatal("expected error for invalid SQL, got nil")
	}
}

func TestExecutor_QueryPage_BasicPagination(t *testing.T) {
	exec, cleanup := newTestExecutor(t)
	defer cleanup()

	// Create a table with 10 rows
	_, err := exec.Execute(context.Background(), "CREATE TABLE page_test AS SELECT * FROM generate_series(1, 10) AS t(val)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Get page of 5
	result, err := exec.QueryPage(context.Background(), "SELECT val FROM page_test ORDER BY val", 0, 5)
	if err != nil {
		t.Fatalf("QueryPage failed: %v", err)
	}
	if result.TotalRows != 5 {
		t.Errorf("expected 5 rows, got %d", result.TotalRows)
	}
}

func TestExecutor_QueryPage_FirstPage(t *testing.T) {
	exec, cleanup := newTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute(context.Background(), "CREATE TABLE fp_test AS SELECT * FROM generate_series(1, 10) AS t(val)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	result, err := exec.QueryPage(context.Background(), "SELECT val FROM fp_test ORDER BY val", 0, 3)
	if err != nil {
		t.Fatalf("QueryPage failed: %v", err)
	}
	if result.TotalRows != 3 {
		t.Errorf("expected 3 rows, got %d", result.TotalRows)
	}
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows in data, got %d", len(result.Rows))
	}
}

func TestExecutor_QueryPage_SecondPage(t *testing.T) {
	exec, cleanup := newTestExecutor(t)
	defer cleanup()

	_, err := exec.Execute(context.Background(), "CREATE TABLE sp_test AS SELECT * FROM generate_series(1, 10) AS t(val)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// First page: offset=0, limit=3
	first, err := exec.QueryPage(context.Background(), "SELECT val FROM sp_test ORDER BY val", 0, 3)
	if err != nil {
		t.Fatalf("QueryPage (first) failed: %v", err)
	}

	// Second page: offset=3, limit=3
	second, err := exec.QueryPage(context.Background(), "SELECT val FROM sp_test ORDER BY val", 3, 3)
	if err != nil {
		t.Fatalf("QueryPage (second) failed: %v", err)
	}

	if len(second.Rows) != 3 {
		t.Fatalf("expected 3 rows on second page, got %d", len(second.Rows))
	}

	// Second page should have different values than first page
	if len(first.Rows) > 0 && len(second.Rows) > 0 {
		// The first value on the second page should differ from the first value on the first page
		if first.Rows[0][0] == second.Rows[0][0] {
			t.Errorf("first page and second page returned same first row: %v", first.Rows[0][0])
		}
	}
}

func TestExecutor_Query_Concurrent(t *testing.T) {
	exec, cleanup := newTestExecutor(t)
	defer cleanup()

	var wg sync.WaitGroup
	errs := make(chan error, 5)

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := exec.Query(context.Background(), "SELECT 1 AS n")
			if err != nil {
				errs <- err
			}
		}()
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("concurrent query error: %v", err)
	}
}

func TestExecutor_Query_TypeMapping(t *testing.T) {
	exec, cleanup := newTestExecutor(t)
	defer cleanup()

	// Create a table with various types
	_, err := exec.Execute(context.Background(),
		"CREATE TABLE type_test ("+
			"i BIGINT, "+
			"f DOUBLE, "+
			"b BOOLEAN, "+
			"s VARCHAR, "+
			"d DATE, "+
			"ts TIMESTAMP"+
			")")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = exec.Execute(context.Background(),
		"INSERT INTO type_test VALUES (42, 3.14, true, 'hello', '2024-01-15', '2024-01-15 10:30:00')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	result, err := exec.Query(context.Background(), "SELECT * FROM type_test")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Verify schema types are BigQuery types
	expectedTypes := map[string]string{
		"i":  "INTEGER",
		"f":  "FLOAT",
		"b":  "BOOLEAN",
		"s":  "STRING",
		"d":  "DATE",
		"ts": "TIMESTAMP", // TIMESTAMP in DuckDB -> TIMESTAMP in BQ
	}

	for _, col := range result.Schema {
		expected, ok := expectedTypes[col.Name]
		if !ok {
			continue
		}
		if col.Type != expected {
			t.Errorf("column %q: expected BQ type %q, got %q", col.Name, expected, col.Type)
		}
	}
}

func BenchmarkExecutor_Query_Simple(b *testing.B) {
	logger, _ := zap.NewDevelopment()
	mgr, err := connection.NewManager(":memory:", logger)
	if err != nil {
		b.Fatalf("failed to create manager: %v", err)
	}
	defer mgr.Close()

	exec := NewExecutor(mgr, logger)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := exec.Query(ctx, "SELECT 1")
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}
	}
}

func BenchmarkExecutor_Query_Table(b *testing.B) {
	logger, _ := zap.NewDevelopment()
	mgr, err := connection.NewManager(":memory:", logger)
	if err != nil {
		b.Fatalf("failed to create manager: %v", err)
	}
	defer mgr.Close()

	exec := NewExecutor(mgr, logger)
	ctx := context.Background()

	// Set up a table with data
	_, err = exec.Execute(ctx, "CREATE TABLE bench_data AS SELECT i AS id, 'row_' || i AS name FROM generate_series(1, 1000) AS t(i)")
	if err != nil {
		b.Fatalf("CREATE TABLE failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := exec.Query(ctx, "SELECT id, name FROM bench_data WHERE id > 500")
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}
	}
}
