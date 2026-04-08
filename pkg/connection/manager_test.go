package connection

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
)

func newTestLogger(t *testing.T) *zap.Logger {
	t.Helper()
	logger, err := zap.NewDevelopment()
	if err != nil {
		t.Fatalf("failed to create logger: %v", err)
	}
	return logger
}

func newTestManager(t *testing.T) *Manager {
	t.Helper()
	logger := newTestLogger(t)
	m, err := NewManager(":memory:", logger)
	if err != nil {
		t.Fatalf("NewManager(:memory:) returned error: %v", err)
	}
	t.Cleanup(func() {
		if err := m.Close(); err != nil {
			t.Errorf("Close() returned error: %v", err)
		}
	})
	return m
}

func TestNewManager(t *testing.T) {
	logger := newTestLogger(t)
	m, err := NewManager(":memory:", logger)
	if err != nil {
		t.Fatalf("NewManager(:memory:) returned error: %v", err)
	}
	if m == nil {
		t.Fatal("NewManager(:memory:) returned nil manager")
	}
	if m.DB() == nil {
		t.Fatal("DB() returned nil")
	}
	if err := m.Close(); err != nil {
		t.Fatalf("Close() returned error: %v", err)
	}
}

func TestNewManager_InvalidDSN(t *testing.T) {
	logger := newTestLogger(t)
	_, err := NewManager("/nonexistent/deeply/nested/path/db.duckdb", logger)
	if err == nil {
		t.Fatal("NewManager with invalid DSN should return error, got nil")
	}
}

func TestManager_Close(t *testing.T) {
	logger := newTestLogger(t)
	m, err := NewManager(":memory:", logger)
	if err != nil {
		t.Fatalf("NewManager(:memory:) returned error: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("Close() returned error: %v", err)
	}
}

func TestManager_Query_SimpleSelect(t *testing.T) {
	m := newTestManager(t)

	rows, err := m.Query(context.Background(), "SELECT 1 AS n")
	if err != nil {
		t.Fatalf("Query(SELECT 1) returned error: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Query(SELECT 1) returned no rows")
	}

	var n int
	if err := rows.Scan(&n); err != nil {
		t.Fatalf("Scan() returned error: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected n=1, got n=%d", n)
	}

	if rows.Next() {
		t.Fatal("Query(SELECT 1) returned more than one row")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err() returned: %v", err)
	}
}

func TestManager_Query_MultipleRows(t *testing.T) {
	m := newTestManager(t)

	rows, err := m.Query(context.Background(), "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name)")
	if err != nil {
		t.Fatalf("Query returned error: %v", err)
	}
	defer rows.Close()

	type row struct {
		id   int
		name string
	}
	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.name); err != nil {
			t.Fatalf("Scan() returned error: %v", err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err() returned: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(got))
	}
	if got[0].id != 1 || got[0].name != "a" {
		t.Fatalf("row 0: expected {1, a}, got {%d, %s}", got[0].id, got[0].name)
	}
	if got[2].id != 3 || got[2].name != "c" {
		t.Fatalf("row 2: expected {3, c}, got {%d, %s}", got[2].id, got[2].name)
	}
}

func TestManager_Query_Concurrent(t *testing.T) {
	m := newTestManager(t)

	const goroutines = 10
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			rows, err := m.Query(context.Background(), "SELECT 42 AS answer")
			if err != nil {
				errs <- err
				return
			}
			defer rows.Close()

			if !rows.Next() {
				errs <- fmt.Errorf("no rows returned")
				return
			}
			var n int
			if err := rows.Scan(&n); err != nil {
				errs <- err
				return
			}
			if n != 42 {
				errs <- fmt.Errorf("expected 42, got %d", n)
				return
			}
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatalf("concurrent query error: %v", err)
	}
}

func TestManager_Exec_CreateTable(t *testing.T) {
	m := newTestManager(t)

	_, err := m.Exec(context.Background(), "CREATE TABLE test_table (id INTEGER, name VARCHAR)")
	if err != nil {
		t.Fatalf("Exec(CREATE TABLE) returned error: %v", err)
	}

	// Verify table exists by querying it
	rows, err := m.Query(context.Background(), "SELECT * FROM test_table")
	if err != nil {
		t.Fatalf("Query(SELECT * FROM test_table) returned error: %v", err)
	}
	rows.Close()
}

func TestManager_Exec_InsertAndQuery(t *testing.T) {
	m := newTestManager(t)

	_, err := m.Exec(context.Background(), "CREATE TABLE roundtrip (id INTEGER, value VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	_, err = m.Exec(context.Background(), "INSERT INTO roundtrip VALUES (1, 'hello'), (2, 'world')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	row := m.QueryRow(context.Background(), "SELECT value FROM roundtrip WHERE id = 1")
	var value string
	if err := row.Scan(&value); err != nil {
		t.Fatalf("QueryRow Scan failed: %v", err)
	}
	if value != "hello" {
		t.Fatalf("expected 'hello', got '%s'", value)
	}
}

func TestManager_Exec_Sequential(t *testing.T) {
	m := newTestManager(t)

	_, err := m.Exec(context.Background(), "CREATE TABLE seq_test (id INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	// Multiple sequential writes should not deadlock
	for i := 0; i < 20; i++ {
		_, err := m.Exec(context.Background(), "INSERT INTO seq_test VALUES (?)", i)
		if err != nil {
			t.Fatalf("INSERT %d failed: %v", i, err)
		}
	}

	row := m.QueryRow(context.Background(), "SELECT COUNT(*) FROM seq_test")
	var count int
	if err := row.Scan(&count); err != nil {
		t.Fatalf("COUNT query failed: %v", err)
	}
	if count != 20 {
		t.Fatalf("expected 20 rows, got %d", count)
	}
}

func TestManager_ExecTx_Commit(t *testing.T) {
	m := newTestManager(t)

	_, err := m.Exec(context.Background(), "CREATE TABLE tx_test (id INTEGER, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	err = m.ExecTx(context.Background(), func(tx *sql.Tx) error {
		_, err := tx.ExecContext(context.Background(), "INSERT INTO tx_test VALUES (1, 'committed')")
		return err
	})
	if err != nil {
		t.Fatalf("ExecTx returned error: %v", err)
	}

	row := m.QueryRow(context.Background(), "SELECT name FROM tx_test WHERE id = 1")
	var name string
	if err := row.Scan(&name); err != nil {
		t.Fatalf("QueryRow Scan failed: %v", err)
	}
	if name != "committed" {
		t.Fatalf("expected 'committed', got '%s'", name)
	}
}

func TestManager_ExecTx_Rollback(t *testing.T) {
	m := newTestManager(t)

	_, err := m.Exec(context.Background(), "CREATE TABLE tx_rollback (id INTEGER, name VARCHAR)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	rollbackErr := fmt.Errorf("intentional rollback error")
	err = m.ExecTx(context.Background(), func(tx *sql.Tx) error {
		_, err := tx.ExecContext(context.Background(), "INSERT INTO tx_rollback VALUES (1, 'should_not_exist')")
		if err != nil {
			return err
		}
		return rollbackErr
	})
	if err != rollbackErr {
		t.Fatalf("ExecTx expected rollbackErr, got: %v", err)
	}

	row := m.QueryRow(context.Background(), "SELECT COUNT(*) FROM tx_rollback")
	var count int
	if err := row.Scan(&count); err != nil {
		t.Fatalf("COUNT query failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 rows after rollback, got %d", count)
	}
}

func TestManager_QueryContext_Canceled(t *testing.T) {
	m := newTestManager(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// A canceled context should cause an error
	// Give a small delay to ensure cancellation propagates
	time.Sleep(1 * time.Millisecond)

	_, err := m.Query(ctx, "SELECT 1")
	if err == nil {
		t.Fatal("expected error from canceled context, got nil")
	}
}

func TestManager_LoadExtension_JSON(t *testing.T) {
	m := newTestManager(t)

	err := m.LoadExtension(context.Background(), "json")
	if err != nil {
		t.Fatalf("LoadExtension(json) returned error: %v", err)
	}
}

func TestManager_LoadExtension_Parquet(t *testing.T) {
	m := newTestManager(t)

	err := m.LoadExtension(context.Background(), "parquet")
	if err != nil {
		t.Fatalf("LoadExtension(parquet) returned error: %v", err)
	}
}

func BenchmarkManager_Query(b *testing.B) {
	logger, _ := zap.NewDevelopment()
	m, err := NewManager(":memory:", logger)
	if err != nil {
		b.Fatalf("NewManager failed: %v", err)
	}
	defer m.Close()

	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := m.Query(ctx, "SELECT 1")
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}
		rows.Close()
	}
}

func BenchmarkManager_Exec(b *testing.B) {
	logger, _ := zap.NewDevelopment()
	m, err := NewManager(":memory:", logger)
	if err != nil {
		b.Fatalf("NewManager failed: %v", err)
	}
	defer m.Close()

	ctx := context.Background()
	_, err = m.Exec(ctx, "CREATE TABLE bench_exec (id INTEGER, value VARCHAR)")
	if err != nil {
		b.Fatalf("CREATE TABLE failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := m.Exec(ctx, "INSERT INTO bench_exec VALUES (?, ?)", i, "bench")
		if err != nil {
			b.Fatalf("INSERT failed: %v", err)
		}
	}
}
