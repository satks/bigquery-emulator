package benchmark

import (
	"context"
	"fmt"
	"testing"

	"github.com/sathish/bigquery-emulator/pkg/connection"
	"github.com/sathish/bigquery-emulator/pkg/query"
	"github.com/sathish/bigquery-emulator/pkg/types"
	"go.uber.org/zap"
)

// setupBenchmarkEnv creates an in-memory DuckDB with a pre-populated bench_data
// table containing 10,000 rows. Returns the executor, translator, type mapper,
// and a cleanup function.
func setupBenchmarkEnv(b *testing.B) (*query.Executor, *query.Translator, *types.TypeMapper, func()) {
	b.Helper()
	logger := zap.NewNop()
	mgr, err := connection.NewManager(":memory:", logger)
	if err != nil {
		b.Fatalf("setup: %v", err)
	}
	exec := query.NewExecutor(mgr, logger)
	trans := query.NewTranslator()
	tm := types.NewTypeMapper()

	// Create test table and bulk-insert 10,000 rows.
	ctx := context.Background()
	if _, err := mgr.Exec(ctx, "CREATE TABLE bench_data (id BIGINT, name VARCHAR, value DOUBLE, active BOOLEAN)"); err != nil {
		b.Fatalf("create table: %v", err)
	}

	// Use a single INSERT with generate_series for fast bulk load instead of 10k round-trips.
	if _, err := mgr.Exec(ctx, `
		INSERT INTO bench_data
		SELECT
			i AS id,
			'name_' || CAST(i AS VARCHAR) AS name,
			i * 1.5 AS value,
			(i % 2 = 0) AS active
		FROM generate_series(0, 9999) AS t(i)
	`); err != nil {
		b.Fatalf("insert data: %v", err)
	}

	return exec, trans, tm, func() { mgr.Close() }
}

// ---------------------------------------------------------------------------
// Query benchmarks
// ---------------------------------------------------------------------------

func BenchmarkQuery_SimpleSelect(b *testing.B) {
	exec, _, _, cleanup := setupBenchmarkEnv(b)
	defer cleanup()

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := exec.Query(ctx, "SELECT 1")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQuery_SelectFromTable(b *testing.B) {
	exec, _, _, cleanup := setupBenchmarkEnv(b)
	defer cleanup()

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := exec.Query(ctx, "SELECT * FROM bench_data LIMIT 100")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQuery_Aggregation(b *testing.B) {
	exec, _, _, cleanup := setupBenchmarkEnv(b)
	defer cleanup()

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := exec.Query(ctx, "SELECT COUNT(*), SUM(value), AVG(value) FROM bench_data")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQuery_WhereClause(b *testing.B) {
	exec, _, _, cleanup := setupBenchmarkEnv(b)
	defer cleanup()

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := exec.Query(ctx, "SELECT * FROM bench_data WHERE id > 5000")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQuery_GroupBy(b *testing.B) {
	exec, _, _, cleanup := setupBenchmarkEnv(b)
	defer cleanup()

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := exec.Query(ctx, "SELECT active, COUNT(*) FROM bench_data GROUP BY active")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQuery_OrderBy(b *testing.B) {
	exec, _, _, cleanup := setupBenchmarkEnv(b)
	defer cleanup()

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := exec.Query(ctx, "SELECT * FROM bench_data ORDER BY value DESC LIMIT 100")
		if err != nil {
			b.Fatal(err)
		}
	}
}

// ---------------------------------------------------------------------------
// Translator benchmarks
// ---------------------------------------------------------------------------

func BenchmarkTranslator_SimpleQuery(b *testing.B) {
	trans := query.NewTranslator()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := trans.Translate("SELECT id, name FROM users WHERE active = true")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTranslator_ComplexQuery(b *testing.B) {
	trans := query.NewTranslator()
	sql := "SELECT CURRENT_TIMESTAMP(), DATE_ADD(`project.dataset.table`.created_at, INTERVAL 7 DAY), " +
		"CAST(amount AS INT64), DATE_DIFF(end_date, start_date, DAY) " +
		"FROM `project.dataset.table` WHERE SAFE_CAST(value AS FLOAT64) > 0"

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := trans.Translate(sql)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTranslator_DDLWithOptions(b *testing.B) {
	trans := query.NewTranslator()
	sql := "CREATE TABLE `my_project.my_dataset.my_table` (" +
		"id INT64, name STRING, created TIMESTAMP" +
		") OPTIONS(description='A test table', labels=[('env','prod')])"

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := trans.Translate(sql)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// ---------------------------------------------------------------------------
// Type mapping benchmarks
// ---------------------------------------------------------------------------

func BenchmarkTypeMapper_BQToDuckDB(b *testing.B) {
	tm := types.NewTypeMapper()

	bqTypes := []string{
		types.BQInt64, types.BQFloat64, types.BQBool, types.BQString,
		types.BQTimestamp, types.BQDate, types.BQNumeric, types.BQJson,
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tm.BQToDuckDB(bqTypes[i%len(bqTypes)])
	}
}

func BenchmarkTypeMapper_SchemaToDuckDB(b *testing.B) {
	tm := types.NewTypeMapper()

	schema := types.TableSchema{
		Fields: []types.FieldSchema{
			{Name: "id", Type: types.BQInt64, Mode: types.ModeRequired},
			{Name: "name", Type: types.BQString, Mode: types.ModeNullable},
			{Name: "email", Type: types.BQString, Mode: types.ModeNullable},
			{Name: "age", Type: types.BQInt64, Mode: types.ModeNullable},
			{Name: "balance", Type: types.BQFloat64, Mode: types.ModeNullable},
			{Name: "active", Type: types.BQBool, Mode: types.ModeNullable},
			{Name: "created", Type: types.BQTimestamp, Mode: types.ModeNullable},
			{Name: "tags", Type: types.BQString, Mode: types.ModeRepeated},
			{Name: "metadata", Type: types.BQJson, Mode: types.ModeNullable},
			{Name: "address", Type: types.BQStruct, Mode: types.ModeNullable, Fields: []types.FieldSchema{
				{Name: "street", Type: types.BQString},
				{Name: "city", Type: types.BQString},
				{Name: "zip", Type: types.BQString},
			}},
		},
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tm.SchemaToDuckDBColumns(schema)
	}
}

// ---------------------------------------------------------------------------
// Classifier benchmarks
// ---------------------------------------------------------------------------

func BenchmarkClassifySQL_Select(b *testing.B) {
	sql := "SELECT id, name, value FROM bench_data WHERE active = true ORDER BY id LIMIT 100"

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result := query.ClassifySQL(sql)
		if !result.IsQuery {
			b.Fatal("expected IsQuery=true")
		}
	}
}

func BenchmarkClassifySQL_WithComments(b *testing.B) {
	sql := `-- This is a line comment
/* This is a
   block comment */
-- Another line comment
SELECT id, name FROM bench_data WHERE id > 100`

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result := query.ClassifySQL(sql)
		if !result.IsQuery {
			b.Fatal("expected IsQuery=true")
		}
	}
}

// ---------------------------------------------------------------------------
// Sub-benchmarks for query scaling
// ---------------------------------------------------------------------------

func BenchmarkQuery_SelectScaling(b *testing.B) {
	exec, _, _, cleanup := setupBenchmarkEnv(b)
	defer cleanup()

	ctx := context.Background()

	limits := []int{10, 100, 1000, 5000}
	for _, limit := range limits {
		b.Run(fmt.Sprintf("Limit_%d", limit), func(b *testing.B) {
			q := fmt.Sprintf("SELECT * FROM bench_data LIMIT %d", limit)
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, err := exec.Query(ctx, q)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
