package query

import (
	"strings"
	"testing"
)

func TestTranslator_Translate_Passthrough(t *testing.T) {
	tr := NewTranslator()

	input := "SELECT id, name FROM users WHERE id = 1"
	result, err := tr.Translate(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != input {
		t.Errorf("expected passthrough:\n  input:  %q\n  output: %q", input, result)
	}
}

func TestTranslator_Translate_BacktickToDoubleQuote(t *testing.T) {
	tr := NewTranslator()

	tests := []struct {
		name   string
		input  string
		expect string
	}{
		{
			name:   "three-part identifier",
			input:  "SELECT * FROM `project.dataset.table`",
			expect: `SELECT * FROM "project"."dataset"."table"`,
		},
		{
			name:   "two-part identifier",
			input:  "SELECT * FROM `dataset.table`",
			expect: `SELECT * FROM "dataset"."table"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tr.Translate(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result != tt.expect {
				t.Errorf("expected %q, got %q", tt.expect, result)
			}
		})
	}
}

func TestTranslator_Translate_BacktickSingle(t *testing.T) {
	tr := NewTranslator()

	input := "SELECT `column_name` FROM users"
	expect := `SELECT "column_name" FROM users`
	result, err := tr.Translate(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != expect {
		t.Errorf("expected %q, got %q", expect, result)
	}
}

func TestTranslator_Translate_CURRENT_TIMESTAMP(t *testing.T) {
	tr := NewTranslator()

	input := "SELECT CURRENT_TIMESTAMP() AS ts"
	expect := "SELECT current_timestamp AS ts"
	result, err := tr.Translate(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != expect {
		t.Errorf("expected %q, got %q", expect, result)
	}
}

func TestTranslator_Translate_CURRENT_DATE(t *testing.T) {
	tr := NewTranslator()

	input := "SELECT CURRENT_DATE() AS d"
	expect := "SELECT current_date AS d"
	result, err := tr.Translate(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != expect {
		t.Errorf("expected %q, got %q", expect, result)
	}
}

func TestTranslator_Translate_StripOPTIONS(t *testing.T) {
	tr := NewTranslator()

	input := `CREATE TABLE t (x BIGINT) OPTIONS(description='test table', labels=[('env','prod')])`
	result, err := tr.Translate(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(result, "OPTIONS") {
		t.Errorf("expected OPTIONS to be stripped, got %q", result)
	}
	if !strings.Contains(result, "CREATE TABLE t (x BIGINT)") {
		t.Errorf("expected DDL preserved without OPTIONS, got %q", result)
	}
}

func TestTranslator_Translate_IFNULL(t *testing.T) {
	tr := NewTranslator()

	input := "SELECT IFNULL(name, 'unknown') FROM users"
	expect := "SELECT COALESCE(name, 'unknown') FROM users"
	result, err := tr.Translate(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != expect {
		t.Errorf("expected %q, got %q", expect, result)
	}
}

func TestTranslator_Translate_SAFE_DIVIDE(t *testing.T) {
	tr := NewTranslator()

	input := "SELECT SAFE_DIVIDE(revenue, cost) FROM sales"
	result, err := tr.Translate(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(result, "CASE WHEN") {
		t.Errorf("expected CASE WHEN in result, got %q", result)
	}
	if strings.Contains(result, "SAFE_DIVIDE") {
		t.Errorf("expected SAFE_DIVIDE to be replaced, got %q", result)
	}
}

func TestTranslator_Translate_SAFE_CAST(t *testing.T) {
	tr := NewTranslator()

	input := "SELECT SAFE_CAST(x AS INT64) FROM t"
	expect := "SELECT TRY_CAST(x AS BIGINT) FROM t"
	result, err := tr.Translate(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != expect {
		t.Errorf("expected %q, got %q", expect, result)
	}
}

func TestTranslator_Translate_CAST_TypeNames(t *testing.T) {
	tr := NewTranslator()

	tests := []struct {
		name   string
		input  string
		expect string
	}{
		{
			name:   "INT64 to BIGINT",
			input:  "SELECT CAST(x AS INT64) FROM t",
			expect: "SELECT CAST(x AS BIGINT) FROM t",
		},
		{
			name:   "FLOAT64 to DOUBLE",
			input:  "SELECT CAST(x AS FLOAT64) FROM t",
			expect: "SELECT CAST(x AS DOUBLE) FROM t",
		},
		{
			name:   "BOOL to BOOLEAN",
			input:  "SELECT CAST(x AS BOOL) FROM t",
			expect: "SELECT CAST(x AS BOOLEAN) FROM t",
		},
		{
			name:   "STRING to VARCHAR",
			input:  "SELECT CAST(x AS STRING) FROM t",
			expect: "SELECT CAST(x AS VARCHAR) FROM t",
		},
		{
			name:   "BYTES to BLOB",
			input:  "SELECT CAST(x AS BYTES) FROM t",
			expect: "SELECT CAST(x AS BLOB) FROM t",
		},
		{
			name:   "NUMERIC to DECIMAL",
			input:  "SELECT CAST(x AS NUMERIC) FROM t",
			expect: "SELECT CAST(x AS DECIMAL(38,9)) FROM t",
		},
		{
			name:   "BIGNUMERIC to DECIMAL",
			input:  "SELECT CAST(x AS BIGNUMERIC) FROM t",
			expect: "SELECT CAST(x AS DECIMAL(76,38)) FROM t",
		},
		{
			name:   "TIMESTAMP to TIMESTAMPTZ",
			input:  "SELECT CAST(x AS TIMESTAMP) FROM t",
			expect: "SELECT CAST(x AS TIMESTAMPTZ) FROM t",
		},
		{
			name:   "DATETIME to TIMESTAMP",
			input:  "SELECT CAST(x AS DATETIME) FROM t",
			expect: "SELECT CAST(x AS TIMESTAMP) FROM t",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tr.Translate(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result != tt.expect {
				t.Errorf("expected %q, got %q", tt.expect, result)
			}
		})
	}
}

func TestTranslator_Translate_ARRAY_AGG(t *testing.T) {
	tr := NewTranslator()

	input := "SELECT ARRAY_AGG(x) FROM t"
	expect := "SELECT list(x) FROM t"
	result, err := tr.Translate(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != expect {
		t.Errorf("expected %q, got %q", expect, result)
	}
}

func TestTranslator_Translate_REGEXP_CONTAINS(t *testing.T) {
	tr := NewTranslator()

	input := "SELECT REGEXP_CONTAINS(name, r'^test') FROM t"
	expect := "SELECT regexp_matches(name, r'^test') FROM t"
	result, err := tr.Translate(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != expect {
		t.Errorf("expected %q, got %q", expect, result)
	}
}

func TestTranslator_Translate_DATE_ADD(t *testing.T) {
	tr := NewTranslator()

	input := "SELECT DATE_ADD(hire_date, INTERVAL 1 DAY) FROM emp"
	result, err := tr.Translate(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expect := "SELECT (hire_date) + INTERVAL 1 DAY FROM emp"
	if result != expect {
		t.Errorf("expected %q, got %q", expect, result)
	}
}

func TestTranslator_Translate_DATE_SUB(t *testing.T) {
	tr := NewTranslator()

	input := "SELECT DATE_SUB(hire_date, INTERVAL 7 DAY) FROM emp"
	result, err := tr.Translate(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expect := "SELECT (hire_date) - INTERVAL 7 DAY FROM emp"
	if result != expect {
		t.Errorf("expected %q, got %q", expect, result)
	}
}

func TestTranslator_Translate_DATE_DIFF(t *testing.T) {
	tr := NewTranslator()

	input := "SELECT DATE_DIFF(end_date, start_date, DAY) FROM t"
	result, err := tr.Translate(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expect := "SELECT date_diff('DAY', start_date, end_date) FROM t"
	if result != expect {
		t.Errorf("expected %q, got %q", expect, result)
	}
}

func TestTranslator_Translate_DATE_TRUNC(t *testing.T) {
	tr := NewTranslator()

	input := "SELECT DATE_TRUNC(created_at, MONTH) FROM t"
	result, err := tr.Translate(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expect := "SELECT date_trunc('MONTH', created_at) FROM t"
	if result != expect {
		t.Errorf("expected %q, got %q", expect, result)
	}
}

func TestTranslator_Translate_FORMAT_TIMESTAMP(t *testing.T) {
	tr := NewTranslator()

	input := "SELECT FORMAT_TIMESTAMP('%Y-%m-%d', created_at) FROM t"
	result, err := tr.Translate(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expect := "SELECT strftime(created_at, '%Y-%m-%d') FROM t"
	if result != expect {
		t.Errorf("expected %q, got %q", expect, result)
	}
}

func TestTranslator_Translate_GENERATE_UUID(t *testing.T) {
	tr := NewTranslator()

	input := "SELECT GENERATE_UUID() AS id"
	expect := "SELECT uuid() AS id"
	result, err := tr.Translate(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != expect {
		t.Errorf("expected %q, got %q", expect, result)
	}
}

func TestTranslator_Translate_MultipleTranslations(t *testing.T) {
	tr := NewTranslator()

	input := "SELECT IFNULL(name, 'x'), CURRENT_TIMESTAMP(), ARRAY_AGG(val) FROM `project.dataset.table`"
	result, err := tr.Translate(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Check all translations happened
	if strings.Contains(result, "IFNULL") {
		t.Errorf("IFNULL not translated: %q", result)
	}
	if !strings.Contains(result, "COALESCE") {
		t.Errorf("expected COALESCE: %q", result)
	}
	if strings.Contains(result, "CURRENT_TIMESTAMP()") {
		t.Errorf("CURRENT_TIMESTAMP() not translated: %q", result)
	}
	if !strings.Contains(result, "current_timestamp") {
		t.Errorf("expected current_timestamp: %q", result)
	}
	if strings.Contains(result, "ARRAY_AGG") {
		t.Errorf("ARRAY_AGG not translated: %q", result)
	}
	if !strings.Contains(result, "list") {
		t.Errorf("expected list: %q", result)
	}
	if strings.Contains(result, "`") {
		t.Errorf("backticks not translated: %q", result)
	}
}

func TestTranslator_Translate_EmptyString(t *testing.T) {
	tr := NewTranslator()

	_, err := tr.Translate("")
	if err == nil {
		t.Error("expected error for empty string")
	}
}

func TestTranslator_Translate_NestedFunctions(t *testing.T) {
	tr := NewTranslator()

	input := "SELECT IFNULL(SAFE_CAST(x AS INT64), 0) FROM t"
	expect := "SELECT COALESCE(TRY_CAST(x AS BIGINT), 0) FROM t"
	result, err := tr.Translate(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != expect {
		t.Errorf("expected %q, got %q", expect, result)
	}
}

func TestTranslator_TranslateAndExtractOptions(t *testing.T) {
	tr := NewTranslator()

	input := `CREATE TABLE t (x BIGINT) OPTIONS(description='test table', labels=[('env','prod')])`
	sql, opts, err := tr.TranslateAndExtractOptions(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if strings.Contains(sql, "OPTIONS") {
		t.Errorf("expected OPTIONS stripped from SQL, got %q", sql)
	}
	if !strings.Contains(sql, "CREATE TABLE t (x BIGINT)") {
		t.Errorf("expected DDL preserved, got %q", sql)
	}

	if opts == nil {
		t.Fatal("expected non-nil options map")
	}
	if desc, ok := opts["description"]; !ok || desc != "test table" {
		t.Errorf("expected description='test table', got %q (ok=%v)", desc, ok)
	}
}

func BenchmarkTranslator_Translate_Simple(b *testing.B) {
	tr := NewTranslator()
	sql := "SELECT id, name FROM users WHERE id = 1"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = tr.Translate(sql)
	}
}

func BenchmarkTranslator_Translate_Complex(b *testing.B) {
	tr := NewTranslator()
	sql := "SELECT IFNULL(name, 'unknown'), SAFE_CAST(age AS INT64), CURRENT_TIMESTAMP(), ARRAY_AGG(val) FROM `project.dataset.table` WHERE DATE_DIFF(end_date, start_date, DAY) > 30"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = tr.Translate(sql)
	}
}
