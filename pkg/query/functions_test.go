package query

import (
	"strings"
	"testing"
)

func TestNewFunctionRegistry(t *testing.T) {
	r := NewFunctionRegistry()
	if r == nil {
		t.Fatal("NewFunctionRegistry returned nil")
	}
	if r.RegisteredCount() == 0 {
		t.Fatal("expected non-zero registered count")
	}
	// We have 20 function mappings defined
	if r.RegisteredCount() < 20 {
		t.Errorf("expected at least 20 registered functions, got %d", r.RegisteredCount())
	}
}

func TestFunctionRegistry_Get_SimpleRename(t *testing.T) {
	r := NewFunctionRegistry()

	tr, ok := r.Get("IFNULL")
	if !ok {
		t.Fatal("IFNULL not found in registry")
	}
	if tr.DuckDBName != "COALESCE" {
		t.Errorf("expected DuckDBName=COALESCE, got %q", tr.DuckDBName)
	}
	if tr.Handler != nil {
		t.Error("expected Handler to be nil for simple rename")
	}
}

func TestFunctionRegistry_Get_AllSimpleRenames(t *testing.T) {
	r := NewFunctionRegistry()

	tests := []struct {
		bqName   string
		duckName string
	}{
		{"IFNULL", "COALESCE"},
		{"ARRAY_AGG", "list"},
		{"ARRAY_LENGTH", "len"},
		{"GENERATE_UUID", "uuid"},
		{"SAFE_CAST", "TRY_CAST"},
		{"REGEXP_CONTAINS", "regexp_matches"},
		{"REGEXP_EXTRACT", "regexp_extract"},
		{"REGEXP_REPLACE", "regexp_replace"},
		{"GENERATE_ARRAY", "generate_series"},
		{"STARTS_WITH", "starts_with"},
		{"ENDS_WITH", "suffix"},
		{"BYTE_LENGTH", "octet_length"},
		{"CHAR_LENGTH", "length"},
		{"ST_GEOGPOINT", "ST_Point"},
	}

	for _, tt := range tests {
		t.Run(tt.bqName, func(t *testing.T) {
			tr, ok := r.Get(tt.bqName)
			if !ok {
				t.Fatalf("%s not found in registry", tt.bqName)
			}
			if tr.DuckDBName != tt.duckName {
				t.Errorf("expected DuckDBName=%q, got %q", tt.duckName, tr.DuckDBName)
			}
			if tr.Handler != nil {
				t.Errorf("expected nil Handler for simple rename %s", tt.bqName)
			}
		})
	}
}

func TestFunctionRegistry_Get_Handler_SAFE_DIVIDE(t *testing.T) {
	r := NewFunctionRegistry()

	tr, ok := r.Get("SAFE_DIVIDE")
	if !ok {
		t.Fatal("SAFE_DIVIDE not found in registry")
	}
	if tr.DuckDBName != "" {
		t.Errorf("expected empty DuckDBName for handler-based function, got %q", tr.DuckDBName)
	}
	if tr.Handler == nil {
		t.Fatal("expected non-nil Handler for SAFE_DIVIDE")
	}

	result := tr.Handler("a, b")
	if !strings.Contains(result, "CASE WHEN") {
		t.Errorf("expected CASE WHEN in SAFE_DIVIDE result, got %q", result)
	}
	if !strings.Contains(result, "= 0") {
		t.Errorf("expected '= 0' in SAFE_DIVIDE result, got %q", result)
	}
	if !strings.Contains(result, "NULL") {
		t.Errorf("expected NULL in SAFE_DIVIDE result, got %q", result)
	}
}

func TestFunctionRegistry_Get_Handler_FORMAT(t *testing.T) {
	r := NewFunctionRegistry()

	tests := []struct {
		name     string
		funcName string
		args     string
		expect   string
	}{
		{
			name:     "FORMAT_TIMESTAMP reorders args",
			funcName: "FORMAT_TIMESTAMP",
			args:     "'%Y-%m-%d', created_at",
			expect:   "strftime(created_at, '%Y-%m-%d')",
		},
		{
			name:     "FORMAT_DATE reorders args",
			funcName: "FORMAT_DATE",
			args:     "'%Y-%m-%d', my_date",
			expect:   "strftime(my_date, '%Y-%m-%d')",
		},
		{
			name:     "PARSE_DATE reorders args and casts",
			funcName: "PARSE_DATE",
			args:     "'%Y-%m-%d', date_str",
			expect:   "strptime(date_str, '%Y-%m-%d')::DATE",
		},
		{
			name:     "PARSE_TIMESTAMP reorders args and casts",
			funcName: "PARSE_TIMESTAMP",
			args:     "'%Y-%m-%d %H:%M:%S', ts_str",
			expect:   "strptime(ts_str, '%Y-%m-%d %H:%M:%S')::TIMESTAMPTZ",
		},
		{
			name:     "TO_JSON_STRING wraps with cast",
			funcName: "TO_JSON_STRING",
			args:     "my_col",
			expect:   "to_json(my_col)::VARCHAR",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr, ok := r.Get(tt.funcName)
			if !ok {
				t.Fatalf("%s not found in registry", tt.funcName)
			}
			if tr.Handler == nil {
				t.Fatalf("expected non-nil Handler for %s", tt.funcName)
			}
			result := tr.Handler(tt.args)
			if result != tt.expect {
				t.Errorf("expected %q, got %q", tt.expect, result)
			}
		})
	}
}

func TestFunctionRegistry_Get_Unknown(t *testing.T) {
	r := NewFunctionRegistry()

	_, ok := r.Get("TOTALLY_FAKE_FUNCTION")
	if ok {
		t.Error("expected false for unknown function")
	}
}

func TestFunctionRegistry_Get_CaseInsensitive(t *testing.T) {
	r := NewFunctionRegistry()

	cases := []string{"ifnull", "IFNULL", "Ifnull", "IfNull"}
	for _, c := range cases {
		t.Run(c, func(t *testing.T) {
			tr, ok := r.Get(c)
			if !ok {
				t.Fatalf("%q not found in registry", c)
			}
			if tr.DuckDBName != "COALESCE" {
				t.Errorf("expected DuckDBName=COALESCE for %q, got %q", c, tr.DuckDBName)
			}
		})
	}
}
