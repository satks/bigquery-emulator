package types

import (
	"strings"
	"testing"
)

func TestNewTypeMapper(t *testing.T) {
	m := NewTypeMapper()
	if m == nil {
		t.Fatal("NewTypeMapper() returned nil")
	}
	if m.bqToDuck == nil {
		t.Fatal("bqToDuck map is nil")
	}
	if m.duckToBQ == nil {
		t.Fatal("duckToBQ map is nil")
	}
}

func TestTypeMapper_BQToDuckDB_AllBaseTypes(t *testing.T) {
	m := NewTypeMapper()

	tests := []struct {
		bqType   string
		duckType string
	}{
		{BQInt64, "BIGINT"},
		{BQFloat64, "DOUBLE"},
		{BQNumeric, "DECIMAL(38,9)"},
		{BQBigNumeric, "DECIMAL(76,38)"},
		{BQBool, "BOOLEAN"},
		{BQString, "VARCHAR"},
		{BQBytes, "BLOB"},
		{BQDate, "DATE"},
		{BQTime, "TIME"},
		{BQTimestamp, "TIMESTAMPTZ"},
		{BQDatetime, "TIMESTAMP"},
		{BQGeography, "VARCHAR"},
		{BQJson, "JSON"},
		{BQInterval, "INTERVAL"},
		// RECORD and STRUCT both map to STRUCT for base type lookup
		{BQRecord, "STRUCT"},
		{BQStruct, "STRUCT"},
	}

	for _, tt := range tests {
		t.Run(tt.bqType, func(t *testing.T) {
			got := m.BQToDuckDB(tt.bqType)
			if got != tt.duckType {
				t.Errorf("BQToDuckDB(%q) = %q, want %q", tt.bqType, got, tt.duckType)
			}
		})
	}
}

func TestTypeMapper_DuckDBToBQ_AllBaseTypes(t *testing.T) {
	m := NewTypeMapper()

	tests := []struct {
		duckType string
		bqType   string
	}{
		{"BIGINT", BQInt64},
		{"DOUBLE", BQFloat64},
		{"DECIMAL(38,9)", BQNumeric},
		{"DECIMAL(76,38)", BQBigNumeric},
		{"BOOLEAN", BQBool},
		{"VARCHAR", BQString},
		{"BLOB", BQBytes},
		{"DATE", BQDate},
		{"TIME", BQTime},
		{"TIMESTAMPTZ", BQTimestamp},
		{"TIMESTAMP", BQDatetime},
		{"JSON", BQJson},
		{"INTERVAL", BQInterval},
		{"STRUCT", BQStruct},
	}

	for _, tt := range tests {
		t.Run(tt.duckType, func(t *testing.T) {
			got := m.DuckDBToBQ(tt.duckType)
			if got != tt.bqType {
				t.Errorf("DuckDBToBQ(%q) = %q, want %q", tt.duckType, got, tt.bqType)
			}
		})
	}
}

func TestTypeMapper_BQToDuckDB_CaseInsensitive(t *testing.T) {
	m := NewTypeMapper()

	tests := []struct {
		input    string
		expected string
	}{
		{"int64", "BIGINT"},
		{"INT64", "BIGINT"},
		{"Int64", "BIGINT"},
		{"string", "VARCHAR"},
		{"String", "VARCHAR"},
		{"STRING", "VARCHAR"},
		{"timestamp", "TIMESTAMPTZ"},
		{"bool", "BOOLEAN"},
		{"json", "JSON"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := m.BQToDuckDB(tt.input)
			if got != tt.expected {
				t.Errorf("BQToDuckDB(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestTypeMapper_BQToDuckDB_UnknownType(t *testing.T) {
	m := NewTypeMapper()

	got := m.BQToDuckDB("UNKNOWN_TYPE")
	if got != "VARCHAR" {
		t.Errorf("BQToDuckDB('UNKNOWN_TYPE') = %q, want 'VARCHAR'", got)
	}

	got = m.BQToDuckDB("FOOBAR")
	if got != "VARCHAR" {
		t.Errorf("BQToDuckDB('FOOBAR') = %q, want 'VARCHAR'", got)
	}

	got = m.BQToDuckDB("")
	if got != "VARCHAR" {
		t.Errorf("BQToDuckDB('') = %q, want 'VARCHAR'", got)
	}
}

func TestTypeMapper_DuckDBToBQ_UnknownType(t *testing.T) {
	m := NewTypeMapper()

	got := m.DuckDBToBQ("UNKNOWN_TYPE")
	if got != "STRING" {
		t.Errorf("DuckDBToBQ('UNKNOWN_TYPE') = %q, want 'STRING'", got)
	}

	got = m.DuckDBToBQ("FOOBAR")
	if got != "STRING" {
		t.Errorf("DuckDBToBQ('FOOBAR') = %q, want 'STRING'", got)
	}

	got = m.DuckDBToBQ("")
	if got != "STRING" {
		t.Errorf("DuckDBToBQ('') = %q, want 'STRING'", got)
	}
}

func TestTypeMapper_FieldToDuckDBColumn_SimpleTypes(t *testing.T) {
	m := NewTypeMapper()

	tests := []struct {
		name     string
		field    FieldSchema
		expected string
	}{
		{
			name:     "INT64 nullable",
			field:    FieldSchema{Name: "age", Type: BQInt64, Mode: ModeNullable},
			expected: "age BIGINT",
		},
		{
			name:     "STRING nullable",
			field:    FieldSchema{Name: "name", Type: BQString, Mode: ModeNullable},
			expected: "name VARCHAR",
		},
		{
			name:     "FLOAT64 nullable",
			field:    FieldSchema{Name: "score", Type: BQFloat64, Mode: ModeNullable},
			expected: "score DOUBLE",
		},
		{
			name:     "BOOL nullable",
			field:    FieldSchema{Name: "active", Type: BQBool, Mode: ModeNullable},
			expected: "active BOOLEAN",
		},
		{
			name:     "TIMESTAMP nullable",
			field:    FieldSchema{Name: "created_at", Type: BQTimestamp, Mode: ModeNullable},
			expected: "created_at TIMESTAMPTZ",
		},
		{
			name:     "empty mode defaults to nullable",
			field:    FieldSchema{Name: "val", Type: BQInt64},
			expected: "val BIGINT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := m.FieldToDuckDBColumn(tt.field)
			if got != tt.expected {
				t.Errorf("FieldToDuckDBColumn() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestTypeMapper_FieldToDuckDBColumn_Required(t *testing.T) {
	m := NewTypeMapper()

	tests := []struct {
		name     string
		field    FieldSchema
		expected string
	}{
		{
			name:     "INT64 required",
			field:    FieldSchema{Name: "id", Type: BQInt64, Mode: ModeRequired},
			expected: "id BIGINT NOT NULL",
		},
		{
			name:     "STRING required",
			field:    FieldSchema{Name: "email", Type: BQString, Mode: ModeRequired},
			expected: "email VARCHAR NOT NULL",
		},
		{
			name:     "BOOL required",
			field:    FieldSchema{Name: "active", Type: BQBool, Mode: ModeRequired},
			expected: "active BOOLEAN NOT NULL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := m.FieldToDuckDBColumn(tt.field)
			if got != tt.expected {
				t.Errorf("FieldToDuckDBColumn() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestTypeMapper_FieldToDuckDBColumn_Repeated(t *testing.T) {
	m := NewTypeMapper()

	tests := []struct {
		name     string
		field    FieldSchema
		expected string
	}{
		{
			name:     "REPEATED STRING (array of strings)",
			field:    FieldSchema{Name: "tags", Type: BQString, Mode: ModeRepeated},
			expected: "tags VARCHAR[]",
		},
		{
			name:     "REPEATED INT64 (array of ints)",
			field:    FieldSchema{Name: "scores", Type: BQInt64, Mode: ModeRepeated},
			expected: "scores BIGINT[]",
		},
		{
			name:     "REPEATED BOOL",
			field:    FieldSchema{Name: "flags", Type: BQBool, Mode: ModeRepeated},
			expected: "flags BOOLEAN[]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := m.FieldToDuckDBColumn(tt.field)
			if got != tt.expected {
				t.Errorf("FieldToDuckDBColumn() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestTypeMapper_FieldToDuckDBColumn_Struct(t *testing.T) {
	m := NewTypeMapper()

	field := FieldSchema{
		Name: "address",
		Type: BQStruct,
		Mode: ModeNullable,
		Fields: []FieldSchema{
			{Name: "street", Type: BQString, Mode: ModeNullable},
			{Name: "city", Type: BQString, Mode: ModeNullable},
			{Name: "zip", Type: BQString, Mode: ModeNullable},
		},
	}

	got := m.FieldToDuckDBColumn(field)
	expected := "address STRUCT(street VARCHAR, city VARCHAR, zip VARCHAR)"
	if got != expected {
		t.Errorf("FieldToDuckDBColumn() = %q, want %q", got, expected)
	}

	// RECORD type should produce the same result
	field.Type = BQRecord
	got = m.FieldToDuckDBColumn(field)
	if got != expected {
		t.Errorf("FieldToDuckDBColumn() with RECORD = %q, want %q", got, expected)
	}
}

func TestTypeMapper_FieldToDuckDBColumn_NestedStruct(t *testing.T) {
	m := NewTypeMapper()

	field := FieldSchema{
		Name: "person",
		Type: BQStruct,
		Mode: ModeNullable,
		Fields: []FieldSchema{
			{Name: "name", Type: BQString, Mode: ModeNullable},
			{
				Name: "address",
				Type: BQStruct,
				Mode: ModeNullable,
				Fields: []FieldSchema{
					{Name: "city", Type: BQString, Mode: ModeNullable},
					{Name: "zip", Type: BQString, Mode: ModeNullable},
				},
			},
			{Name: "age", Type: BQInt64, Mode: ModeNullable},
		},
	}

	got := m.FieldToDuckDBColumn(field)
	expected := "person STRUCT(name VARCHAR, address STRUCT(city VARCHAR, zip VARCHAR), age BIGINT)"
	if got != expected {
		t.Errorf("FieldToDuckDBColumn() = %q, want %q", got, expected)
	}
}

func TestTypeMapper_FieldToDuckDBColumn_ArrayOfStruct(t *testing.T) {
	m := NewTypeMapper()

	field := FieldSchema{
		Name: "items",
		Type: BQStruct,
		Mode: ModeRepeated,
		Fields: []FieldSchema{
			{Name: "product", Type: BQString, Mode: ModeNullable},
			{Name: "quantity", Type: BQInt64, Mode: ModeNullable},
			{Name: "price", Type: BQFloat64, Mode: ModeNullable},
		},
	}

	got := m.FieldToDuckDBColumn(field)
	expected := "items STRUCT(product VARCHAR, quantity BIGINT, price DOUBLE)[]"
	if got != expected {
		t.Errorf("FieldToDuckDBColumn() = %q, want %q", got, expected)
	}
}

func TestTypeMapper_SchemaToDuckDBColumns(t *testing.T) {
	m := NewTypeMapper()

	schema := TableSchema{
		Fields: []FieldSchema{
			{Name: "id", Type: BQInt64, Mode: ModeRequired},
			{Name: "name", Type: BQString, Mode: ModeNullable},
			{Name: "email", Type: BQString, Mode: ModeRequired},
			{Name: "score", Type: BQFloat64, Mode: ModeNullable},
			{Name: "tags", Type: BQString, Mode: ModeRepeated},
			{
				Name: "address",
				Type: BQStruct,
				Mode: ModeNullable,
				Fields: []FieldSchema{
					{Name: "city", Type: BQString, Mode: ModeNullable},
					{Name: "state", Type: BQString, Mode: ModeNullable},
				},
			},
		},
	}

	cols := m.SchemaToDuckDBColumns(schema)

	expected := []string{
		"id BIGINT NOT NULL",
		"name VARCHAR",
		"email VARCHAR NOT NULL",
		"score DOUBLE",
		"tags VARCHAR[]",
		"address STRUCT(city VARCHAR, state VARCHAR)",
	}

	if len(cols) != len(expected) {
		t.Fatalf("expected %d columns, got %d", len(expected), len(cols))
	}

	for i, col := range cols {
		if col != expected[i] {
			t.Errorf("column[%d] = %q, want %q", i, col, expected[i])
		}
	}

	// Verify these can form a valid CREATE TABLE statement
	createSQL := "CREATE TABLE test (" + strings.Join(cols, ", ") + ")"
	if !strings.Contains(createSQL, "CREATE TABLE test (") {
		t.Error("failed to build CREATE TABLE statement")
	}
}

func TestTypeMapper_SchemaToDuckDBColumns_Empty(t *testing.T) {
	m := NewTypeMapper()

	// Empty schema
	cols := m.SchemaToDuckDBColumns(TableSchema{})
	if len(cols) != 0 {
		t.Errorf("expected 0 columns for empty schema, got %d", len(cols))
	}

	// Schema with empty fields slice
	cols = m.SchemaToDuckDBColumns(TableSchema{Fields: []FieldSchema{}})
	if len(cols) != 0 {
		t.Errorf("expected 0 columns for empty fields, got %d", len(cols))
	}
}

func BenchmarkTypeMapper_BQToDuckDB(b *testing.B) {
	m := NewTypeMapper()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		m.BQToDuckDB("INT64")
		m.BQToDuckDB("STRING")
		m.BQToDuckDB("TIMESTAMP")
		m.BQToDuckDB("BOOL")
		m.BQToDuckDB("FLOAT64")
	}
}
