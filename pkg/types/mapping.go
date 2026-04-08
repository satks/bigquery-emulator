package types

import (
	"fmt"
	"strings"
)

// TypeMapper converts between BigQuery and DuckDB type systems.
type TypeMapper struct {
	bqToDuck map[string]string
	duckToBQ map[string]string
}

// NewTypeMapper creates an initialized type mapper with all known mappings.
func NewTypeMapper() *TypeMapper {
	m := &TypeMapper{
		bqToDuck: make(map[string]string),
		duckToBQ: make(map[string]string),
	}

	// BQ -> DuckDB mappings (keys are uppercase)
	bqMappings := map[string]string{
		BQInt64:      "BIGINT",
		BQFloat64:    "DOUBLE",
		BQNumeric:    "DECIMAL(38,9)",
		BQBigNumeric: "DECIMAL(76,38)",
		BQBool:       "BOOLEAN",
		BQString:     "VARCHAR",
		BQBytes:      "BLOB",
		BQDate:       "DATE",
		BQTime:       "TIME",
		BQTimestamp:  "TIMESTAMPTZ",
		BQDatetime:   "TIMESTAMP",
		BQGeography:  "VARCHAR", // Store as WKT string; spatial extension may not be available
		BQJson:       "JSON",
		BQInterval:   "INTERVAL",
		BQStruct:     "STRUCT",
		BQRecord:     "STRUCT", // alias for STRUCT
	}

	for bq, duck := range bqMappings {
		m.bqToDuck[bq] = duck
	}

	// DuckDB -> BQ reverse mappings (keys are uppercase)
	// Note: GEOGRAPHY maps to VARCHAR in DuckDB, so reverse mapping for VARCHAR -> STRING (not GEOGRAPHY)
	duckMappings := map[string]string{
		"BIGINT":        BQInt64,
		"DOUBLE":        BQFloat64,
		"DECIMAL(38,9)": BQNumeric,
		"DECIMAL(76,38)": BQBigNumeric,
		"BOOLEAN":       BQBool,
		"VARCHAR":       BQString,
		"BLOB":          BQBytes,
		"DATE":          BQDate,
		"TIME":          BQTime,
		"TIMESTAMPTZ":   BQTimestamp,
		"TIMESTAMP":     BQDatetime,
		"JSON":          BQJson,
		"INTERVAL":      BQInterval,
		"STRUCT":        BQStruct,
	}

	for duck, bq := range duckMappings {
		m.duckToBQ[duck] = bq
	}

	return m
}

// BQToDuckDB converts a BigQuery type string to the equivalent DuckDB type.
// Case-insensitive. For unknown types, returns VARCHAR as a safe fallback.
func (m *TypeMapper) BQToDuckDB(bqType string) string {
	upper := strings.ToUpper(bqType)
	if duck, ok := m.bqToDuck[upper]; ok {
		return duck
	}
	return "VARCHAR"
}

// DuckDBToBQ converts a DuckDB type string to the equivalent BigQuery type.
// Case-insensitive. For unknown types, returns STRING as a safe fallback.
func (m *TypeMapper) DuckDBToBQ(duckType string) string {
	upper := strings.ToUpper(duckType)
	if bq, ok := m.duckToBQ[upper]; ok {
		return bq
	}
	return "STRING"
}

// FieldToDuckDBColumn converts a BigQuery FieldSchema to a DuckDB column definition string.
// Handles REPEATED mode (arrays), STRUCT/RECORD nested fields, and all base types.
//
// Examples:
//
//	FieldSchema{Name:"age", Type:"INT64", Mode:"REQUIRED"} -> "age BIGINT NOT NULL"
//	FieldSchema{Name:"tags", Type:"STRING", Mode:"REPEATED"} -> "tags VARCHAR[]"
//	FieldSchema{Name:"addr", Type:"STRUCT", Fields:[{Name:"city",Type:"STRING"}]} -> "addr STRUCT(city VARCHAR)"
func (m *TypeMapper) FieldToDuckDBColumn(field FieldSchema) string {
	typeDef := m.fieldTypeToDuckDB(field)

	// Handle REPEATED mode: wrap in array
	if strings.ToUpper(field.Mode) == ModeRepeated {
		return fmt.Sprintf("%s %s[]", field.Name, typeDef)
	}

	// Handle REQUIRED mode: add NOT NULL
	if strings.ToUpper(field.Mode) == ModeRequired {
		return fmt.Sprintf("%s %s NOT NULL", field.Name, typeDef)
	}

	// NULLABLE (default)
	return fmt.Sprintf("%s %s", field.Name, typeDef)
}

// fieldTypeToDuckDB converts a FieldSchema's type to a DuckDB type string,
// handling STRUCT/RECORD recursion.
func (m *TypeMapper) fieldTypeToDuckDB(field FieldSchema) string {
	upperType := strings.ToUpper(field.Type)

	// STRUCT/RECORD with nested fields
	if (upperType == BQStruct || upperType == BQRecord) && len(field.Fields) > 0 {
		parts := make([]string, len(field.Fields))
		for i, f := range field.Fields {
			innerType := m.fieldTypeToDuckDB(f)
			parts[i] = fmt.Sprintf("%s %s", f.Name, innerType)
		}
		return fmt.Sprintf("STRUCT(%s)", strings.Join(parts, ", "))
	}

	return m.BQToDuckDB(field.Type)
}

// SchemaToDuckDBColumns converts a full BigQuery TableSchema to DuckDB column definitions.
// Returns a slice of "name TYPE" strings suitable for CREATE TABLE.
func (m *TypeMapper) SchemaToDuckDBColumns(schema TableSchema) []string {
	if len(schema.Fields) == 0 {
		return []string{}
	}

	cols := make([]string, len(schema.Fields))
	for i, field := range schema.Fields {
		cols[i] = m.FieldToDuckDBColumn(field)
	}
	return cols
}
