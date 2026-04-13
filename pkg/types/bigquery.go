package types

// BigQuery type constants — REST API names (what the SDKs expect in schema responses).
// The Go SDK read path does direct string match: INTEGER, FLOAT, BOOLEAN, RECORD.
// Standard SQL aliases (INT64, FLOAT64, BOOL, STRUCT) are accepted on the WRITE path
// but must NOT be returned in schema responses.
const (
	BQInt64      = "INTEGER"  // REST API name (not "INT64")
	BQFloat64    = "FLOAT"    // REST API name (not "FLOAT64")
	BQNumeric    = "NUMERIC"
	BQBigNumeric = "BIGNUMERIC"
	BQBool       = "BOOLEAN"  // REST API name (not "BOOL")
	BQString     = "STRING"
	BQBytes      = "BYTES"
	BQDate       = "DATE"
	BQTime       = "TIME"
	BQTimestamp  = "TIMESTAMP"
	BQDatetime   = "DATETIME"
	BQGeography  = "GEOGRAPHY"
	BQJson       = "JSON"
	BQArray      = "ARRAY"
	BQStruct     = "RECORD"   // REST API name (not "STRUCT")
	BQRecord     = "RECORD"
	BQInterval   = "INTERVAL"
)

// Field modes control nullability and array behavior.
const (
	ModeNullable = "NULLABLE"
	ModeRequired = "REQUIRED"
	ModeRepeated = "REPEATED" // This means ARRAY in BigQuery
)

// FieldSchema represents a BigQuery table column definition.
type FieldSchema struct {
	Name        string         `json:"name"`
	Type        string         `json:"type"`
	Mode        string         `json:"mode,omitempty"`        // NULLABLE, REQUIRED, REPEATED
	Description string         `json:"description,omitempty"`
	Fields      []FieldSchema  `json:"fields,omitempty"`      // For STRUCT/RECORD nested fields
	PolicyTags  *PolicyTagList `json:"policyTags,omitempty"`  // For column-level security
}

// PolicyTagList holds policy tags for column-level security.
type PolicyTagList struct {
	Names []string `json:"names,omitempty"`
}

// TableSchema represents the schema of a BigQuery table.
type TableSchema struct {
	Fields []FieldSchema `json:"fields"`
}

// ColumnMeta describes a result column with its name and BigQuery type.
// Used by BuildArrowRecord to build Arrow record batches from query results.
type ColumnMeta struct {
	Name string // Column name
	Type string // BigQuery type (e.g., "INT64", "STRING")
}
