package query

// QueryResult holds the result of a query execution.
type QueryResult struct {
	Schema      []ColumnMeta    // Column metadata
	Rows        [][]interface{} // Row data
	TotalRows   uint64          // Total number of rows
	JobComplete bool            // Whether the query completed
}

// ColumnMeta describes a result column.
type ColumnMeta struct {
	Name string // Column name
	Type string // BigQuery type (e.g., "INT64", "STRING")
	Mode string // "NULLABLE", "REQUIRED", or "REPEATED" (for arrays)
}

// ExecResult holds the result of a non-query execution (DDL/DML).
type ExecResult struct {
	RowsAffected int64
}
