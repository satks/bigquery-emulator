package query

import (
	"fmt"
	"strings"
)

// FunctionTranslation defines how a BigQuery function maps to DuckDB.
type FunctionTranslation struct {
	// DuckDBName is the simple renamed function (e.g., IFNULL -> COALESCE).
	// If empty, Handler is used instead.
	DuckDBName string

	// Handler is a custom translation function for complex cases.
	// Receives the original function arguments string and returns the full DuckDB equivalent.
	// Only used if DuckDBName is empty.
	Handler func(args string) string
}

// FunctionRegistry maps BigQuery function names to their DuckDB translations.
type FunctionRegistry struct {
	functions map[string]FunctionTranslation
}

// NewFunctionRegistry creates a registry with all known BQ->DuckDB function mappings.
func NewFunctionRegistry() *FunctionRegistry {
	r := &FunctionRegistry{
		functions: make(map[string]FunctionTranslation),
	}

	// Simple renames: BQ function name -> DuckDB function name
	simpleRenames := map[string]string{
		"IFNULL":          "COALESCE",
		"ARRAY_AGG":       "list",
		"ARRAY_LENGTH":    "len",
		"GENERATE_UUID":   "uuid",
		"SAFE_CAST":       "TRY_CAST",
		"REGEXP_CONTAINS": "regexp_matches",
		"REGEXP_EXTRACT":  "regexp_extract",
		"REGEXP_REPLACE":  "regexp_replace",
		"GENERATE_ARRAY":  "generate_series",
		"STARTS_WITH":     "starts_with",
		"ENDS_WITH":       "suffix",
		"BYTE_LENGTH":     "octet_length",
		"CHAR_LENGTH":     "length",
		"ST_GEOGPOINT":    "ST_Point",
	}

	for bq, duck := range simpleRenames {
		r.functions[strings.ToUpper(bq)] = FunctionTranslation{DuckDBName: duck}
	}

	// Handler-based translations for complex cases

	// TO_JSON_STRING(x) -> to_json(x)::VARCHAR
	r.functions["TO_JSON_STRING"] = FunctionTranslation{
		Handler: func(args string) string {
			return fmt.Sprintf("to_json(%s)::VARCHAR", strings.TrimSpace(args))
		},
	}

	// SAFE_DIVIDE(a, b) -> (CASE WHEN (b) = 0 THEN NULL ELSE (a) / (b) END)
	r.functions["SAFE_DIVIDE"] = FunctionTranslation{
		Handler: func(args string) string {
			parts := splitArgs(args)
			if len(parts) != 2 {
				return fmt.Sprintf("SAFE_DIVIDE(%s)", args) // fallback
			}
			a := strings.TrimSpace(parts[0])
			b := strings.TrimSpace(parts[1])
			return fmt.Sprintf("(CASE WHEN (%s) = 0 THEN NULL ELSE (%s) / (%s) END)", b, a, b)
		},
	}

	// FORMAT_DATE(fmt, d) -> strftime(d, fmt)
	r.functions["FORMAT_DATE"] = FunctionTranslation{
		Handler: func(args string) string {
			parts := splitArgs(args)
			if len(parts) != 2 {
				return fmt.Sprintf("FORMAT_DATE(%s)", args)
			}
			fmtStr := strings.TrimSpace(parts[0])
			dateExpr := strings.TrimSpace(parts[1])
			return fmt.Sprintf("strftime(%s, %s)", dateExpr, fmtStr)
		},
	}

	// FORMAT_TIMESTAMP(fmt, t) -> strftime(t, fmt)
	r.functions["FORMAT_TIMESTAMP"] = FunctionTranslation{
		Handler: func(args string) string {
			parts := splitArgs(args)
			if len(parts) != 2 {
				return fmt.Sprintf("FORMAT_TIMESTAMP(%s)", args)
			}
			fmtStr := strings.TrimSpace(parts[0])
			tsExpr := strings.TrimSpace(parts[1])
			return fmt.Sprintf("strftime(%s, %s)", tsExpr, fmtStr)
		},
	}

	// PARSE_DATE(fmt, s) -> strptime(s, fmt)::DATE
	r.functions["PARSE_DATE"] = FunctionTranslation{
		Handler: func(args string) string {
			parts := splitArgs(args)
			if len(parts) != 2 {
				return fmt.Sprintf("PARSE_DATE(%s)", args)
			}
			fmtStr := strings.TrimSpace(parts[0])
			strExpr := strings.TrimSpace(parts[1])
			return fmt.Sprintf("strptime(%s, %s)::DATE", strExpr, fmtStr)
		},
	}

	// PARSE_TIMESTAMP(fmt, s) -> strptime(s, fmt)::TIMESTAMPTZ
	r.functions["PARSE_TIMESTAMP"] = FunctionTranslation{
		Handler: func(args string) string {
			parts := splitArgs(args)
			if len(parts) != 2 {
				return fmt.Sprintf("PARSE_TIMESTAMP(%s)", args)
			}
			fmtStr := strings.TrimSpace(parts[0])
			strExpr := strings.TrimSpace(parts[1])
			return fmt.Sprintf("strptime(%s, %s)::TIMESTAMPTZ", strExpr, fmtStr)
		},
	}

	return r
}

// Get returns the translation for a function, and whether it exists.
// Lookup is case-insensitive.
func (r *FunctionRegistry) Get(name string) (FunctionTranslation, bool) {
	tr, ok := r.functions[strings.ToUpper(name)]
	return tr, ok
}

// RegisteredCount returns the number of registered function translations.
func (r *FunctionRegistry) RegisteredCount() int {
	return len(r.functions)
}

// splitArgs splits a comma-separated argument string, respecting parentheses nesting
// and quoted strings. For example: "'%Y', my_col" -> ["'%Y'", "my_col"]
func splitArgs(args string) []string {
	var parts []string
	depth := 0
	inSingleQuote := false
	inDoubleQuote := false
	start := 0

	for i := 0; i < len(args); i++ {
		ch := args[i]
		switch {
		case ch == '\'' && !inDoubleQuote:
			inSingleQuote = !inSingleQuote
		case ch == '"' && !inSingleQuote:
			inDoubleQuote = !inDoubleQuote
		case ch == '(' && !inSingleQuote && !inDoubleQuote:
			depth++
		case ch == ')' && !inSingleQuote && !inDoubleQuote:
			depth--
		case ch == ',' && depth == 0 && !inSingleQuote && !inDoubleQuote:
			parts = append(parts, args[start:i])
			start = i + 1
		}
	}
	parts = append(parts, args[start:])
	return parts
}
