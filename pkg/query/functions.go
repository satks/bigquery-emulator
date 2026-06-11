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
		"COUNTIF":         "count_if",
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

	// TIMESTAMP('x') -> TIMESTAMPTZ 'x' (BQ literal constructor)
	r.functions["TIMESTAMP"] = FunctionTranslation{
		Handler: func(args string) string {
			arg := strings.TrimSpace(args)
			// If the argument is a quoted string literal, use DuckDB's typed literal syntax
			if len(arg) >= 2 && arg[0] == '\'' && arg[len(arg)-1] == '\'' {
				return fmt.Sprintf("TIMESTAMPTZ %s", arg)
			}
			// For expressions (not literals), use a CAST
			return fmt.Sprintf("CAST(%s AS TIMESTAMPTZ)", arg)
		},
	}

	// DATE('x') -> DATE 'x' (BQ literal constructor)
	r.functions["DATE"] = FunctionTranslation{
		Handler: func(args string) string {
			arg := strings.TrimSpace(args)
			if len(arg) >= 2 && arg[0] == '\'' && arg[len(arg)-1] == '\'' {
				return fmt.Sprintf("DATE %s", arg)
			}
			return fmt.Sprintf("CAST(%s AS DATE)", arg)
		},
	}

	// MD5/SHA1/SHA256: BQ returns BYTES; DuckDB's functions return hex
	// VARCHAR. Wrap with unhex() so the result is a BLOB — JSON encoding
	// then yields base64 like BQ, and TO_BASE64 binds. SHA512 is not in
	// DuckDB core and stays unmapped.
	for bq, duck := range map[string]string{
		"MD5":    "md5",
		"SHA1":   "sha1",
		"SHA256": "sha256",
	} {
		duckName := duck
		r.functions[bq] = FunctionTranslation{
			Handler: func(args string) string {
				return fmt.Sprintf("unhex(%s(%s))", duckName, strings.TrimSpace(args))
			},
		}
	}

	// STRUCT(e1 AS n1, e2 AS n2) -> {'n1': e1, 'n2': e2}
	// DuckDB doesn't allow AS inside function args; rewrite to a struct literal.
	r.functions["STRUCT"] = FunctionTranslation{
		Handler: structHandler,
	}

	// TIME('x') -> TIME 'x' (BQ literal constructor)
	r.functions["TIME"] = FunctionTranslation{
		Handler: func(args string) string {
			arg := strings.TrimSpace(args)
			if len(arg) >= 2 && arg[0] == '\'' && arg[len(arg)-1] == '\'' {
				return fmt.Sprintf("TIME %s", arg)
			}
			return fmt.Sprintf("CAST(%s AS TIME)", arg)
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

// structHandler rewrites BQ STRUCT(expr AS name, ...) into a DuckDB struct
// literal {'name': expr, ...}. Unnamed args take the trailing identifier
// segment as the field name (BQ behavior), or a synthesized fN_ for plain
// expressions. Must never emit text containing "STRUCT(" — the registry
// rewrite loop would match its own output.
func structHandler(args string) string {
	parts := splitArgs(args)
	fields := make([]string, len(parts))
	for i, part := range parts {
		expr, name, ok := splitTopLevelAs(part)
		if !ok {
			expr = strings.TrimSpace(part)
			name = implicitFieldName(expr, i)
		}
		fields[i] = fmt.Sprintf("'%s': %s", name, expr)
	}
	return "{" + strings.Join(fields, ", ") + "}"
}

// splitTopLevelAs splits an expression on its last top-level AS keyword
// (depth 0, outside quotes), so CAST(x AS BIGINT) AS y splits at the outer AS
// and the inner one is left alone. Returns ok=false if no top-level AS exists.
func splitTopLevelAs(s string) (expr, name string, ok bool) {
	depth := 0
	inSingleQuote := false
	inDoubleQuote := false
	asIdx := -1

	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch {
		case ch == '\'' && !inDoubleQuote:
			inSingleQuote = !inSingleQuote
		case ch == '"' && !inSingleQuote:
			inDoubleQuote = !inDoubleQuote
		case ch == '(' && !inSingleQuote && !inDoubleQuote:
			depth++
		case ch == ')' && !inSingleQuote && !inDoubleQuote:
			depth--
		case (ch == 'A' || ch == 'a') && depth == 0 && !inSingleQuote && !inDoubleQuote:
			if i > 0 && i+2 < len(s) &&
				isSpaceByte(s[i-1]) && isSpaceByte(s[i+2]) &&
				(s[i+1] == 'S' || s[i+1] == 's') {
				asIdx = i
			}
		}
	}

	if asIdx < 0 {
		return "", "", false
	}
	expr = strings.TrimSpace(s[:asIdx])
	name = strings.Trim(strings.TrimSpace(s[asIdx+2:]), `"`)
	if expr == "" || name == "" {
		return "", "", false
	}
	return expr, name, true
}

// implicitFieldName derives a struct field name from an unnamed expression:
// the trailing identifier segment of a (possibly dotted) column reference,
// or a synthesized fN_ name for anything else.
func implicitFieldName(expr string, idx int) string {
	candidate := expr
	if dot := strings.LastIndex(candidate, "."); dot >= 0 {
		candidate = candidate[dot+1:]
	}
	candidate = strings.Trim(candidate, `"`)
	if isIdentifier(candidate) {
		return candidate
	}
	return fmt.Sprintf("f%d_", idx)
}

// isIdentifier reports whether s is a plain SQL identifier.
func isIdentifier(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		isAlpha := c == '_' || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
		if i == 0 && !isAlpha {
			return false
		}
		if !isAlpha && !(c >= '0' && c <= '9') {
			return false
		}
	}
	return true
}

// isSpaceByte reports whether c is an ASCII whitespace byte.
func isSpaceByte(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
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
