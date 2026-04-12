package query

import (
	"errors"
	"regexp"
	"strings"
)

// Compiled regex patterns at package level per Go best practices.
var (
	// backtickRe matches backtick-quoted identifiers like `project.dataset.table`
	backtickRe = regexp.MustCompile("`([^`]+)`")

	// optionsRe matches trailing OPTIONS(...) in DDL statements.
	// Uses a greedy match for the content inside OPTIONS() to handle nested parens in labels.
	optionsRe = regexp.MustCompile(`(?i)\s*OPTIONS\s*\((.+)\)\s*$`)

	// currentTimestampRe matches CURRENT_TIMESTAMP() with optional parens.
	currentTimestampRe = regexp.MustCompile(`(?i)\bCURRENT_TIMESTAMP\s*\(\s*\)`)

	// currentDateRe matches CURRENT_DATE() with optional parens.
	currentDateRe = regexp.MustCompile(`(?i)\bCURRENT_DATE\s*\(\s*\)`)

	// currentTimeRe matches CURRENT_TIME() with optional parens.
	currentTimeRe = regexp.MustCompile(`(?i)\bCURRENT_TIME\s*\(\s*\)`)

	// dateAddRe matches DATE_ADD(expr, INTERVAL n unit).
	dateAddRe = regexp.MustCompile(`(?i)\bDATE_ADD\s*\(\s*(.+?)\s*,\s*(INTERVAL\s+.+?)\s*\)`)

	// dateSubRe matches DATE_SUB(expr, INTERVAL n unit).
	dateSubRe = regexp.MustCompile(`(?i)\bDATE_SUB\s*\(\s*(.+?)\s*,\s*(INTERVAL\s+.+?)\s*\)`)

	// dateDiffRe matches DATE_DIFF(d1, d2, part).
	dateDiffRe = regexp.MustCompile(`(?i)\bDATE_DIFF\s*\(\s*(.+?)\s*,\s*(.+?)\s*,\s*(\w+)\s*\)`)

	// dateTruncRe matches DATE_TRUNC(expr, part).
	dateTruncRe = regexp.MustCompile(`(?i)\bDATE_TRUNC\s*\(\s*(.+?)\s*,\s*(\w+)\s*\)`)

	// timestampTruncRe matches TIMESTAMP_TRUNC(expr, part).
	timestampTruncRe = regexp.MustCompile(`(?i)\bTIMESTAMP_TRUNC\s*\(\s*(.+?)\s*,\s*(\w+)\s*\)`)

	// castTypeRe matches CAST(expr AS TYPE), TRY_CAST(expr AS TYPE), or SAFE_CAST(expr AS TYPE)
	// to translate BQ type names to DuckDB type names.
	castTypeRe = regexp.MustCompile(`(?i)\b(CAST|TRY_CAST|SAFE_CAST)\s*\(\s*(.+?)\s+AS\s+(\w+)\s*\)`)

	// optionKVRe matches key='value' pairs inside OPTIONS().
	optionKVRe = regexp.MustCompile(`(\w+)\s*=\s*'([^']*)'`)
)

// bqTypeToDuckDB maps BigQuery type names to DuckDB type names for CAST expressions.
var bqTypeToDuckDB = map[string]string{
	"INT64":      "BIGINT",
	"FLOAT64":    "DOUBLE",
	"BOOL":       "BOOLEAN",
	"STRING":     "VARCHAR",
	"BYTES":      "BLOB",
	"NUMERIC":    "DECIMAL(38,9)",
	"BIGNUMERIC": "DECIMAL(76,38)",
	"TIMESTAMP":  "TIMESTAMPTZ",
	"DATETIME":   "TIMESTAMP",
	"DATE":       "DATE",
	"TIME":       "TIME",
	"JSON":       "JSON",
	"GEOGRAPHY":  "VARCHAR",
	"INTERVAL":   "INTERVAL",
}

// Translator converts BigQuery SQL to DuckDB SQL.
type Translator struct {
	funcRegistry *FunctionRegistry
}

// NewTranslator creates a new SQL translator.
func NewTranslator() *Translator {
	return &Translator{
		funcRegistry: NewFunctionRegistry(),
	}
}

// Translate converts a BigQuery SQL string to DuckDB-compatible SQL.
// It performs these transformations in order:
//  1. Replace backtick-quoted identifiers with double-quoted identifiers
//  2. Strip OPTIONS(...) from DDL statements
//  3. Translate CURRENT_TIMESTAMP(), CURRENT_DATE(), CURRENT_TIME()
//  4. Translate DATE_ADD, DATE_SUB, DATE_DIFF, DATE_TRUNC, TIMESTAMP_TRUNC
//  5. Translate CAST/TRY_CAST type names
//  6. Translate BQ function calls via FunctionRegistry
func (t *Translator) Translate(sql string) (string, error) {
	if strings.TrimSpace(sql) == "" {
		return "", errors.New("empty SQL string")
	}

	result := sql

	// 1. Backtick-quoted identifiers -> double-quoted identifiers
	// Strip project prefix: `project.dataset.table` -> "dataset"."table"
	// DuckDB has no catalog matching the BQ project ID.
	result = backtickRe.ReplaceAllStringFunc(result, func(match string) string {
		inner := match[1 : len(match)-1]
		parts := strings.Split(inner, ".")
		// 3-part: project.dataset.table -> dataset.table
		if len(parts) == 3 {
			parts = parts[1:]
		}
		// 2-part inside backticks: if first part looks like a project ID
		// (contains hyphen — dataset names can't have hyphens), strip it
		if len(parts) == 2 && strings.Contains(parts[0], "-") {
			parts = parts[1:]
		}
		quoted := make([]string, len(parts))
		for i, p := range parts {
			quoted[i] = `"` + p + `"`
		}
		return strings.Join(quoted, ".")
	})

	// Also strip unquoted project-qualified identifiers:
	// "project-id"."dataset"."table" -> "dataset"."table"
	// This handles the output of backtick conversion when the project was
	// separately backtick-quoted: `project-id`.dataset.table
	result = stripProjectPrefix(result)

	// 2. Strip OPTIONS(...) from DDL
	result = optionsRe.ReplaceAllString(result, "")

	// 3. CURRENT_TIMESTAMP() -> current_timestamp
	result = currentTimestampRe.ReplaceAllString(result, "current_timestamp")

	// 4. CURRENT_DATE() -> current_date
	result = currentDateRe.ReplaceAllString(result, "current_date")

	// 5. CURRENT_TIME() -> current_time
	result = currentTimeRe.ReplaceAllString(result, "current_time")

	// 6. DATE_ADD(expr, INTERVAL n unit) -> (expr) + INTERVAL n unit
	result = dateAddRe.ReplaceAllString(result, "($1) + $2")

	// 7. DATE_SUB(expr, INTERVAL n unit) -> (expr) - INTERVAL n unit
	result = dateSubRe.ReplaceAllString(result, "($1) - $2")

	// 8. DATE_DIFF(d1, d2, part) -> date_diff('part', d2, d1) (note reversal)
	result = dateDiffRe.ReplaceAllString(result, "date_diff('$3', $2, $1)")

	// 9. DATE_TRUNC(expr, part) -> date_trunc('part', expr) (note reversal)
	result = dateTruncRe.ReplaceAllString(result, "date_trunc('$2', $1)")

	// 10. TIMESTAMP_TRUNC(expr, part) -> date_trunc('part', expr) (note reversal)
	result = timestampTruncRe.ReplaceAllString(result, "date_trunc('$2', $1)")

	// 11. CAST type names: CAST(x AS INT64) -> CAST(x AS BIGINT), etc.
	result = castTypeRe.ReplaceAllStringFunc(result, func(match string) string {
		sub := castTypeRe.FindStringSubmatch(match)
		if len(sub) != 4 {
			return match
		}
		castFunc := sub[1] // CAST or TRY_CAST
		expr := sub[2]
		bqType := strings.ToUpper(sub[3])

		if duckType, ok := bqTypeToDuckDB[bqType]; ok {
			return castFunc + "(" + expr + " AS " + duckType + ")"
		}
		return match
	})

	// 12. Function translations via registry
	result = t.translateFunctions(result)

	return result, nil
}

// TranslateAndExtractOptions translates SQL and returns any extracted OPTIONS separately.
// Returns (translatedSQL, optionsMap, error).
func (t *Translator) TranslateAndExtractOptions(sql string) (string, map[string]string, error) {
	if strings.TrimSpace(sql) == "" {
		return "", nil, errors.New("empty SQL string")
	}

	// Extract OPTIONS before full translation
	opts := make(map[string]string)
	optMatch := optionsRe.FindStringSubmatch(sql)
	if len(optMatch) >= 2 {
		optContent := optMatch[1]
		// Parse key='value' pairs
		kvMatches := optionKVRe.FindAllStringSubmatch(optContent, -1)
		for _, kv := range kvMatches {
			if len(kv) == 3 {
				opts[kv[1]] = kv[2]
			}
		}
	}

	translated, err := t.Translate(sql)
	if err != nil {
		return "", nil, err
	}

	return translated, opts, nil
}

// translateFunctions replaces BigQuery function calls with DuckDB equivalents
// using the function registry. Handles simple renames and handler-based translations.
func (t *Translator) translateFunctions(sql string) string {
	result := sql

	for bqName, translation := range t.funcRegistry.functions {
		// Build a case-insensitive pattern for the function name followed by (
		pattern := regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(bqName) + `\s*\(`)

		for {
			loc := pattern.FindStringIndex(result)
			if loc == nil {
				break
			}

			// Find the matching closing parenthesis
			openIdx := strings.Index(result[loc[0]:], "(")
			if openIdx < 0 {
				break
			}
			argsStart := loc[0] + openIdx + 1
			closeIdx := findMatchingParen(result, loc[0]+openIdx)
			if closeIdx < 0 {
				break
			}

			args := result[argsStart:closeIdx]
			var replacement string

			if translation.DuckDBName != "" {
				// Simple rename: replace function name, keep args
				replacement = translation.DuckDBName + "(" + args + ")"
			} else if translation.Handler != nil {
				// Handler-based: let handler produce the full replacement
				replacement = translation.Handler(args)
			} else {
				break // should not happen
			}

			result = result[:loc[0]] + replacement + result[closeIdx+1:]
		}
	}

	return result
}

// findMatchingParen finds the index of the closing parenthesis matching the
// open parenthesis at position openIdx. Returns -1 if not found.
// Respects nested parentheses and quoted strings.
func findMatchingParen(s string, openIdx int) int {
	depth := 0
	inSingleQuote := false
	inDoubleQuote := false

	for i := openIdx; i < len(s); i++ {
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
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// projectQualifiedRe matches a double-quoted project prefix (containing a hyphen)
// followed by a dot and the rest of the identifier (quoted or unquoted).
// Examples:
//   "test-project"."dataset"."table" -> "dataset"."table"
//   "test-project"."dataset" -> "dataset"
//   "test-project".dataset.table -> dataset.table
//   "test-project".dataset -> dataset
var projectQualifiedRe = regexp.MustCompile(`"([^"]*-[^"]*)"\s*\.`)

// stripProjectPrefix removes double-quoted project prefixes from identifiers.
func stripProjectPrefix(sql string) string {
	return projectQualifiedRe.ReplaceAllString(sql, "")
}
