package query

import (
	"strings"
	"unicode"
)

// StatementType represents the type of SQL statement.
type StatementType int

const (
	StatementUnknown     StatementType = iota
	StatementQuery                     // SELECT, WITH ... SELECT
	StatementDML                       // INSERT, UPDATE, DELETE, MERGE, TRUNCATE
	StatementDDLCreate                 // CREATE TABLE/VIEW/SCHEMA/FUNCTION
	StatementDDLDrop                   // DROP TABLE/VIEW/SCHEMA/FUNCTION
	StatementDDLAlter                  // ALTER TABLE/VIEW/SCHEMA
	StatementTransaction               // BEGIN, COMMIT, ROLLBACK
)

// String returns the string representation of the statement type.
func (s StatementType) String() string {
	switch s {
	case StatementQuery:
		return "QUERY"
	case StatementDML:
		return "DML"
	case StatementDDLCreate:
		return "DDL_CREATE"
	case StatementDDLDrop:
		return "DDL_DROP"
	case StatementDDLAlter:
		return "DDL_ALTER"
	case StatementTransaction:
		return "TRANSACTION"
	default:
		return "UNKNOWN"
	}
}

// ClassifyResult contains the classification of a SQL statement.
type ClassifyResult struct {
	Type    StatementType
	IsQuery bool // true if the statement returns rows
	IsDDL   bool // true if the statement modifies schema
	IsDML   bool // true if the statement modifies data
}

// ClassifySQL determines the type of a SQL statement by examining its leading keyword(s).
// It handles whitespace, comments (-- and /* */), and is case-insensitive.
func ClassifySQL(sql string) ClassifyResult {
	keyword := extractFirstKeyword(sql)
	if keyword == "" {
		return ClassifyResult{Type: StatementUnknown}
	}

	switch keyword {
	case "SELECT", "WITH":
		return ClassifyResult{Type: StatementQuery, IsQuery: true}
	case "INSERT", "UPDATE", "DELETE", "MERGE", "TRUNCATE":
		return ClassifyResult{Type: StatementDML, IsDML: true}
	case "CREATE":
		return ClassifyResult{Type: StatementDDLCreate, IsDDL: true}
	case "DROP":
		return ClassifyResult{Type: StatementDDLDrop, IsDDL: true}
	case "ALTER":
		return ClassifyResult{Type: StatementDDLAlter, IsDDL: true}
	case "BEGIN", "COMMIT", "ROLLBACK":
		return ClassifyResult{Type: StatementTransaction}
	default:
		return ClassifyResult{Type: StatementUnknown}
	}
}

// extractFirstKeyword strips leading whitespace and comments, then returns the
// first keyword uppercased. Returns "" if no keyword is found.
func extractFirstKeyword(sql string) string {
	s := sql
	for {
		// Trim leading whitespace.
		s = strings.TrimLeftFunc(s, unicode.IsSpace)
		if len(s) == 0 {
			return ""
		}

		// Skip line comments (-- ...\n).
		if strings.HasPrefix(s, "--") {
			idx := strings.IndexByte(s, '\n')
			if idx == -1 {
				// Comment extends to end of string, no keyword.
				return ""
			}
			s = s[idx+1:]
			continue
		}

		// Skip block comments (/* ... */).
		if strings.HasPrefix(s, "/*") {
			idx := strings.Index(s[2:], "*/")
			if idx == -1 {
				// Unclosed block comment, no keyword.
				return ""
			}
			s = s[idx+4:] // skip past the closing */
			continue
		}

		// No more whitespace or comments; extract the keyword.
		break
	}

	// Extract the first word (sequence of non-space characters up to a space or end).
	end := strings.IndexFunc(s, unicode.IsSpace)
	if end == -1 {
		return strings.ToUpper(s)
	}
	return strings.ToUpper(s[:end])
}
