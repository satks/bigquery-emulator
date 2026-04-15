package query

import (
	"fmt"
	"regexp"
	"strings"
)

// MERGE INTO target [AS t]
// USING (source) [AS s]
// ON condition
// WHEN MATCHED [AND condition] THEN UPDATE SET col = val, ...
// WHEN NOT MATCHED [AND condition] THEN INSERT (cols) VALUES (vals)
// WHEN MATCHED [AND condition] THEN DELETE

var (
	// mergeHeaderRe extracts: target alias, source, source alias, ON condition
	mergeHeaderRe = regexp.MustCompile(
		`(?is)MERGE\s+INTO\s+(\S+)(?:\s+AS\s+(\w+))?\s+` +
			`USING\s+(\(.+?\)|\S+)(?:\s+AS\s+(\w+))?\s+` +
			`ON\s+(.+?)(?:\s+WHEN\s)`)

	// whenMatchedUpdateRe extracts SET clause from WHEN MATCHED THEN UPDATE
	whenMatchedUpdateRe = regexp.MustCompile(
		`(?is)WHEN\s+MATCHED\s+(?:AND\s+.+?\s+)?THEN\s+UPDATE\s+SET\s+(.+?)(?:\s+WHEN\s|\s*$)`)

	// whenNotMatchedInsertRe extracts column list and values from WHEN NOT MATCHED THEN INSERT
	whenNotMatchedInsertRe = regexp.MustCompile(
		`(?is)WHEN\s+NOT\s+MATCHED\s+(?:AND\s+.+?\s+)?THEN\s+INSERT\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)`)

	// whenMatchedDeleteRe detects WHEN MATCHED THEN DELETE
	whenMatchedDeleteRe = regexp.MustCompile(
		`(?is)WHEN\s+MATCHED\s+(?:AND\s+.+?\s+)?THEN\s+DELETE`)
)

// TranslateMerge converts a BigQuery MERGE statement to DuckDB-compatible SQL.
// Returns one or more SQL statements to execute in sequence.
func TranslateMerge(sql string) ([]string, error) {
	header := mergeHeaderRe.FindStringSubmatch(sql)
	if header == nil {
		return nil, fmt.Errorf("cannot parse MERGE statement")
	}

	target := header[1]
	targetAlias := header[2] // may be empty
	source := header[3]
	sourceAlias := header[4] // may be empty
	onCondition := strings.TrimSpace(header[5])

	hasMatchedUpdate := whenMatchedUpdateRe.MatchString(sql)
	hasNotMatchedInsert := whenNotMatchedInsertRe.MatchString(sql)
	hasMatchedDelete := whenMatchedDeleteRe.MatchString(sql)

	// Determine the conflict column from the ON condition.
	// Common pattern: t.pk = s.pk -> conflict on pk
	conflictCol := extractConflictColumn(onCondition, targetAlias)

	var stmts []string

	// WHEN MATCHED THEN DELETE
	if hasMatchedDelete && !hasMatchedUpdate && !hasNotMatchedInsert {
		// Simple delete-merge: DELETE rows from target that match source
		deleteSQL := fmt.Sprintf(
			"DELETE FROM %s WHERE %s IN (SELECT %s FROM %s%s)",
			target, conflictCol, conflictCol, source, aliasClause(sourceAlias))
		stmts = append(stmts, deleteSQL)
		return stmts, nil
	}

	// WHEN MATCHED THEN UPDATE + WHEN NOT MATCHED THEN INSERT
	// -> INSERT ... ON CONFLICT DO UPDATE
	if hasNotMatchedInsert {
		insertMatch := whenNotMatchedInsertRe.FindStringSubmatch(sql)
		if insertMatch == nil {
			return nil, fmt.Errorf("cannot parse WHEN NOT MATCHED INSERT clause")
		}
		insertCols := strings.TrimSpace(insertMatch[1])
		insertVals := strings.TrimSpace(insertMatch[2])

		// Build the source subquery
		sourceQuery := source
		if sourceAlias != "" {
			sourceQuery = source + " AS " + sourceAlias
		}

		if hasMatchedUpdate {
			updateMatch := whenMatchedUpdateRe.FindStringSubmatch(sql)
			if updateMatch == nil {
				return nil, fmt.Errorf("cannot parse WHEN MATCHED UPDATE clause")
			}
			setClause := strings.TrimSpace(updateMatch[1])

			// Convert SET clause: replace source alias references with EXCLUDED
			setClause = replaceAliasWithExcluded(setClause, sourceAlias)

			// INSERT INTO target (cols) SELECT vals FROM source ON CONFLICT (pk) DO UPDATE SET ...
			upsertSQL := fmt.Sprintf(
				"INSERT INTO %s (%s) SELECT %s FROM %s ON CONFLICT (%s) DO UPDATE SET %s",
				target, insertCols, insertVals, sourceQuery, conflictCol, setClause)
			stmts = append(stmts, upsertSQL)
		} else {
			// INSERT-only merge (WHEN NOT MATCHED only)
			insertSQL := fmt.Sprintf(
				"INSERT INTO %s (%s) SELECT %s FROM %s ON CONFLICT (%s) DO NOTHING",
				target, insertCols, insertVals, sourceQuery, conflictCol)
			stmts = append(stmts, insertSQL)
		}
		return stmts, nil
	}

	// WHEN MATCHED THEN UPDATE only (no insert)
	if hasMatchedUpdate {
		updateMatch := whenMatchedUpdateRe.FindStringSubmatch(sql)
		if updateMatch == nil {
			return nil, fmt.Errorf("cannot parse WHEN MATCHED UPDATE clause")
		}
		setClause := strings.TrimSpace(updateMatch[1])

		sourceQuery := source
		if sourceAlias != "" {
			sourceQuery = source + " AS " + sourceAlias
		}

		// UPDATE target SET ... FROM source WHERE condition
		updateSQL := fmt.Sprintf(
			"UPDATE %s SET %s FROM %s WHERE %s",
			target, setClause, sourceQuery, onCondition)

		// Replace target alias with table name in the update
		if targetAlias != "" {
			updateSQL = strings.ReplaceAll(updateSQL, targetAlias+".", target+".")
		}

		stmts = append(stmts, updateSQL)
		return stmts, nil
	}

	return nil, fmt.Errorf("unsupported MERGE pattern")
}

// extractConflictColumn extracts the column name from an ON condition like "t.pk = s.pk"
func extractConflictColumn(onCondition, targetAlias string) string {
	// Try to extract from pattern: alias.column = ...
	parts := strings.SplitN(onCondition, "=", 2)
	if len(parts) != 2 {
		return "id" // fallback
	}

	leftSide := strings.TrimSpace(parts[0])
	// Strip alias prefix
	if targetAlias != "" && strings.HasPrefix(leftSide, targetAlias+".") {
		return strings.TrimPrefix(leftSide, targetAlias+".")
	}

	// Try to extract column from dotted name
	dotParts := strings.SplitN(leftSide, ".", 2)
	if len(dotParts) == 2 {
		return dotParts[1]
	}

	return leftSide
}

// replaceAliasWithExcluded replaces source alias references (s.col) with EXCLUDED.col
// in a SET clause, which is what DuckDB's ON CONFLICT expects.
func replaceAliasWithExcluded(setClause, sourceAlias string) string {
	if sourceAlias == "" {
		return setClause
	}
	// Replace s.col with EXCLUDED.col
	re := regexp.MustCompile(`\b` + regexp.QuoteMeta(sourceAlias) + `\.(\w+)`)
	return re.ReplaceAllString(setClause, "EXCLUDED.$1")
}

func aliasClause(alias string) string {
	if alias == "" {
		return ""
	}
	return " AS " + alias
}
