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
	mergeHeaderRe = regexp.MustCompile(
		`(?is)MERGE\s+INTO\s+(\S+)(?:\s+AS\s+(\w+))?\s+` +
			`USING\s+(\(.+?\)|\S+)(?:\s+AS\s+(\w+))?\s+` +
			`ON\s+(.+?)(?:\s+WHEN\s)`)

	whenMatchedUpdateRe = regexp.MustCompile(
		`(?is)WHEN\s+MATCHED\s+(?:AND\s+.+?\s+)?THEN\s+UPDATE\s+SET\s+(.+?)(?:\s+WHEN\s|\s*$)`)

	whenNotMatchedInsertRe = regexp.MustCompile(
		`(?is)WHEN\s+NOT\s+MATCHED\s+(?:AND\s+.+?\s+)?THEN\s+INSERT\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)`)

	whenMatchedDeleteRe = regexp.MustCompile(
		`(?is)WHEN\s+MATCHED\s+(?:AND\s+.+?\s+)?THEN\s+DELETE`)
)

// TranslateMerge converts a BigQuery MERGE statement to DuckDB-compatible SQL.
// Uses UPDATE + INSERT WHERE NOT EXISTS instead of INSERT ON CONFLICT,
// since BigQuery tables don't have UNIQUE constraints.
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

	// Build source reference for FROM clauses
	sourceRef := source
	if sourceAlias != "" {
		sourceRef = source + " AS " + sourceAlias
	}

	var stmts []string

	// WHEN MATCHED THEN DELETE
	if hasMatchedDelete {
		deleteSQL := fmt.Sprintf(
			"DELETE FROM %s WHERE EXISTS (SELECT 1 FROM %s WHERE %s)",
			target, sourceRef, onCondition)
		if targetAlias != "" {
			aliasRe := regexp.MustCompile(`\b` + regexp.QuoteMeta(targetAlias) + `\.`)
			deleteSQL = aliasRe.ReplaceAllString(deleteSQL, target+".")
		}
		stmts = append(stmts, deleteSQL)
	}

	// WHEN MATCHED THEN UPDATE
	if hasMatchedUpdate {
		updateMatch := whenMatchedUpdateRe.FindStringSubmatch(sql)
		if updateMatch == nil {
			return nil, fmt.Errorf("cannot parse WHEN MATCHED UPDATE clause")
		}
		setClause := strings.TrimSpace(updateMatch[1])

		// UPDATE target SET ... FROM source WHERE on_condition
		updateSQL := fmt.Sprintf(
			"UPDATE %s SET %s FROM %s WHERE %s",
			target, setClause, sourceRef, onCondition)
		// Replace target alias with table name using word boundary
		if targetAlias != "" {
			aliasRe := regexp.MustCompile(`\b` + regexp.QuoteMeta(targetAlias) + `\.`)
			updateSQL = aliasRe.ReplaceAllString(updateSQL, target+".")
		}
		stmts = append(stmts, updateSQL)
	}

	// WHEN NOT MATCHED THEN INSERT
	if hasNotMatchedInsert {
		insertMatch := whenNotMatchedInsertRe.FindStringSubmatch(sql)
		if insertMatch == nil {
			return nil, fmt.Errorf("cannot parse WHEN NOT MATCHED INSERT clause")
		}
		insertCols := strings.TrimSpace(insertMatch[1])
		insertVals := strings.TrimSpace(insertMatch[2])

		// Build the NOT EXISTS condition by swapping alias references in onCondition
		notExistsCondition := onCondition
		if targetAlias != "" {
			aliasRe := regexp.MustCompile(`\b` + regexp.QuoteMeta(targetAlias) + `\.`)
			notExistsCondition = aliasRe.ReplaceAllString(notExistsCondition, target+".")
		}

		// INSERT INTO target (cols) SELECT vals FROM source
		// WHERE NOT EXISTS (SELECT 1 FROM target WHERE target.pk = source.pk)
		insertSQL := fmt.Sprintf(
			"INSERT INTO %s (%s) SELECT %s FROM %s WHERE NOT EXISTS (SELECT 1 FROM %s WHERE %s)",
			target, insertCols, insertVals, sourceRef, target, notExistsCondition)
		stmts = append(stmts, insertSQL)
	}

	if len(stmts) == 0 {
		return nil, fmt.Errorf("unsupported MERGE pattern: no WHEN clauses found")
	}

	return stmts, nil
}
