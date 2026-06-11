package query

import (
	"fmt"
	"regexp"
	"strings"
)

// MERGE INTO target [[AS] t]
// USING (source) | source_table [[AS] s]
// ON condition
// WHEN MATCHED [AND condition] THEN UPDATE SET col = val, ...
// WHEN NOT MATCHED [BY TARGET] [AND condition] THEN INSERT (cols) VALUES (vals)
// WHEN MATCHED [AND condition] THEN DELETE
//
// Parsing is token-aware (paren/quote depth) rather than one big regex:
// real-world MERGE statements have aliases without AS, USING subqueries with
// nested parens (joins, QUALIFY, window functions), and INSERT values with
// function calls.

var (
	mergeIntoRe = regexp.MustCompile(`(?is)^\s*MERGE\s+INTO\s+`)

	whenMatchedUpdateSegRe = regexp.MustCompile(
		`(?is)^WHEN\s+MATCHED\s+(?:AND\s+(.+?)\s+)?THEN\s+UPDATE\s+SET\s+(.+)$`)

	whenMatchedDeleteSegRe = regexp.MustCompile(
		`(?is)^WHEN\s+MATCHED\s+(?:AND\s+(.+?)\s+)?THEN\s+DELETE\s*$`)

	whenNotMatchedInsertSegRe = regexp.MustCompile(
		`(?is)^WHEN\s+NOT\s+MATCHED\s+(?:BY\s+TARGET\s+)?(?:AND\s+(.+?)\s+)?THEN\s+INSERT\b`)
)

// TranslateMerge converts a BigQuery MERGE statement to DuckDB-compatible SQL.
// Uses UPDATE + INSERT WHERE NOT EXISTS instead of INSERT ON CONFLICT,
// since BigQuery tables don't have UNIQUE constraints (and the bundled DuckDB
// has no native MERGE). Returns one or more SQL statements to execute in sequence.
func TranslateMerge(sql string) ([]string, error) {
	s := strings.TrimSpace(sql)
	s = strings.TrimSuffix(s, ";")

	loc := mergeIntoRe.FindStringIndex(s)
	if loc == nil {
		return nil, fmt.Errorf("cannot parse MERGE statement: expected MERGE INTO")
	}
	pos := loc[1]

	// Target table, optional [AS] alias
	target, pos, err := readMergeToken(s, pos)
	if err != nil {
		return nil, fmt.Errorf("cannot parse MERGE statement: target: %w", err)
	}
	targetAlias := ""
	tok, next, err := readMergeToken(s, pos)
	if err != nil {
		return nil, fmt.Errorf("cannot parse MERGE statement: after target: %w", err)
	}
	if strings.EqualFold(tok, "AS") {
		if targetAlias, next, err = readMergeToken(s, next); err != nil {
			return nil, fmt.Errorf("cannot parse MERGE statement: target alias: %w", err)
		}
		pos = next
		if tok, next, err = readMergeToken(s, pos); err != nil {
			return nil, fmt.Errorf("cannot parse MERGE statement: expected USING: %w", err)
		}
	} else if !strings.EqualFold(tok, "USING") {
		targetAlias = tok
		pos = next
		if tok, next, err = readMergeToken(s, pos); err != nil {
			return nil, fmt.Errorf("cannot parse MERGE statement: expected USING: %w", err)
		}
	}
	if !strings.EqualFold(tok, "USING") {
		return nil, fmt.Errorf("cannot parse MERGE statement: expected USING, got %q", tok)
	}
	pos = next

	// Source: parenthesized subquery (paren-matched) or table name, optional [AS] alias
	source, pos, err := readMergeToken(s, pos)
	if err != nil {
		return nil, fmt.Errorf("cannot parse MERGE statement: source: %w", err)
	}
	sourceAlias := ""
	if tok, next, err = readMergeToken(s, pos); err != nil {
		return nil, fmt.Errorf("cannot parse MERGE statement: expected ON: %w", err)
	}
	if strings.EqualFold(tok, "AS") {
		if sourceAlias, next, err = readMergeToken(s, next); err != nil {
			return nil, fmt.Errorf("cannot parse MERGE statement: source alias: %w", err)
		}
		pos = next
		if tok, next, err = readMergeToken(s, pos); err != nil {
			return nil, fmt.Errorf("cannot parse MERGE statement: expected ON: %w", err)
		}
	} else if !strings.EqualFold(tok, "ON") {
		sourceAlias = tok
		pos = next
		if tok, next, err = readMergeToken(s, pos); err != nil {
			return nil, fmt.Errorf("cannot parse MERGE statement: expected ON: %w", err)
		}
	}
	if !strings.EqualFold(tok, "ON") {
		return nil, fmt.Errorf("cannot parse MERGE statement: expected ON, got %q", tok)
	}
	pos = next

	// ON condition runs until the first top-level WHEN
	firstWhen := findTopLevelKeyword(s, pos, "WHEN")
	if firstWhen < 0 {
		return nil, fmt.Errorf("cannot parse MERGE statement: no WHEN clauses found")
	}
	onCondition := strings.TrimSpace(s[pos:firstWhen])

	// Split the remainder into top-level WHEN segments
	var segments []string
	segStart := firstWhen
	for {
		nextWhen := findTopLevelKeyword(s, segStart+4, "WHEN")
		if nextWhen < 0 {
			segments = append(segments, strings.TrimSpace(s[segStart:]))
			break
		}
		segments = append(segments, strings.TrimSpace(s[segStart:nextWhen]))
		segStart = nextWhen
	}

	// Build source reference for FROM clauses
	sourceRef := source
	if sourceAlias != "" {
		sourceRef = source + " AS " + sourceAlias
	}

	// replaceTargetAlias rewrites "alias." references to the real table name.
	replaceTargetAlias := func(expr string) string {
		if targetAlias == "" {
			return expr
		}
		aliasRe := regexp.MustCompile(`\b` + regexp.QuoteMeta(targetAlias) + `\.`)
		return aliasRe.ReplaceAllString(expr, target+".")
	}

	var deletes, updates, inserts []string

	for _, seg := range segments {
		switch {
		case whenMatchedDeleteSegRe.MatchString(seg):
			m := whenMatchedDeleteSegRe.FindStringSubmatch(seg)
			cond := onCondition
			if strings.TrimSpace(m[1]) != "" {
				cond += " AND (" + strings.TrimSpace(m[1]) + ")"
			}
			deletes = append(deletes, replaceTargetAlias(fmt.Sprintf(
				"DELETE FROM %s WHERE EXISTS (SELECT 1 FROM %s WHERE %s)",
				target, sourceRef, cond)))

		case whenMatchedUpdateSegRe.MatchString(seg):
			m := whenMatchedUpdateSegRe.FindStringSubmatch(seg)
			setClause := strings.TrimSpace(m[2])
			cond := onCondition
			if strings.TrimSpace(m[1]) != "" {
				cond += " AND (" + strings.TrimSpace(m[1]) + ")"
			}
			updates = append(updates, replaceTargetAlias(fmt.Sprintf(
				"UPDATE %s SET %s FROM %s WHERE %s",
				target, setClause, sourceRef, cond)))

		case whenNotMatchedInsertSegRe.MatchString(seg):
			m := whenNotMatchedInsertSegRe.FindStringSubmatch(seg)
			insertCols, insertVals, perr := parseInsertColsVals(seg)
			if perr != nil {
				return nil, fmt.Errorf("cannot parse WHEN NOT MATCHED INSERT clause: %w", perr)
			}
			notExists := replaceTargetAlias(onCondition)
			srcWhere := fmt.Sprintf("NOT EXISTS (SELECT 1 FROM %s WHERE %s)", target, notExists)
			if strings.TrimSpace(m[1]) != "" {
				srcWhere += " AND (" + strings.TrimSpace(m[1]) + ")"
			}
			inserts = append(inserts, fmt.Sprintf(
				"INSERT INTO %s (%s) SELECT %s FROM %s WHERE %s",
				target, insertCols, insertVals, sourceRef, srcWhere))

		default:
			return nil, fmt.Errorf("unsupported MERGE WHEN clause: %q", truncateForError(seg))
		}
	}

	stmts := append(append(deletes, updates...), inserts...)
	if len(stmts) == 0 {
		return nil, fmt.Errorf("unsupported MERGE pattern: no WHEN clauses found")
	}
	return stmts, nil
}

// readMergeToken reads the next token starting at pos: a parenthesized block
// (matched across nesting and quotes) or a run of non-space characters.
// Returns the token and the position after it.
func readMergeToken(s string, pos int) (string, int, error) {
	for pos < len(s) && isSpaceByte(s[pos]) {
		pos++
	}
	if pos >= len(s) {
		return "", pos, fmt.Errorf("unexpected end of statement")
	}
	if s[pos] == '(' {
		close := findMatchingParen(s, pos)
		if close < 0 {
			return "", pos, fmt.Errorf("unbalanced parentheses")
		}
		return s[pos : close+1], close + 1, nil
	}
	start := pos
	for pos < len(s) && !isSpaceByte(s[pos]) && s[pos] != '(' {
		pos++
	}
	return s[start:pos], pos, nil
}

// findTopLevelKeyword returns the index of the first occurrence of keyword at
// paren depth 0 (outside quotes), matched on word boundaries. Returns -1 if
// not found.
func findTopLevelKeyword(s string, from int, keyword string) int {
	depth := 0
	inSingleQuote := false
	inDoubleQuote := false
	kw := strings.ToUpper(keyword)

	for i := from; i < len(s); i++ {
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
		default:
			if depth == 0 && !inSingleQuote && !inDoubleQuote &&
				i+len(kw) <= len(s) &&
				strings.EqualFold(s[i:i+len(kw)], kw) &&
				(i == 0 || !isWordChar(s[i-1])) &&
				(i+len(kw) == len(s) || !isWordChar(s[i+len(kw)])) {
				return i
			}
		}
	}
	return -1
}

// parseInsertColsVals extracts the column list and VALUES list from a
// WHEN NOT MATCHED ... INSERT (cols) VALUES (vals) segment, tolerating nested
// parens (function calls) inside the values.
func parseInsertColsVals(seg string) (string, string, error) {
	colsOpen := strings.Index(seg, "(")
	if colsOpen < 0 {
		return "", "", fmt.Errorf("missing column list")
	}
	colsClose := findMatchingParen(seg, colsOpen)
	if colsClose < 0 {
		return "", "", fmt.Errorf("unbalanced column list")
	}
	rest := seg[colsClose+1:]
	valuesIdx := findTopLevelKeyword(rest, 0, "VALUES")
	if valuesIdx < 0 {
		return "", "", fmt.Errorf("missing VALUES")
	}
	valsOpen := strings.Index(rest[valuesIdx:], "(")
	if valsOpen < 0 {
		return "", "", fmt.Errorf("missing VALUES list")
	}
	valsOpen += valuesIdx
	valsClose := findMatchingParen(rest, valsOpen)
	if valsClose < 0 {
		return "", "", fmt.Errorf("unbalanced VALUES list")
	}
	cols := strings.TrimSpace(seg[colsOpen+1 : colsClose])
	vals := strings.TrimSpace(rest[valsOpen+1 : valsClose])
	return cols, vals, nil
}

// truncateForError shortens a segment for inclusion in an error message.
func truncateForError(s string) string {
	if len(s) > 80 {
		return s[:80] + "…"
	}
	return s
}
