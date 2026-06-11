package query

import (
	"strings"
	"testing"
)

func TestTranslateMerge_SimpleWithAS(t *testing.T) {
	sql := `MERGE INTO tgt_table AS t
USING src_table AS s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET v = s.v
WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)`

	stmts, err := TranslateMerge(sql)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if len(stmts) != 2 {
		t.Fatalf("expected 2 statements, got %d: %v", len(stmts), stmts)
	}
	if !strings.HasPrefix(stmts[0], "UPDATE tgt_table SET v = s.v FROM src_table AS s") {
		t.Errorf("unexpected UPDATE: %q", stmts[0])
	}
	if !strings.HasPrefix(stmts[1], "INSERT INTO tgt_table (id, v) SELECT s.id, s.v FROM src_table AS s") {
		t.Errorf("unexpected INSERT: %q", stmts[1])
	}
	// Target alias must be replaced with the table name
	if strings.Contains(stmts[0], "t.id") {
		t.Errorf("target alias not replaced in UPDATE: %q", stmts[0])
	}
}

// TestTranslateMerge_AuditFlushShape is the SignalSmith sync_snapshot audit
// MERGE (gap #8): aliases WITHOUT the AS keyword, a USING subquery with
// nested parens (LEFT JOIN, QUALIFY, window function), and INSERT values.
func TestTranslateMerge_AuditFlushShape(t *testing.T) {
	sql := `MERGE INTO sync_snapshot tgt
USING (
    SELECT
        'sync-1' AS sync_id,
        d.pk AS primary_key_value,
        d._diff_op AS last_operation,
        'run-9' AS last_run_id
    FROM diff_table d
    LEFT JOIN errors_table e
        ON e.batch_number = 3
       AND e.primary_key_value = d.pk
    WHERE d._row_num > 0 AND d._row_num <= 100
      AND e.primary_key_value IS NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY d.pk ORDER BY d._row_num DESC) = 1
        AND d._diff_op IN ('added','changed')
) src
ON tgt.sync_id = src.sync_id AND tgt.primary_key_value = src.primary_key_value
WHEN MATCHED THEN UPDATE SET
    row_data = NULL,
    last_operation = src.last_operation,
    last_run_id = src.last_run_id,
    last_synced_at = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT
    (sync_id, primary_key_value, row_data, last_operation, last_run_id, last_synced_at)
    VALUES (src.sync_id, src.primary_key_value, NULL, src.last_operation, src.last_run_id, CURRENT_TIMESTAMP)`

	stmts, err := TranslateMerge(sql)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if len(stmts) != 2 {
		t.Fatalf("expected 2 statements, got %d: %v", len(stmts), stmts)
	}

	update, insert := stmts[0], stmts[1]
	if !strings.HasPrefix(update, "UPDATE sync_snapshot SET") {
		t.Errorf("unexpected UPDATE prefix: %q", update)
	}
	// Full USING subquery must survive intact (nested parens, QUALIFY)
	if !strings.Contains(update, "QUALIFY ROW_NUMBER() OVER (PARTITION BY d.pk ORDER BY d._row_num DESC) = 1") {
		t.Errorf("USING subquery truncated in UPDATE: %q", update)
	}
	if !strings.Contains(update, ") AS src WHERE sync_snapshot.sync_id = src.sync_id") {
		t.Errorf("ON condition/alias replacement wrong in UPDATE: %q", update)
	}
	if !strings.HasPrefix(insert, "INSERT INTO sync_snapshot (sync_id, primary_key_value, row_data, last_operation, last_run_id, last_synced_at)") {
		t.Errorf("unexpected INSERT prefix: %q", insert)
	}
	if !strings.Contains(insert, "WHERE NOT EXISTS (SELECT 1 FROM sync_snapshot WHERE sync_snapshot.sync_id = src.sync_id") {
		t.Errorf("NOT EXISTS guard wrong in INSERT: %q", insert)
	}
}

func TestTranslateMerge_AliasWithoutAS_Delete(t *testing.T) {
	sql := `MERGE INTO t1 a USING t2 b ON a.id = b.id WHEN MATCHED THEN DELETE`
	stmts, err := TranslateMerge(sql)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
	expected := "DELETE FROM t1 WHERE EXISTS (SELECT 1 FROM t2 AS b WHERE t1.id = b.id)"
	if stmts[0] != expected {
		t.Errorf("got %q, want %q", stmts[0], expected)
	}
}

func TestTranslateMerge_WhenClauseConditions(t *testing.T) {
	sql := `MERGE INTO t USING s ON t.id = s.id
WHEN MATCHED AND s.op = 'del' THEN DELETE
WHEN MATCHED AND s.op = 'upd' THEN UPDATE SET v = s.v
WHEN NOT MATCHED AND s.op = 'add' THEN INSERT (id, v) VALUES (s.id, s.v)`

	stmts, err := TranslateMerge(sql)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if len(stmts) != 3 {
		t.Fatalf("expected 3 statements, got %d: %v", len(stmts), stmts)
	}
	if !strings.Contains(stmts[0], "AND (s.op = 'del')") {
		t.Errorf("DELETE missing AND condition: %q", stmts[0])
	}
	if !strings.Contains(stmts[1], "AND (s.op = 'upd')") {
		t.Errorf("UPDATE missing AND condition: %q", stmts[1])
	}
	if !strings.Contains(stmts[2], "AND (s.op = 'add')") {
		t.Errorf("INSERT missing AND condition: %q", stmts[2])
	}
}

func TestTranslateMerge_InsertValuesWithFunctionCalls(t *testing.T) {
	sql := `MERGE INTO t USING s ON t.id = s.id
WHEN NOT MATCHED THEN INSERT (id, payload) VALUES (s.id, CONCAT(s.a, ':', COALESCE(s.b, '')))`
	stmts, err := TranslateMerge(sql)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	if !strings.Contains(stmts[0], "SELECT s.id, CONCAT(s.a, ':', COALESCE(s.b, '')) FROM s") {
		t.Errorf("nested-paren VALUES mangled: %q", stmts[0])
	}
}

func TestTranslateMerge_NotAMerge(t *testing.T) {
	if _, err := TranslateMerge("SELECT 1"); err == nil {
		t.Fatal("expected error for non-MERGE input")
	}
}
