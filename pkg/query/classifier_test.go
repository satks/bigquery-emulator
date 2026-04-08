package query

import (
	"testing"
)

func TestClassifySQL_Select(t *testing.T) {
	result := ClassifySQL("SELECT 1")
	if result.Type != StatementQuery {
		t.Errorf("expected StatementQuery, got %v", result.Type)
	}
	if !result.IsQuery {
		t.Error("expected IsQuery=true")
	}
	if result.IsDDL {
		t.Error("expected IsDDL=false")
	}
	if result.IsDML {
		t.Error("expected IsDML=false")
	}
}

func TestClassifySQL_SelectWithWhitespace(t *testing.T) {
	result := ClassifySQL("  \n  SELECT 1")
	if result.Type != StatementQuery {
		t.Errorf("expected StatementQuery, got %v", result.Type)
	}
	if !result.IsQuery {
		t.Error("expected IsQuery=true")
	}
}

func TestClassifySQL_WithCTE(t *testing.T) {
	result := ClassifySQL("WITH cte AS (SELECT 1) SELECT * FROM cte")
	if result.Type != StatementQuery {
		t.Errorf("expected StatementQuery, got %v", result.Type)
	}
	if !result.IsQuery {
		t.Error("expected IsQuery=true")
	}
}

func TestClassifySQL_Insert(t *testing.T) {
	result := ClassifySQL("INSERT INTO t (col) VALUES (1)")
	if result.Type != StatementDML {
		t.Errorf("expected StatementDML, got %v", result.Type)
	}
	if !result.IsDML {
		t.Error("expected IsDML=true")
	}
	if result.IsQuery {
		t.Error("expected IsQuery=false")
	}
	if result.IsDDL {
		t.Error("expected IsDDL=false")
	}
}

func TestClassifySQL_Update(t *testing.T) {
	result := ClassifySQL("UPDATE t SET col = 1")
	if result.Type != StatementDML {
		t.Errorf("expected StatementDML, got %v", result.Type)
	}
	if !result.IsDML {
		t.Error("expected IsDML=true")
	}
}

func TestClassifySQL_Delete(t *testing.T) {
	result := ClassifySQL("DELETE FROM t WHERE id = 1")
	if result.Type != StatementDML {
		t.Errorf("expected StatementDML, got %v", result.Type)
	}
	if !result.IsDML {
		t.Error("expected IsDML=true")
	}
}

func TestClassifySQL_Merge(t *testing.T) {
	result := ClassifySQL("MERGE INTO target USING source ON target.id = source.id WHEN MATCHED THEN UPDATE SET col = source.col")
	if result.Type != StatementDML {
		t.Errorf("expected StatementDML, got %v", result.Type)
	}
	if !result.IsDML {
		t.Error("expected IsDML=true")
	}
}

func TestClassifySQL_Truncate(t *testing.T) {
	result := ClassifySQL("TRUNCATE TABLE t")
	if result.Type != StatementDML {
		t.Errorf("expected StatementDML, got %v", result.Type)
	}
	if !result.IsDML {
		t.Error("expected IsDML=true")
	}
}

func TestClassifySQL_CreateTable(t *testing.T) {
	result := ClassifySQL("CREATE TABLE t (id INT)")
	if result.Type != StatementDDLCreate {
		t.Errorf("expected StatementDDLCreate, got %v", result.Type)
	}
	if !result.IsDDL {
		t.Error("expected IsDDL=true")
	}
	if result.IsQuery {
		t.Error("expected IsQuery=false")
	}
	if result.IsDML {
		t.Error("expected IsDML=false")
	}
}

func TestClassifySQL_CreateOrReplace(t *testing.T) {
	result := ClassifySQL("CREATE OR REPLACE TABLE t (id INT)")
	if result.Type != StatementDDLCreate {
		t.Errorf("expected StatementDDLCreate, got %v", result.Type)
	}
	if !result.IsDDL {
		t.Error("expected IsDDL=true")
	}
}

func TestClassifySQL_CreateSchema(t *testing.T) {
	result := ClassifySQL("CREATE SCHEMA my_schema")
	if result.Type != StatementDDLCreate {
		t.Errorf("expected StatementDDLCreate, got %v", result.Type)
	}
	if !result.IsDDL {
		t.Error("expected IsDDL=true")
	}
}

func TestClassifySQL_CreateView(t *testing.T) {
	result := ClassifySQL("CREATE VIEW v AS SELECT 1")
	if result.Type != StatementDDLCreate {
		t.Errorf("expected StatementDDLCreate, got %v", result.Type)
	}
	if !result.IsDDL {
		t.Error("expected IsDDL=true")
	}
}

func TestClassifySQL_DropTable(t *testing.T) {
	result := ClassifySQL("DROP TABLE t")
	if result.Type != StatementDDLDrop {
		t.Errorf("expected StatementDDLDrop, got %v", result.Type)
	}
	if !result.IsDDL {
		t.Error("expected IsDDL=true")
	}
	if result.IsQuery {
		t.Error("expected IsQuery=false")
	}
	if result.IsDML {
		t.Error("expected IsDML=false")
	}
}

func TestClassifySQL_DropSchema(t *testing.T) {
	result := ClassifySQL("DROP SCHEMA s")
	if result.Type != StatementDDLDrop {
		t.Errorf("expected StatementDDLDrop, got %v", result.Type)
	}
	if !result.IsDDL {
		t.Error("expected IsDDL=true")
	}
}

func TestClassifySQL_AlterTable(t *testing.T) {
	result := ClassifySQL("ALTER TABLE t ADD COLUMN col INT")
	if result.Type != StatementDDLAlter {
		t.Errorf("expected StatementDDLAlter, got %v", result.Type)
	}
	if !result.IsDDL {
		t.Error("expected IsDDL=true")
	}
	if result.IsQuery {
		t.Error("expected IsQuery=false")
	}
	if result.IsDML {
		t.Error("expected IsDML=false")
	}
}

func TestClassifySQL_Begin(t *testing.T) {
	result := ClassifySQL("BEGIN")
	if result.Type != StatementTransaction {
		t.Errorf("expected StatementTransaction, got %v", result.Type)
	}
	if result.IsQuery || result.IsDDL || result.IsDML {
		t.Error("transaction should have all flags false")
	}
}

func TestClassifySQL_Commit(t *testing.T) {
	result := ClassifySQL("COMMIT")
	if result.Type != StatementTransaction {
		t.Errorf("expected StatementTransaction, got %v", result.Type)
	}
	if result.IsQuery || result.IsDDL || result.IsDML {
		t.Error("transaction should have all flags false")
	}
}

func TestClassifySQL_Rollback(t *testing.T) {
	result := ClassifySQL("ROLLBACK")
	if result.Type != StatementTransaction {
		t.Errorf("expected StatementTransaction, got %v", result.Type)
	}
	if result.IsQuery || result.IsDDL || result.IsDML {
		t.Error("transaction should have all flags false")
	}
}

func TestClassifySQL_CaseInsensitive(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"lowercase", "select 1"},
		{"uppercase", "SELECT 1"},
		{"mixedcase", "Select 1"},
		{"oddcase", "sElEcT 1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ClassifySQL(tt.sql)
			if result.Type != StatementQuery {
				t.Errorf("ClassifySQL(%q) = %v, want StatementQuery", tt.sql, result.Type)
			}
		})
	}
}

func TestClassifySQL_EmptyString(t *testing.T) {
	result := ClassifySQL("")
	if result.Type != StatementUnknown {
		t.Errorf("expected StatementUnknown, got %v", result.Type)
	}
	if result.IsQuery || result.IsDDL || result.IsDML {
		t.Error("empty string should have all flags false")
	}
}

func TestClassifySQL_WhitespaceOnly(t *testing.T) {
	result := ClassifySQL("   \t\n  ")
	if result.Type != StatementUnknown {
		t.Errorf("expected StatementUnknown, got %v", result.Type)
	}
	if result.IsQuery || result.IsDDL || result.IsDML {
		t.Error("whitespace-only should have all flags false")
	}
}

func TestClassifySQL_WithLineComment(t *testing.T) {
	result := ClassifySQL("-- this is a comment\nSELECT 1")
	if result.Type != StatementQuery {
		t.Errorf("expected StatementQuery, got %v", result.Type)
	}
	if !result.IsQuery {
		t.Error("expected IsQuery=true")
	}
}

func TestClassifySQL_WithBlockComment(t *testing.T) {
	result := ClassifySQL("/* block comment */ SELECT 1")
	if result.Type != StatementQuery {
		t.Errorf("expected StatementQuery, got %v", result.Type)
	}
	if !result.IsQuery {
		t.Error("expected IsQuery=true")
	}
}

func TestClassifySQL_Unknown(t *testing.T) {
	result := ClassifySQL("EXPLAIN SELECT 1")
	if result.Type != StatementUnknown {
		t.Errorf("expected StatementUnknown, got %v", result.Type)
	}
	if result.IsQuery || result.IsDDL || result.IsDML {
		t.Error("unknown statement should have all flags false")
	}
}

func TestStatementType_String(t *testing.T) {
	tests := []struct {
		st   StatementType
		want string
	}{
		{StatementUnknown, "UNKNOWN"},
		{StatementQuery, "QUERY"},
		{StatementDML, "DML"},
		{StatementDDLCreate, "DDL_CREATE"},
		{StatementDDLDrop, "DDL_DROP"},
		{StatementDDLAlter, "DDL_ALTER"},
		{StatementTransaction, "TRANSACTION"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := tt.st.String()
			if got != tt.want {
				t.Errorf("StatementType(%d).String() = %q, want %q", tt.st, got, tt.want)
			}
		})
	}
}

func TestClassifyResult_Flags(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		isQuery bool
		isDDL   bool
		isDML   bool
	}{
		{"select is query only", "SELECT 1", true, false, false},
		{"with is query only", "WITH x AS (SELECT 1) SELECT * FROM x", true, false, false},
		{"insert is dml only", "INSERT INTO t VALUES (1)", false, false, true},
		{"update is dml only", "UPDATE t SET x=1", false, false, true},
		{"delete is dml only", "DELETE FROM t", false, false, true},
		{"merge is dml only", "MERGE INTO t USING s ON t.id=s.id WHEN MATCHED THEN DELETE", false, false, true},
		{"truncate is dml only", "TRUNCATE TABLE t", false, false, true},
		{"create is ddl only", "CREATE TABLE t (id INT)", false, true, false},
		{"drop is ddl only", "DROP TABLE t", false, true, false},
		{"alter is ddl only", "ALTER TABLE t ADD COLUMN c INT", false, true, false},
		{"begin has no flags", "BEGIN", false, false, false},
		{"commit has no flags", "COMMIT", false, false, false},
		{"rollback has no flags", "ROLLBACK", false, false, false},
		{"unknown has no flags", "EXPLAIN SELECT 1", false, false, false},
		{"empty has no flags", "", false, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ClassifySQL(tt.sql)
			if result.IsQuery != tt.isQuery {
				t.Errorf("IsQuery = %v, want %v", result.IsQuery, tt.isQuery)
			}
			if result.IsDDL != tt.isDDL {
				t.Errorf("IsDDL = %v, want %v", result.IsDDL, tt.isDDL)
			}
			if result.IsDML != tt.isDML {
				t.Errorf("IsDML = %v, want %v", result.IsDML, tt.isDML)
			}
		})
	}
}

// Additional edge case tests for thorough comment handling.

func TestClassifySQL_MultipleLineComments(t *testing.T) {
	result := ClassifySQL("-- comment 1\n-- comment 2\nSELECT 1")
	if result.Type != StatementQuery {
		t.Errorf("expected StatementQuery, got %v", result.Type)
	}
}

func TestClassifySQL_NestedBlockComment(t *testing.T) {
	// DuckDB/SQL standard: nested block comments are not standard,
	// but we should handle the outer /* */ correctly.
	result := ClassifySQL("/* outer /* not nested */ SELECT 1")
	if result.Type != StatementQuery {
		t.Errorf("expected StatementQuery, got %v", result.Type)
	}
}

func TestClassifySQL_BlockCommentThenLineComment(t *testing.T) {
	result := ClassifySQL("/* block */ -- line\nSELECT 1")
	if result.Type != StatementQuery {
		t.Errorf("expected StatementQuery, got %v", result.Type)
	}
}

func TestClassifySQL_OnlyComments(t *testing.T) {
	result := ClassifySQL("-- just a comment")
	if result.Type != StatementUnknown {
		t.Errorf("expected StatementUnknown, got %v", result.Type)
	}
}

func TestClassifySQL_UnclosedBlockComment(t *testing.T) {
	result := ClassifySQL("/* unclosed block comment")
	if result.Type != StatementUnknown {
		t.Errorf("expected StatementUnknown, got %v", result.Type)
	}
}
