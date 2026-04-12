package server

import "testing"

func TestMatchCreateSchema(t *testing.T) {
	tests := []struct {
		name   string
		sql    string
		expect string // expected captured schema name, "" if no match
	}{
		{"plain", "CREATE SCHEMA my_dataset", "my_dataset"},
		{"if not exists", "CREATE SCHEMA IF NOT EXISTS my_dataset", "my_dataset"},
		{"if not exists lowercase", "create schema if not exists my_dataset", "my_dataset"},
		{"if not exists mixed case", "Create Schema If Not Exists my_dataset", "my_dataset"},
		{"quoted", `CREATE SCHEMA "my_dataset"`, "my_dataset"},
		{"if not exists quoted", `CREATE SCHEMA IF NOT EXISTS "my_dataset"`, "my_dataset"},
		{"with trailing space", "CREATE SCHEMA IF NOT EXISTS my_dataset ", "my_dataset"},
		{"no match insert", "INSERT INTO my_table VALUES (1)", ""},
		{"no match select", "SELECT * FROM my_table", ""},
		// Regression: must NOT capture "IF" as schema name
		{"regression IF captured", "CREATE SCHEMA IF NOT EXISTS test_ds", "test_ds"},
		// After translation, project-qualified becomes just the dataset name
		// e.g. `test-project`.e2e_test -> e2e_test (translator strips project)
		{"post-translation plain", "CREATE SCHEMA e2e_test_123", "e2e_test_123"},
		{"post-translation if not exists", "CREATE SCHEMA IF NOT EXISTS e2e_test_123", "e2e_test_123"},
		// Double-quoted (translator output for backtick identifiers)
		{"double-quoted", `CREATE SCHEMA "my_dataset"`, "my_dataset"},
		{"double-quoted if not exists", `CREATE SCHEMA IF NOT EXISTS "my_dataset"`, "my_dataset"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := matchCreateSchema(tc.sql)
			if tc.expect == "" {
				if m != nil && len(m) >= 2 && m[1] != "" {
					t.Errorf("expected no match, got %q", m[1])
				}
				return
			}
			if len(m) < 2 {
				t.Fatalf("expected match for %q, got nil", tc.sql)
			}
			if m[1] != tc.expect {
				t.Errorf("got %q, want %q", m[1], tc.expect)
			}
		})
	}
}

func TestMatchDropSchema(t *testing.T) {
	tests := []struct {
		name, sql, expect string
	}{
		{"plain", "DROP SCHEMA my_dataset", "my_dataset"},
		{"if exists", "DROP SCHEMA IF EXISTS my_dataset", "my_dataset"},
		{"cascade", "DROP SCHEMA IF EXISTS my_dataset CASCADE", "my_dataset"},
		{"no match", "SELECT 1", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := matchDropSchema(tc.sql)
			if tc.expect == "" {
				if m != nil && len(m) >= 2 && m[1] != "" {
					t.Errorf("expected no match, got %q", m[1])
				}
				return
			}
			if len(m) < 2 {
				t.Fatalf("expected match, got nil")
			}
			if m[1] != tc.expect {
				t.Errorf("got %q, want %q", m[1], tc.expect)
			}
		})
	}
}

func TestMatchCreateTable(t *testing.T) {
	tests := []struct {
		name, sql        string
		expectSchema     string // capture group 1
		expectTable      string // capture group 2 (may be empty for single-part)
	}{
		{"schema.table", "CREATE TABLE my_ds.my_tbl (id BIGINT)", "my_ds", "my_tbl"},
		{"if not exists", "CREATE TABLE IF NOT EXISTS my_ds.my_tbl (id BIGINT)", "my_ds", "my_tbl"},
		{"single part", "CREATE TABLE my_tbl (id BIGINT)", "my_tbl", ""},
		{"or replace", "CREATE OR REPLACE TABLE my_ds.my_tbl (id BIGINT)", "my_ds", "my_tbl"},
		{"temp table", "CREATE TEMPORARY TABLE my_tbl (id BIGINT)", "my_tbl", ""},
		{"no match", "SELECT 1", "", ""},
		// Regression: must NOT capture "IF" as table name
		{"regression IF", "CREATE TABLE IF NOT EXISTS ds.tbl (id BIGINT)", "ds", "tbl"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := matchCreateTable(tc.sql)
			if tc.expectSchema == "" {
				if m != nil && len(m) >= 2 && m[1] != "" {
					t.Errorf("expected no match, got %v", m)
				}
				return
			}
			if len(m) < 2 {
				t.Fatalf("expected match, got nil")
			}
			if m[1] != tc.expectSchema {
				t.Errorf("group 1: got %q, want %q", m[1], tc.expectSchema)
			}
			got2 := ""
			if len(m) >= 3 {
				got2 = m[2]
			}
			if got2 != tc.expectTable {
				t.Errorf("group 2: got %q, want %q", got2, tc.expectTable)
			}
		})
	}
}

func TestMatchDropTable(t *testing.T) {
	tests := []struct {
		name, sql    string
		expectSchema string
		expectTable  string
	}{
		{"schema.table", "DROP TABLE my_ds.my_tbl", "my_ds", "my_tbl"},
		{"if exists", "DROP TABLE IF EXISTS my_ds.my_tbl", "my_ds", "my_tbl"},
		{"single part", "DROP TABLE my_tbl", "my_tbl", ""},
		{"no match", "SELECT 1", "", ""},
		// Regression: must NOT capture "IF"
		{"regression IF", "DROP TABLE IF EXISTS ds.tbl", "ds", "tbl"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := matchDropTable(tc.sql)
			if tc.expectSchema == "" {
				if m != nil && len(m) >= 2 && m[1] != "" {
					t.Errorf("expected no match, got %v", m)
				}
				return
			}
			if len(m) < 2 {
				t.Fatalf("expected match, got nil")
			}
			if m[1] != tc.expectSchema {
				t.Errorf("group 1: got %q, want %q", m[1], tc.expectSchema)
			}
			got2 := ""
			if len(m) >= 3 {
				got2 = m[2]
			}
			if got2 != tc.expectTable {
				t.Errorf("group 2: got %q, want %q", got2, tc.expectTable)
			}
		})
	}
}

// Integration test: CREATE SCHEMA IF NOT EXISTS via /queries should register correctly
func TestDDLSync_CreateSchemaIfNotExists_Integration(t *testing.T) {
	cfg := Config{
		Host:      "localhost",
		Port:      0,
		ProjectID: "test-project",
		Database:  ":memory:",
	}
	srv, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer srv.Stop(nil)

	ctx := t.Context()

	// Execute CREATE SCHEMA IF NOT EXISTS via executor + sync
	translated, _ := srv.translator.Translate("CREATE SCHEMA IF NOT EXISTS test_ds")
	_, err = srv.executor.Execute(ctx, translated)
	if err != nil {
		t.Fatalf("Execute error = %v", err)
	}
	srv.syncDDLMetadata(ctx, "test-project", "CREATE SCHEMA IF NOT EXISTS test_ds")

	// Verify dataset appears in metadata
	datasets, err := srv.repo.ListDatasets(ctx, "test-project")
	if err != nil {
		t.Fatalf("ListDatasets error = %v", err)
	}

	found := false
	for _, ds := range datasets {
		if ds.DatasetID == "test_ds" {
			found = true
			break
		}
	}
	if !found {
		names := make([]string, len(datasets))
		for i, ds := range datasets {
			names[i] = ds.DatasetID
		}
		t.Errorf("test_ds not found in datasets: %v", names)
	}
}

// Integration test: CREATE TABLE via SQL should register in metadata
func TestDDLSync_CreateTable_Integration(t *testing.T) {
	cfg := Config{
		Host:      "localhost",
		Port:      0,
		ProjectID: "test-project",
		Database:  ":memory:",
	}
	srv, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer srv.Stop(nil)

	ctx := t.Context()

	// Create schema + table via executor
	srv.executor.Execute(ctx, `CREATE SCHEMA test_ds`)
	srv.syncDDLMetadata(ctx, "test-project", "CREATE SCHEMA test_ds")

	srv.executor.Execute(ctx, `CREATE TABLE test_ds.my_table (id BIGINT, name VARCHAR)`)
	srv.syncDDLMetadata(ctx, "test-project", "CREATE TABLE test_ds.my_table (id BIGINT, name VARCHAR)")

	// Verify table appears in metadata
	tables, err := srv.repo.ListTables(ctx, "test-project", "test_ds")
	if err != nil {
		t.Fatalf("ListTables error = %v", err)
	}

	found := false
	for _, tbl := range tables {
		if tbl.TableID == "my_table" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("my_table not found in tables")
	}
}

// Integration test: DROP SCHEMA via SQL should remove metadata
func TestDDLSync_DropSchema_Integration(t *testing.T) {
	cfg := Config{
		Host:      "localhost",
		Port:      0,
		ProjectID: "test-project",
		Database:  ":memory:",
	}
	srv, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer srv.Stop(nil)

	ctx := t.Context()

	// Create then drop
	srv.executor.Execute(ctx, `CREATE SCHEMA drop_ds`)
	srv.syncDDLMetadata(ctx, "test-project", "CREATE SCHEMA drop_ds")

	srv.executor.Execute(ctx, `DROP SCHEMA drop_ds CASCADE`)
	srv.syncDDLMetadata(ctx, "test-project", "DROP SCHEMA drop_ds CASCADE")

	// Verify gone from metadata
	datasets, _ := srv.repo.ListDatasets(ctx, "test-project")
	for _, ds := range datasets {
		if ds.DatasetID == "drop_ds" {
			t.Error("drop_ds still in metadata after DROP")
		}
	}
}

// Integration: project-qualified CREATE SCHEMA via translated SQL
func TestDDLSync_ProjectQualified_CreateSchema(t *testing.T) {
	cfg := Config{
		Host:      "localhost",
		Port:      0,
		ProjectID: "test-project",
		Database:  ":memory:",
	}
	srv, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer srv.Stop(nil)

	ctx := t.Context()

	// Simulate what happens when SDK sends: CREATE SCHEMA IF NOT EXISTS `test-project`.e2e_test
	// The translator strips the project prefix before it reaches syncDDLMetadata
	originalSQL := "CREATE SCHEMA IF NOT EXISTS `test-project`.e2e_test"
	translated, err := srv.translator.Translate(originalSQL)
	if err != nil {
		t.Fatalf("Translate error = %v", err)
	}

	_, err = srv.executor.Execute(ctx, translated)
	if err != nil {
		t.Fatalf("Execute error = %v", err)
	}

	// Pass translated SQL (not original) — this is the fix
	srv.syncDDLMetadata(ctx, "test-project", translated)

	datasets, _ := srv.repo.ListDatasets(ctx, "test-project")
	found := false
	for _, ds := range datasets {
		if ds.DatasetID == "e2e_test" {
			found = true
		}
		if ds.DatasetID == "IF" {
			t.Errorf("BUG: captured 'IF' as dataset name instead of 'e2e_test'")
		}
	}
	if !found {
		names := make([]string, len(datasets))
		for i, ds := range datasets {
			names[i] = ds.DatasetID
		}
		t.Errorf("e2e_test not found in datasets: %v", names)
	}
}
