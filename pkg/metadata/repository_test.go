package metadata

import (
	"context"
	"testing"
	"time"

	"github.com/sathish/bigquery-emulator/pkg/connection"
	"go.uber.org/zap"
)

// newTestRepo creates an in-memory DuckDB repository for testing.
func newTestRepo(t *testing.T) *Repository {
	t.Helper()
	logger, _ := zap.NewDevelopment()
	connMgr, err := connection.NewManager(":memory:", logger)
	if err != nil {
		t.Fatalf("failed to create connection manager: %v", err)
	}
	t.Cleanup(func() { connMgr.Close() })

	repo, err := NewRepository(connMgr, logger)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}
	return repo
}

func TestNewRepository(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	connMgr, err := connection.NewManager(":memory:", logger)
	if err != nil {
		t.Fatalf("failed to create connection manager: %v", err)
	}
	defer connMgr.Close()

	repo, err := NewRepository(connMgr, logger)
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}
	if repo == nil {
		t.Fatal("NewRepository() returned nil")
	}

	// Verify metadata tables exist by querying them
	ctx := context.Background()
	for _, table := range []string{"_bq_projects", "_bq_datasets", "_bq_tables", "_bq_jobs"} {
		rows, err := connMgr.Query(ctx, "SELECT COUNT(*) FROM "+table)
		if err != nil {
			t.Errorf("metadata table %s not created: %v", table, err)
			continue
		}
		rows.Close()
	}
}

func TestRepository_CreateProject(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	project := Project{
		ID:           "test-project",
		FriendlyName: "Test Project",
		NumericID:    12345,
	}

	if err := repo.CreateProject(ctx, project); err != nil {
		t.Fatalf("CreateProject() error = %v", err)
	}

	// Creating a duplicate should error
	err := repo.CreateProject(ctx, project)
	if err == nil {
		t.Fatal("CreateProject() expected error for duplicate, got nil")
	}
}

func TestRepository_GetProject(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	project := Project{
		ID:           "test-project",
		FriendlyName: "Test Project",
		NumericID:    12345,
	}
	if err := repo.CreateProject(ctx, project); err != nil {
		t.Fatalf("CreateProject() error = %v", err)
	}

	got, err := repo.GetProject(ctx, "test-project")
	if err != nil {
		t.Fatalf("GetProject() error = %v", err)
	}
	if got.ID != "test-project" {
		t.Errorf("GetProject() ID = %q, want %q", got.ID, "test-project")
	}
	if got.FriendlyName != "Test Project" {
		t.Errorf("GetProject() FriendlyName = %q, want %q", got.FriendlyName, "Test Project")
	}
	if got.NumericID != 12345 {
		t.Errorf("GetProject() NumericID = %d, want %d", got.NumericID, 12345)
	}

	// Not found
	_, err = repo.GetProject(ctx, "nonexistent")
	if err == nil {
		t.Fatal("GetProject() expected error for nonexistent project, got nil")
	}
}

func TestRepository_ListProjects(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	// Empty list
	projects, err := repo.ListProjects(ctx)
	if err != nil {
		t.Fatalf("ListProjects() error = %v", err)
	}
	if len(projects) != 0 {
		t.Errorf("ListProjects() got %d projects, want 0", len(projects))
	}

	// Add two projects
	_ = repo.CreateProject(ctx, Project{ID: "proj-a", FriendlyName: "A"})
	_ = repo.CreateProject(ctx, Project{ID: "proj-b", FriendlyName: "B"})

	projects, err = repo.ListProjects(ctx)
	if err != nil {
		t.Fatalf("ListProjects() error = %v", err)
	}
	if len(projects) != 2 {
		t.Errorf("ListProjects() got %d projects, want 2", len(projects))
	}
}

func TestRepository_CreateDataset(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	_ = repo.CreateProject(ctx, Project{ID: "test-project"})

	ds := Dataset{
		ProjectID:    "test-project",
		DatasetID:    "test_dataset",
		FriendlyName: "Test Dataset",
		Location:     "US",
		CreationTime: time.Now(),
		LastModifiedTime: time.Now(),
	}

	if err := repo.CreateDataset(ctx, ds); err != nil {
		t.Fatalf("CreateDataset() error = %v", err)
	}

	// Verify DuckDB schema was created by creating a table in it
	_, err := repo.connMgr.Exec(ctx, `CREATE TABLE "test_dataset"."_verify_schema" (id INTEGER)`)
	if err != nil {
		t.Errorf("DuckDB schema not created for dataset: %v", err)
	}

	// Duplicate should error
	err = repo.CreateDataset(ctx, ds)
	if err == nil {
		t.Fatal("CreateDataset() expected error for duplicate, got nil")
	}
}

func TestRepository_GetDataset(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	_ = repo.CreateProject(ctx, Project{ID: "test-project"})
	ds := Dataset{
		ProjectID:        "test-project",
		DatasetID:        "test_dataset",
		FriendlyName:     "Test Dataset",
		Location:         "US",
		CreationTime:     time.Now().Truncate(time.Millisecond),
		LastModifiedTime: time.Now().Truncate(time.Millisecond),
	}
	_ = repo.CreateDataset(ctx, ds)

	got, err := repo.GetDataset(ctx, "test-project", "test_dataset")
	if err != nil {
		t.Fatalf("GetDataset() error = %v", err)
	}
	if got.DatasetID != "test_dataset" {
		t.Errorf("GetDataset() DatasetID = %q, want %q", got.DatasetID, "test_dataset")
	}
	if got.FriendlyName != "Test Dataset" {
		t.Errorf("GetDataset() FriendlyName = %q, want %q", got.FriendlyName, "Test Dataset")
	}

	// Not found
	_, err = repo.GetDataset(ctx, "test-project", "nonexistent")
	if err == nil {
		t.Fatal("GetDataset() expected error for nonexistent dataset, got nil")
	}
}

func TestRepository_ListDatasets(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	_ = repo.CreateProject(ctx, Project{ID: "test-project"})

	// Empty list
	datasets, err := repo.ListDatasets(ctx, "test-project")
	if err != nil {
		t.Fatalf("ListDatasets() error = %v", err)
	}
	if len(datasets) != 0 {
		t.Errorf("ListDatasets() got %d, want 0", len(datasets))
	}

	// Add datasets
	now := time.Now()
	_ = repo.CreateDataset(ctx, Dataset{ProjectID: "test-project", DatasetID: "ds_a", CreationTime: now, LastModifiedTime: now})
	_ = repo.CreateDataset(ctx, Dataset{ProjectID: "test-project", DatasetID: "ds_b", CreationTime: now, LastModifiedTime: now})

	datasets, err = repo.ListDatasets(ctx, "test-project")
	if err != nil {
		t.Fatalf("ListDatasets() error = %v", err)
	}
	if len(datasets) != 2 {
		t.Errorf("ListDatasets() got %d, want 2", len(datasets))
	}
}

func TestRepository_DeleteDataset(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	_ = repo.CreateProject(ctx, Project{ID: "test-project"})
	now := time.Now()
	_ = repo.CreateDataset(ctx, Dataset{ProjectID: "test-project", DatasetID: "to_delete", CreationTime: now, LastModifiedTime: now})

	// Delete with cascade
	if err := repo.DeleteDataset(ctx, "test-project", "to_delete", true); err != nil {
		t.Fatalf("DeleteDataset() error = %v", err)
	}

	// Should no longer exist
	_, err := repo.GetDataset(ctx, "test-project", "to_delete")
	if err == nil {
		t.Fatal("GetDataset() expected error after delete, got nil")
	}

	// Verify DuckDB schema was dropped
	_, err = repo.connMgr.Exec(ctx, `CREATE TABLE "to_delete"."verify" (id INTEGER)`)
	if err == nil {
		t.Error("DuckDB schema should have been dropped but table creation succeeded")
	}
}

func TestRepository_CreateTable(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	_ = repo.CreateProject(ctx, Project{ID: "test-project"})
	now := time.Now()
	_ = repo.CreateDataset(ctx, Dataset{ProjectID: "test-project", DatasetID: "test_dataset", CreationTime: now, LastModifiedTime: now})

	tbl := Table{
		ProjectID: "test-project",
		DatasetID: "test_dataset",
		TableID:   "test_table",
		Type:      "TABLE",
		Schema: &TableSchema{
			Fields: []FieldSchema{
				{Name: "id", Type: "INT64", Mode: "REQUIRED"},
				{Name: "name", Type: "STRING"},
				{Name: "score", Type: "FLOAT64"},
			},
		},
		CreationTime:     now,
		LastModifiedTime: now,
	}

	if err := repo.CreateTable(ctx, tbl); err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	// Verify DuckDB table was created by inserting data
	_, err := repo.connMgr.Exec(ctx, `INSERT INTO "test_dataset"."test_table" (id, name, score) VALUES (1, 'Alice', 9.5)`)
	if err != nil {
		t.Errorf("DuckDB table not created correctly: %v", err)
	}

	// Duplicate should error
	err = repo.CreateTable(ctx, tbl)
	if err == nil {
		t.Fatal("CreateTable() expected error for duplicate, got nil")
	}
}

func TestRepository_GetTable(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	_ = repo.CreateProject(ctx, Project{ID: "test-project"})
	now := time.Now()
	_ = repo.CreateDataset(ctx, Dataset{ProjectID: "test-project", DatasetID: "test_dataset", CreationTime: now, LastModifiedTime: now})
	tbl := Table{
		ProjectID: "test-project",
		DatasetID: "test_dataset",
		TableID:   "test_table",
		Type:      "TABLE",
		Schema: &TableSchema{
			Fields: []FieldSchema{
				{Name: "id", Type: "INT64"},
			},
		},
		CreationTime:     now,
		LastModifiedTime: now,
	}
	_ = repo.CreateTable(ctx, tbl)

	got, err := repo.GetTable(ctx, "test-project", "test_dataset", "test_table")
	if err != nil {
		t.Fatalf("GetTable() error = %v", err)
	}
	if got.TableID != "test_table" {
		t.Errorf("GetTable() TableID = %q, want %q", got.TableID, "test_table")
	}
	if got.Schema == nil || len(got.Schema.Fields) != 1 {
		t.Error("GetTable() schema not preserved")
	}

	// Not found
	_, err = repo.GetTable(ctx, "test-project", "test_dataset", "nonexistent")
	if err == nil {
		t.Fatal("GetTable() expected error for nonexistent, got nil")
	}
}

func TestRepository_ListTables(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	_ = repo.CreateProject(ctx, Project{ID: "test-project"})
	now := time.Now()
	_ = repo.CreateDataset(ctx, Dataset{ProjectID: "test-project", DatasetID: "test_dataset", CreationTime: now, LastModifiedTime: now})

	// Empty list
	tables, err := repo.ListTables(ctx, "test-project", "test_dataset")
	if err != nil {
		t.Fatalf("ListTables() error = %v", err)
	}
	if len(tables) != 0 {
		t.Errorf("ListTables() got %d, want 0", len(tables))
	}

	// Add tables
	for _, id := range []string{"tbl_a", "tbl_b"} {
		_ = repo.CreateTable(ctx, Table{
			ProjectID:        "test-project",
			DatasetID:        "test_dataset",
			TableID:          id,
			Type:             "TABLE",
			Schema:           &TableSchema{Fields: []FieldSchema{{Name: "id", Type: "INT64"}}},
			CreationTime:     now,
			LastModifiedTime: now,
		})
	}

	tables, err = repo.ListTables(ctx, "test-project", "test_dataset")
	if err != nil {
		t.Fatalf("ListTables() error = %v", err)
	}
	if len(tables) != 2 {
		t.Errorf("ListTables() got %d, want 2", len(tables))
	}
}

func TestRepository_DeleteTable(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	_ = repo.CreateProject(ctx, Project{ID: "test-project"})
	now := time.Now()
	_ = repo.CreateDataset(ctx, Dataset{ProjectID: "test-project", DatasetID: "test_dataset", CreationTime: now, LastModifiedTime: now})
	_ = repo.CreateTable(ctx, Table{
		ProjectID:        "test-project",
		DatasetID:        "test_dataset",
		TableID:          "to_delete",
		Type:             "TABLE",
		Schema:           &TableSchema{Fields: []FieldSchema{{Name: "id", Type: "INT64"}}},
		CreationTime:     now,
		LastModifiedTime: now,
	})

	if err := repo.DeleteTable(ctx, "test-project", "test_dataset", "to_delete"); err != nil {
		t.Fatalf("DeleteTable() error = %v", err)
	}

	// Should no longer exist in metadata
	_, err := repo.GetTable(ctx, "test-project", "test_dataset", "to_delete")
	if err == nil {
		t.Fatal("GetTable() expected error after delete, got nil")
	}

	// Verify DuckDB table was dropped
	_, err = repo.connMgr.Exec(ctx, `INSERT INTO "test_dataset"."to_delete" (id) VALUES (1)`)
	if err == nil {
		t.Error("DuckDB table should have been dropped but insert succeeded")
	}
}

func TestRepository_CreateJob_GetJob_UpdateJob(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	_ = repo.CreateProject(ctx, Project{ID: "test-project"})

	job := Job{
		ProjectID: "test-project",
		JobID:     "job-123",
		Config: JobConfig{
			JobType: "QUERY",
			Query: &QueryConfig{
				Query: "SELECT 1",
			},
		},
		Status: JobStatus{
			State: JobStatePending,
		},
		CreationTime: time.Now(),
	}

	// Create
	if err := repo.CreateJob(ctx, job); err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}

	// Get
	got, err := repo.GetJob(ctx, "test-project", "job-123")
	if err != nil {
		t.Fatalf("GetJob() error = %v", err)
	}
	if got.JobID != "job-123" {
		t.Errorf("GetJob() JobID = %q, want %q", got.JobID, "job-123")
	}
	if got.Status.State != JobStatePending {
		t.Errorf("GetJob() State = %q, want %q", got.Status.State, JobStatePending)
	}
	if got.Config.Query == nil || got.Config.Query.Query != "SELECT 1" {
		t.Error("GetJob() query config not preserved")
	}

	// Update
	got.Status.State = JobStateDone
	if err := repo.UpdateJob(ctx, *got); err != nil {
		t.Fatalf("UpdateJob() error = %v", err)
	}

	updated, err := repo.GetJob(ctx, "test-project", "job-123")
	if err != nil {
		t.Fatalf("GetJob() after update error = %v", err)
	}
	if updated.Status.State != JobStateDone {
		t.Errorf("UpdateJob() State = %q, want %q", updated.Status.State, JobStateDone)
	}

	// Not found
	_, err = repo.GetJob(ctx, "test-project", "nonexistent")
	if err == nil {
		t.Fatal("GetJob() expected error for nonexistent job, got nil")
	}
}

func TestRepository_ListJobs(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	_ = repo.CreateProject(ctx, Project{ID: "test-project"})

	// Empty list
	jobs, err := repo.ListJobs(ctx, "test-project")
	if err != nil {
		t.Fatalf("ListJobs() error = %v", err)
	}
	if len(jobs) != 0 {
		t.Errorf("ListJobs() got %d, want 0", len(jobs))
	}

	// Add jobs
	for _, id := range []string{"job-a", "job-b"} {
		_ = repo.CreateJob(ctx, Job{
			ProjectID: "test-project",
			JobID:     id,
			Config:    JobConfig{JobType: "QUERY"},
			Status:    JobStatus{State: JobStatePending},
			CreationTime: time.Now(),
		})
	}

	jobs, err = repo.ListJobs(ctx, "test-project")
	if err != nil {
		t.Fatalf("ListJobs() error = %v", err)
	}
	if len(jobs) != 2 {
		t.Errorf("ListJobs() got %d, want 2", len(jobs))
	}
}

// TestRepository_DeleteDataset_NoGhostData is a regression test for Bug 4:
// DROP SCHEMA CASCADE must fully clean up so that recreating the same dataset+table
// does not surface old rows from the dropped schema.
func TestRepository_DeleteDataset_NoGhostData(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	_ = repo.CreateProject(ctx, Project{ID: "test-project"})
	now := time.Now()

	// Step 1: Create dataset + table + insert rows
	_ = repo.CreateDataset(ctx, Dataset{
		ProjectID:        "test-project",
		DatasetID:        "ghost_ds",
		CreationTime:     now,
		LastModifiedTime: now,
	})
	_ = repo.CreateTable(ctx, Table{
		ProjectID: "test-project",
		DatasetID: "ghost_ds",
		TableID:   "ghost_tbl",
		Type:      "TABLE",
		Schema: &TableSchema{
			Fields: []FieldSchema{
				{Name: "id", Type: "INT64", Mode: "REQUIRED"},
				{Name: "value", Type: "STRING"},
			},
		},
		CreationTime:     now,
		LastModifiedTime: now,
	})

	// Insert old rows
	_, err := repo.connMgr.Exec(ctx, `INSERT INTO "ghost_ds"."ghost_tbl" (id, value) VALUES (1, 'old_row_1'), (2, 'old_row_2')`)
	if err != nil {
		t.Fatalf("insert old rows: %v", err)
	}

	// Verify old rows exist
	rows, err := repo.connMgr.Query(ctx, `SELECT COUNT(*) FROM "ghost_ds"."ghost_tbl"`)
	if err != nil {
		t.Fatalf("count old rows: %v", err)
	}
	var count int64
	if rows.Next() {
		_ = rows.Scan(&count)
	}
	rows.Close()
	if count != 2 {
		t.Fatalf("expected 2 old rows, got %d", count)
	}

	// Step 2: Delete dataset with deleteContents=true
	if err := repo.DeleteDataset(ctx, "test-project", "ghost_ds", true); err != nil {
		t.Fatalf("DeleteDataset() error = %v", err)
	}

	// Verify metadata is gone
	_, err = repo.GetDataset(ctx, "test-project", "ghost_ds")
	if err == nil {
		t.Fatal("dataset metadata should be gone after delete")
	}
	tables, err := repo.ListTables(ctx, "test-project", "ghost_ds")
	if err != nil {
		t.Fatalf("ListTables() error = %v", err)
	}
	if len(tables) != 0 {
		t.Fatalf("expected 0 table metadata entries after delete, got %d", len(tables))
	}

	// Step 3: Recreate the same dataset + table
	_ = repo.CreateDataset(ctx, Dataset{
		ProjectID:        "test-project",
		DatasetID:        "ghost_ds",
		CreationTime:     now,
		LastModifiedTime: now,
	})
	_ = repo.CreateTable(ctx, Table{
		ProjectID: "test-project",
		DatasetID: "ghost_ds",
		TableID:   "ghost_tbl",
		Type:      "TABLE",
		Schema: &TableSchema{
			Fields: []FieldSchema{
				{Name: "id", Type: "INT64", Mode: "REQUIRED"},
				{Name: "value", Type: "STRING"},
			},
		},
		CreationTime:     now,
		LastModifiedTime: now,
	})

	// Step 4: Insert new rows only
	_, err = repo.connMgr.Exec(ctx, `INSERT INTO "ghost_ds"."ghost_tbl" (id, value) VALUES (10, 'new_row_1')`)
	if err != nil {
		t.Fatalf("insert new row: %v", err)
	}

	// Step 5: Query and verify ONLY the new row exists (no ghost data)
	rows, err = repo.connMgr.Query(ctx, `SELECT id, value FROM "ghost_ds"."ghost_tbl" ORDER BY id`)
	if err != nil {
		t.Fatalf("query after recreate: %v", err)
	}
	defer rows.Close()

	type row struct {
		ID    int64
		Value string
	}
	var results []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.ID, &r.Value); err != nil {
			t.Fatalf("scan row: %v", err)
		}
		results = append(results, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows error: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 row after recreate, got %d (ghost data detected!)", len(results))
	}
	if results[0].ID != 10 || results[0].Value != "new_row_1" {
		t.Errorf("unexpected row: id=%d value=%q, want id=10 value=new_row_1", results[0].ID, results[0].Value)
	}
}
