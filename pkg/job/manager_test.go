package job

import (
	"context"
	"testing"
	"time"

	"github.com/sathish/bigquery-emulator/pkg/connection"
	"github.com/sathish/bigquery-emulator/pkg/metadata"
	"github.com/sathish/bigquery-emulator/pkg/query"
	"go.uber.org/zap"
)

// newTestManager creates a Manager backed by an in-memory DuckDB for testing.
// It also creates a test project and a dataset with a simple table so that
// query jobs have something to query.
func newTestManager(t *testing.T) *Manager {
	t.Helper()
	logger, _ := zap.NewDevelopment()

	connMgr, err := connection.NewManager(":memory:", logger)
	if err != nil {
		t.Fatalf("failed to create connection manager: %v", err)
	}
	t.Cleanup(func() { connMgr.Close() })

	repo, err := metadata.NewRepository(connMgr, logger)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	ctx := context.Background()
	_ = repo.CreateProject(ctx, metadata.Project{ID: "test-project"})
	now := time.Now()
	_ = repo.CreateDataset(ctx, metadata.Dataset{
		ProjectID:        "test-project",
		DatasetID:        "test_dataset",
		CreationTime:     now,
		LastModifiedTime: now,
	})
	_ = repo.CreateTable(ctx, metadata.Table{
		ProjectID: "test-project",
		DatasetID: "test_dataset",
		TableID:   "users",
		Type:      "TABLE",
		Schema: &metadata.TableSchema{
			Fields: []metadata.FieldSchema{
				{Name: "id", Type: "INT64", Mode: "REQUIRED"},
				{Name: "name", Type: "STRING"},
			},
		},
		CreationTime:     now,
		LastModifiedTime: now,
	})

	// Insert some test data
	_, err = connMgr.Exec(ctx, `INSERT INTO "test_dataset"."users" VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')`)
	if err != nil {
		t.Fatalf("failed to insert test data: %v", err)
	}

	executor := query.NewExecutor(connMgr, logger)
	translator := query.NewTranslator()

	return NewManager(repo, executor, translator, logger)
}

func TestManager_Submit_QueryJob(t *testing.T) {
	mgr := newTestManager(t)
	ctx := context.Background()

	config := metadata.JobConfig{
		JobType: "QUERY",
		Query: &metadata.QueryConfig{
			Query: `SELECT * FROM "test_dataset"."users" ORDER BY id`,
		},
	}

	job, err := mgr.Submit(ctx, "test-project", config)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if job.JobID == "" {
		t.Fatal("Submit() returned job with empty ID")
	}
	if job.ProjectID != "test-project" {
		t.Errorf("Submit() ProjectID = %q, want %q", job.ProjectID, "test-project")
	}

	// Wait for job to complete (with timeout)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		got, err := mgr.Get(ctx, "test-project", job.JobID)
		if err != nil {
			t.Fatalf("Get() error = %v", err)
		}
		if got.Status.State == metadata.JobStateDone {
			if got.Status.ErrorResult != nil {
				t.Fatalf("job completed with error: %s", got.Status.ErrorResult.Message)
			}
			// Verify results are stored
			result, err := mgr.GetQueryResults(ctx, "test-project", job.JobID, 0, 100)
			if err != nil {
				t.Fatalf("GetQueryResults() error = %v", err)
			}
			if result.TotalRows != 3 {
				t.Errorf("GetQueryResults() TotalRows = %d, want 3", result.TotalRows)
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("job did not complete within timeout")
}

func TestManager_Submit_InvalidSQL(t *testing.T) {
	mgr := newTestManager(t)
	ctx := context.Background()

	config := metadata.JobConfig{
		JobType: "QUERY",
		Query: &metadata.QueryConfig{
			Query: "SELECT * FROM nonexistent_table_xyz",
		},
	}

	job, err := mgr.Submit(ctx, "test-project", config)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	// Wait for job to complete with error
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		got, err := mgr.Get(ctx, "test-project", job.JobID)
		if err != nil {
			t.Fatalf("Get() error = %v", err)
		}
		if got.Status.State == metadata.JobStateDone {
			if got.Status.ErrorResult == nil {
				t.Fatal("expected job to have error result, got nil")
			}
			if got.Status.ErrorResult.Message == "" {
				t.Error("expected non-empty error message")
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("job did not complete within timeout")
}

func TestManager_Get_Exists(t *testing.T) {
	mgr := newTestManager(t)
	ctx := context.Background()

	config := metadata.JobConfig{
		JobType: "QUERY",
		Query: &metadata.QueryConfig{
			Query: "SELECT 1 AS num",
		},
	}

	submitted, err := mgr.Submit(ctx, "test-project", config)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	got, err := mgr.Get(ctx, "test-project", submitted.JobID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got.JobID != submitted.JobID {
		t.Errorf("Get() JobID = %q, want %q", got.JobID, submitted.JobID)
	}
	if got.Config.JobType != "QUERY" {
		t.Errorf("Get() JobType = %q, want %q", got.Config.JobType, "QUERY")
	}
}

func TestManager_Get_NotFound(t *testing.T) {
	mgr := newTestManager(t)
	ctx := context.Background()

	_, err := mgr.Get(ctx, "test-project", "nonexistent-job-id")
	if err == nil {
		t.Fatal("Get() expected error for nonexistent job, got nil")
	}
}

func TestManager_List(t *testing.T) {
	mgr := newTestManager(t)
	ctx := context.Background()

	// Submit a few jobs
	for i := 0; i < 3; i++ {
		_, err := mgr.Submit(ctx, "test-project", metadata.JobConfig{
			JobType: "QUERY",
			Query:   &metadata.QueryConfig{Query: "SELECT 1"},
		})
		if err != nil {
			t.Fatalf("Submit() error = %v", err)
		}
	}

	jobs, err := mgr.List(ctx, "test-project")
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(jobs) != 3 {
		t.Errorf("List() got %d jobs, want 3", len(jobs))
	}
}

func TestManager_Cancel(t *testing.T) {
	mgr := newTestManager(t)
	ctx := context.Background()

	config := metadata.JobConfig{
		JobType: "QUERY",
		Query:   &metadata.QueryConfig{Query: "SELECT 1"},
	}

	job, err := mgr.Submit(ctx, "test-project", config)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	// Cancel the job
	err = mgr.Cancel(ctx, "test-project", job.JobID)
	if err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}

	// Verify job is done with no error (cancel is best-effort)
	got, err := mgr.Get(ctx, "test-project", job.JobID)
	if err != nil {
		t.Fatalf("Get() after cancel error = %v", err)
	}
	if got.Status.State != metadata.JobStateDone {
		t.Errorf("Cancel() State = %q, want %q", got.Status.State, metadata.JobStateDone)
	}
}

func TestManager_GetQueryResults(t *testing.T) {
	mgr := newTestManager(t)
	ctx := context.Background()

	config := metadata.JobConfig{
		JobType: "QUERY",
		Query: &metadata.QueryConfig{
			Query: `SELECT * FROM "test_dataset"."users" ORDER BY id`,
		},
	}

	job, err := mgr.Submit(ctx, "test-project", config)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	// Wait for completion
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		got, _ := mgr.Get(ctx, "test-project", job.JobID)
		if got != nil && got.Status.State == metadata.JobStateDone {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	result, err := mgr.GetQueryResults(ctx, "test-project", job.JobID, 0, 100)
	if err != nil {
		t.Fatalf("GetQueryResults() error = %v", err)
	}
	if result.TotalRows != 3 {
		t.Errorf("GetQueryResults() TotalRows = %d, want 3", result.TotalRows)
	}
	if len(result.Schema) < 2 {
		t.Errorf("GetQueryResults() schema columns = %d, want >= 2", len(result.Schema))
	}
	if !result.JobComplete {
		t.Error("GetQueryResults() JobComplete = false, want true")
	}
}

func TestManager_GetQueryResults_NotReady(t *testing.T) {
	mgr := newTestManager(t)
	ctx := context.Background()

	// Create a job directly in the in-memory map without executing
	jobID := "pending-job"
	now := time.Now()
	j := &metadata.Job{
		ProjectID:    "test-project",
		JobID:        jobID,
		Config:       metadata.JobConfig{JobType: "QUERY", Query: &metadata.QueryConfig{Query: "SELECT 1"}},
		Status:       metadata.JobStatus{State: metadata.JobStateRunning},
		CreationTime: now,
	}

	mgr.mu.Lock()
	mgr.jobs["test-project/"+jobID] = j
	mgr.mu.Unlock()

	// Store in repo too
	_ = mgr.repo.CreateJob(ctx, *j)

	result, err := mgr.GetQueryResults(ctx, "test-project", jobID, 0, 100)
	if err != nil {
		t.Fatalf("GetQueryResults() error = %v", err)
	}
	if result.JobComplete {
		t.Error("GetQueryResults() JobComplete = true, want false for running job")
	}
}
