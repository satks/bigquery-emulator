package job

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sathish/bigquery-emulator/pkg/metadata"
	"github.com/sathish/bigquery-emulator/pkg/query"
	"go.uber.org/zap"
)

// Manager manages BigQuery job lifecycle: submission, execution, status tracking,
// and result retrieval. Query results are stored in memory keyed by jobID.
type Manager struct {
	mu      sync.RWMutex
	jobs    map[string]*metadata.Job       // key: "projectID/jobID"
	results map[string]*query.QueryResult  // key: "projectID/jobID"

	repo       *metadata.Repository
	executor   *query.Executor
	translator *query.Translator
	logger     *zap.Logger
}

// NewManager creates a new job manager.
func NewManager(repo *metadata.Repository, exec *query.Executor, translator *query.Translator, logger *zap.Logger) *Manager {
	return &Manager{
		jobs:       make(map[string]*metadata.Job),
		results:    make(map[string]*query.QueryResult),
		repo:       repo,
		executor:   exec,
		translator: translator,
		logger:     logger,
	}
}

// jobKey returns the map key for a project/job pair.
func jobKey(projectID, jobID string) string {
	return projectID + "/" + jobID
}

// Submit creates a job and executes it asynchronously.
// For QUERY jobs, a goroutine is launched that translates the SQL, executes it,
// and stores the result. The job transitions PENDING -> RUNNING -> DONE.
func (m *Manager) Submit(ctx context.Context, projectID string, config metadata.JobConfig) (*metadata.Job, error) {
	jobID := uuid.New().String()
	now := time.Now()

	job := &metadata.Job{
		ProjectID:    projectID,
		JobID:        jobID,
		Config:       config,
		Status:       metadata.JobStatus{State: metadata.JobStatePending},
		CreationTime: now,
	}

	// Store in repository
	if err := m.repo.CreateJob(ctx, *job); err != nil {
		return nil, fmt.Errorf("create job: %w", err)
	}

	// Store in memory
	key := jobKey(projectID, jobID)
	m.mu.Lock()
	m.jobs[key] = job
	m.mu.Unlock()

	// For LOAD and EXTRACT jobs, mark as DONE immediately (emulator stub)
	if config.JobType == "LOAD" || config.JobType == "EXTRACT" {
		job.Status.State = metadata.JobStateDone
		job.StartTime = &now
		job.EndTime = &now
		_ = m.repo.UpdateJob(ctx, *job)
	}

	// Return a copy BEFORE launching goroutine to avoid data races
	cp := *job

	// For QUERY jobs, launch async execution
	if config.JobType == "QUERY" && config.Query != nil {
		go m.executeQuery(projectID, jobID, config.Query.Query)
	}

	return &cp, nil
}

// executeQuery runs a query job asynchronously. It translates the SQL,
// executes it, stores results, and updates job status.
func (m *Manager) executeQuery(projectID, jobID, sql string) {
	key := jobKey(projectID, jobID)
	ctx := context.Background()

	// Transition to RUNNING
	m.mu.Lock()
	if j, ok := m.jobs[key]; ok {
		now := time.Now()
		j.Status.State = metadata.JobStateRunning
		j.StartTime = &now
	}
	m.mu.Unlock()

	// Translate SQL
	translated, err := m.translator.Translate(sql)
	if err != nil {
		m.failJob(key, ctx, "invalidQuery", fmt.Sprintf("SQL translation error: %s", err.Error()))
		return
	}

	// Execute query
	result, err := m.executor.Query(ctx, translated)
	if err != nil {
		m.failJob(key, ctx, "invalidQuery", err.Error())
		return
	}

	// Store result and mark DONE
	now := time.Now()
	m.mu.Lock()
	m.results[key] = result
	if j, ok := m.jobs[key]; ok {
		j.Status.State = metadata.JobStateDone
		j.EndTime = &now
		j.Statistics = metadata.JobStatistics{
			Query: &metadata.QueryStatistics{
				TotalBytesProcessed: 0,
				CacheHit:            false,
				StatementType:       "SELECT",
			},
		}
		// Persist updated status
		_ = m.repo.UpdateJob(ctx, *j)
	}
	m.mu.Unlock()

	m.logger.Debug("query job completed", zap.String("jobID", jobID), zap.Uint64("rows", result.TotalRows))
}

// failJob marks a job as DONE with an error.
func (m *Manager) failJob(key string, ctx context.Context, reason, message string) {
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()

	if j, ok := m.jobs[key]; ok {
		j.Status.State = metadata.JobStateDone
		j.Status.ErrorResult = &metadata.JobError{
			Reason:  reason,
			Message: message,
		}
		j.EndTime = &now
		_ = m.repo.UpdateJob(ctx, *j)
	}
}

// Get returns a job by ID. Returns a snapshot copy to avoid data races
// with the async execution goroutine.
func (m *Manager) Get(ctx context.Context, projectID, jobID string) (*metadata.Job, error) {
	key := jobKey(projectID, jobID)

	m.mu.RLock()
	job, ok := m.jobs[key]
	if ok {
		// Return a copy to avoid data races with async goroutine
		cp := *job
		m.mu.RUnlock()
		return &cp, nil
	}
	m.mu.RUnlock()

	// Fall back to repository
	return m.repo.GetJob(ctx, projectID, jobID)
}

// List returns jobs for a project.
func (m *Manager) List(ctx context.Context, projectID string) ([]*metadata.Job, error) {
	repoJobs, err := m.repo.ListJobs(ctx, projectID)
	if err != nil {
		return nil, err
	}

	// Convert []*metadata.Job from repo (which returns []metadata.Job)
	// The in-memory map may have more up-to-date status, so merge.
	// Return copies to avoid data races with async goroutines.
	result := make([]*metadata.Job, 0, len(repoJobs))
	m.mu.RLock()
	for i := range repoJobs {
		key := jobKey(projectID, repoJobs[i].JobID)
		if memJob, ok := m.jobs[key]; ok {
			cp := *memJob
			result = append(result, &cp)
		} else {
			j := repoJobs[i]
			result = append(result, &j)
		}
	}
	m.mu.RUnlock()

	return result, nil
}

// Cancel attempts to cancel a running job. Since we don't have context
// cancellation wired through to the DuckDB query, this is best-effort:
// it marks the job as DONE.
func (m *Manager) Cancel(ctx context.Context, projectID, jobID string) error {
	key := jobKey(projectID, jobID)
	now := time.Now()

	m.mu.Lock()
	job, ok := m.jobs[key]
	if ok {
		job.Status.State = metadata.JobStateDone
		job.EndTime = &now
		_ = m.repo.UpdateJob(ctx, *job)
	}
	m.mu.Unlock()

	if !ok {
		// Try repo
		j, err := m.repo.GetJob(ctx, projectID, jobID)
		if err != nil {
			return fmt.Errorf("job not found: %s", jobID)
		}
		j.Status.State = metadata.JobStateDone
		j.EndTime = &now
		return m.repo.UpdateJob(ctx, *j)
	}

	return nil
}

// GetQueryResults returns the results of a completed query job.
// If the job is not yet complete, it returns a partial result with JobComplete=false.
// startIndex and maxResults control pagination over the stored result set.
func (m *Manager) GetQueryResults(ctx context.Context, projectID, jobID string, startIndex, maxResults int) (*query.QueryResult, error) {
	key := jobKey(projectID, jobID)

	m.mu.RLock()
	job, jobOK := m.jobs[key]
	result, resOK := m.results[key]
	m.mu.RUnlock()

	// If job not in memory, check repo
	if !jobOK {
		j, err := m.repo.GetJob(ctx, projectID, jobID)
		if err != nil {
			return nil, fmt.Errorf("job not found: %s", jobID)
		}
		job = j
	}

	// Job not done yet — return incomplete
	if job.Status.State != metadata.JobStateDone || !resOK {
		return &query.QueryResult{
			JobComplete: false,
		}, nil
	}

	// Apply pagination
	totalRows := int(result.TotalRows)
	if startIndex >= totalRows {
		return &query.QueryResult{
			Schema:      result.Schema,
			Rows:        nil,
			TotalRows:   result.TotalRows,
			JobComplete: true,
		}, nil
	}

	end := startIndex + maxResults
	if end > totalRows {
		end = totalRows
	}

	return &query.QueryResult{
		Schema:      result.Schema,
		Rows:        result.Rows[startIndex:end],
		TotalRows:   result.TotalRows,
		JobComplete: true,
	}, nil
}
