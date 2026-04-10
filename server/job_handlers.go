package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/sathish/bigquery-emulator/pkg/metadata"
	"github.com/sathish/bigquery-emulator/server/apierror"
)

// jobInsertRequest represents the JSON body for POST /jobs.
type jobInsertRequest struct {
	JobReference *struct {
		ProjectID string `json:"projectId"`
		JobID     string `json:"jobId"`
	} `json:"jobReference,omitempty"`
	Configuration struct {
		JobType string `json:"jobType,omitempty"`
		Query   *struct {
			Query        string `json:"query"`
			UseLegacySQL *bool  `json:"useLegacySql,omitempty"`
		} `json:"query,omitempty"`
	} `json:"configuration"`
}

// jobToJSON converts a metadata.Job to a BigQuery-compatible JSON map.
func jobToJSON(j *metadata.Job) map[string]interface{} {
	result := map[string]interface{}{
		"kind": "bigquery#job",
		"id":   fmt.Sprintf("%s:%s", j.ProjectID, j.JobID),
		"jobReference": map[string]interface{}{
			"projectId": j.ProjectID,
			"jobId":     j.JobID,
		},
		"status": map[string]interface{}{
			"state": j.Status.State,
		},
		"configuration": map[string]interface{}{},
	}

	// Add error result if present
	if j.Status.ErrorResult != nil {
		status := result["status"].(map[string]interface{})
		status["errorResult"] = map[string]interface{}{
			"reason":  j.Status.ErrorResult.Reason,
			"message": j.Status.ErrorResult.Message,
		}
	}

	// Add query configuration if present
	if j.Config.Query != nil {
		config := result["configuration"].(map[string]interface{})
		config["jobType"] = "QUERY"
		config["query"] = map[string]interface{}{
			"query":        j.Config.Query.Query,
			"useLegacySql": j.Config.Query.UseLegacySQL,
		}
	}

	// Add statistics
	stats := map[string]interface{}{
		"creationTime": fmt.Sprintf("%d", j.CreationTime.UnixMilli()),
	}
	if j.StartTime != nil {
		stats["startTime"] = fmt.Sprintf("%d", j.StartTime.UnixMilli())
	}
	if j.EndTime != nil {
		stats["endTime"] = fmt.Sprintf("%d", j.EndTime.UnixMilli())
	}
	result["statistics"] = stats

	return result
}

// insertJob handles POST /bigquery/v2/projects/{projectId}/jobs
func (s *Server) insertJob(w http.ResponseWriter, r *http.Request) {
	projectID := chi.URLParam(r, "projectId")

	var req jobInsertRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		apierror.NewBadRequestError("Invalid JSON body: " + err.Error()).WriteResponse(w)
		return
	}

	if req.Configuration.Query == nil {
		apierror.NewBadRequestError("Only query jobs are supported; configuration.query is required").WriteResponse(w)
		return
	}

	useLegacySQL := false
	if req.Configuration.Query.UseLegacySQL != nil {
		useLegacySQL = *req.Configuration.Query.UseLegacySQL
	}

	config := metadata.JobConfig{
		JobType: "QUERY",
		Query: &metadata.QueryConfig{
			Query:        req.Configuration.Query.Query,
			UseLegacySQL: useLegacySQL,
		},
	}

	job, err := s.jobMgr.Submit(r.Context(), projectID, config)
	if err != nil {
		apierror.NewInternalError("Failed to submit job: " + err.Error()).WriteResponse(w)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(jobToJSON(job))
}

// getJob handles GET /bigquery/v2/projects/{projectId}/jobs/{jobId}
func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	projectID := chi.URLParam(r, "projectId")
	jobID := chi.URLParam(r, "jobId")

	job, err := s.jobMgr.Get(r.Context(), projectID, jobID)
	if err != nil {
		apierror.NewNotFoundError("Job", fmt.Sprintf("%s:%s", projectID, jobID)).WriteResponse(w)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(jobToJSON(job))
}

// listJobs handles GET /bigquery/v2/projects/{projectId}/jobs
func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	projectID := chi.URLParam(r, "projectId")

	jobs, err := s.jobMgr.List(r.Context(), projectID)
	if err != nil {
		apierror.NewInternalError("Failed to list jobs: " + err.Error()).WriteResponse(w)
		return
	}

	jobList := make([]map[string]interface{}, 0, len(jobs))
	for _, j := range jobs {
		jobList = append(jobList, jobToJSON(j))
	}

	resp := map[string]interface{}{
		"kind": "bigquery#jobList",
		"jobs": jobList,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// cancelJob handles POST /bigquery/v2/projects/{projectId}/jobs/{jobId}/cancel
func (s *Server) cancelJob(w http.ResponseWriter, r *http.Request) {
	projectID := chi.URLParam(r, "projectId")
	jobID := chi.URLParam(r, "jobId")

	if err := s.jobMgr.Cancel(r.Context(), projectID, jobID); err != nil {
		apierror.NewNotFoundError("Job", fmt.Sprintf("%s:%s", projectID, jobID)).WriteResponse(w)
		return
	}

	// Return the updated job
	job, err := s.jobMgr.Get(r.Context(), projectID, jobID)
	if err != nil {
		apierror.NewInternalError("Failed to get job after cancel").WriteResponse(w)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(jobToJSON(job))
}

// getQueryResults handles GET /bigquery/v2/projects/{projectId}/queries/{jobId}
func (s *Server) getQueryResults(w http.ResponseWriter, r *http.Request) {
	projectID := chi.URLParam(r, "projectId")
	jobID := chi.URLParam(r, "jobId")

	startIndex := 0
	maxResults := 1000 // default page size

	if v := r.URL.Query().Get("startIndex"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed >= 0 {
			startIndex = parsed
		}
	}
	if v := r.URL.Query().Get("maxResults"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
			maxResults = parsed
		}
	}

	result, err := s.jobMgr.GetQueryResults(r.Context(), projectID, jobID, startIndex, maxResults)
	if err != nil {
		apierror.NewNotFoundError("Job", fmt.Sprintf("%s:%s", projectID, jobID)).WriteResponse(w)
		return
	}

	resp := map[string]interface{}{
		"kind": "bigquery#getQueryResultsResponse",
		"jobReference": map[string]interface{}{
			"projectId": projectID,
			"jobId":     jobID,
		},
		"jobComplete": result.JobComplete,
	}

	if result.JobComplete {
		resp["totalRows"] = fmt.Sprintf("%d", result.TotalRows)

		// Convert schema
		if len(result.Schema) > 0 {
			fields := make([]map[string]interface{}, len(result.Schema))
			for i, col := range result.Schema {
				fields[i] = map[string]interface{}{
					"name": col.Name,
					"type": col.Type,
					"mode": "NULLABLE",
				}
			}
			resp["schema"] = map[string]interface{}{
				"fields": fields,
			}
		}

		// Convert rows to BQ format: {"f": [{"v": "value1"}, {"v": "value2"}]}
		rows := rowsToBQFormat(result.Rows)
		resp["rows"] = rows

		// Page token: if there are more rows beyond this page
		nextIndex := startIndex + len(result.Rows)
		if uint64(nextIndex) < result.TotalRows {
			resp["pageToken"] = fmt.Sprintf("%d", nextIndex)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// rowsToBQFormat and formatValue are defined in helpers.go

// queriesInsertRequest represents the JSON body for POST /queries (synchronous query).
type queriesInsertRequest struct {
	Query        string `json:"query"`
	UseLegacySQL *bool  `json:"useLegacySql,omitempty"`
	MaxResults   *int   `json:"maxResults,omitempty"`
	TimeoutMs    *int   `json:"timeoutMs,omitempty"`
}

// queriesInsert handles POST /bigquery/v2/projects/{projectId}/queries
// This is BigQuery's synchronous query API — runs the query and returns results inline.
func (s *Server) queriesInsert(w http.ResponseWriter, r *http.Request) {
	projectID := chi.URLParam(r, "projectId")

	var req queriesInsertRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		apierror.NewBadRequestError("Invalid JSON body: " + err.Error()).WriteResponse(w)
		return
	}

	if req.Query == "" {
		apierror.NewBadRequestError("query field is required").WriteResponse(w)
		return
	}

	// Translate BQ SQL to DuckDB SQL
	translated, err := s.translator.Translate(req.Query)
	if err != nil {
		apierror.NewBadRequestError("SQL translation error: " + err.Error()).WriteResponse(w)
		return
	}

	// Execute synchronously
	result, err := s.executor.Query(r.Context(), translated)
	if err != nil {
		// Submit as job so caller gets a jobReference with error details
		config := metadata.JobConfig{
			JobType: "QUERY",
			Query:   &metadata.QueryConfig{Query: req.Query},
		}
		job, submitErr := s.jobMgr.Submit(r.Context(), projectID, config)
		if submitErr != nil {
			apierror.NewInternalError("Query failed: " + err.Error()).WriteResponse(w)
			return
		}
		// Return partial response with jobComplete=false so SDK retries via getQueryResults
		resp := map[string]interface{}{
			"kind": "bigquery#queryResponse",
			"jobReference": map[string]interface{}{
				"projectId": projectID,
				"jobId":     job.JobID,
			},
			"jobComplete": false,
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
		return
	}

	// Apply maxResults if set
	rows := result.Rows
	maxResults := len(rows)
	if req.MaxResults != nil && *req.MaxResults > 0 && *req.MaxResults < maxResults {
		maxResults = *req.MaxResults
		rows = rows[:maxResults]
	}

	// Build response
	resp := map[string]interface{}{
		"kind":        "bigquery#queryResponse",
		"jobComplete": true,
		"totalRows":   fmt.Sprintf("%d", result.TotalRows),
	}

	// We still need a jobReference — create a completed job record
	config := metadata.JobConfig{
		JobType: "QUERY",
		Query:   &metadata.QueryConfig{Query: req.Query},
	}
	job, _ := s.jobMgr.Submit(r.Context(), projectID, config)
	if job != nil {
		resp["jobReference"] = map[string]interface{}{
			"projectId": projectID,
			"jobId":     job.JobID,
		}
	}

	// Schema
	if len(result.Schema) > 0 {
		fields := make([]map[string]interface{}, len(result.Schema))
		for i, col := range result.Schema {
			fields[i] = map[string]interface{}{
				"name": col.Name,
				"type": col.Type,
				"mode": "NULLABLE",
			}
		}
		resp["schema"] = map[string]interface{}{
			"fields": fields,
		}
	}

	// Rows in BQ format
	resp["rows"] = rowsToBQFormat(rows)

	// Page token if truncated
	if len(result.Rows) > maxResults {
		resp["pageToken"] = fmt.Sprintf("%d", maxResults)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
