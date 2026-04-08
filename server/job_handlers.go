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
