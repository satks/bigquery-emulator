package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/sathish/bigquery-emulator/pkg/metadata"
	"github.com/sathish/bigquery-emulator/pkg/query"
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
		Load *struct {
			DestinationTable struct {
				ProjectID string `json:"projectId"`
				DatasetID string `json:"datasetId"`
				TableID   string `json:"tableId"`
			} `json:"destinationTable"`
			Schema *struct {
				Fields []metadata.FieldSchema `json:"fields"`
			} `json:"schema,omitempty"`
			SourceURIs       []string `json:"sourceUris,omitempty"`
			SourceFormat     string   `json:"sourceFormat,omitempty"`
			WriteDisposition string   `json:"writeDisposition,omitempty"`
		} `json:"load,omitempty"`
		Extract *struct {
			SourceTable struct {
				ProjectID string `json:"projectId"`
				DatasetID string `json:"datasetId"`
				TableID   string `json:"tableId"`
			} `json:"sourceTable"`
			DestinationURIs   []string `json:"destinationUris,omitempty"`
			DestinationFormat string   `json:"destinationFormat,omitempty"`
		} `json:"extract,omitempty"`
	} `json:"configuration"`
}

// jobToJSON converts a metadata.Job to a BigQuery-compatible JSON map.
func jobToJSON(j *metadata.Job) map[string]interface{} {
	result := map[string]interface{}{
		"kind": "bigquery#job",
		"etag": generateEtag(j.ProjectID + ":" + j.JobID),
		"id":   fmt.Sprintf("%s:%s", j.ProjectID, j.JobID),
		"jobReference": map[string]interface{}{
			"projectId": j.ProjectID,
			"jobId":     j.JobID,
			"location":  "US",
		},
		"user_email": "emulator@localhost",
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

	// Add load configuration if present
	if j.Config.Load != nil {
		config := result["configuration"].(map[string]interface{})
		config["jobType"] = "LOAD"
		loadCfg := map[string]interface{}{
			"destinationTable": map[string]interface{}{
				"projectId": j.Config.Load.DestinationTable.ProjectID,
				"datasetId": j.Config.Load.DestinationTable.DatasetID,
				"tableId":   j.Config.Load.DestinationTable.TableID,
			},
		}
		if j.Config.Load.SourceFormat != "" {
			loadCfg["sourceFormat"] = j.Config.Load.SourceFormat
		}
		if len(j.Config.Load.SourceURIs) > 0 {
			loadCfg["sourceUris"] = j.Config.Load.SourceURIs
		}
		if j.Config.Load.WriteDisposition != "" {
			loadCfg["writeDisposition"] = j.Config.Load.WriteDisposition
		}
		if j.Config.Load.Schema != nil {
			loadCfg["schema"] = map[string]interface{}{
				"fields": j.Config.Load.Schema.Fields,
			}
		}
		config["load"] = loadCfg
	}

	// Add extract configuration if present
	if j.Config.Extract != nil {
		config := result["configuration"].(map[string]interface{})
		config["jobType"] = "EXTRACT"
		extractCfg := map[string]interface{}{
			"sourceTable": map[string]interface{}{
				"projectId": j.Config.Extract.SourceTable.ProjectID,
				"datasetId": j.Config.Extract.SourceTable.DatasetID,
				"tableId":   j.Config.Extract.SourceTable.TableID,
			},
		}
		if len(j.Config.Extract.DestinationURIs) > 0 {
			extractCfg["destinationUris"] = j.Config.Extract.DestinationURIs
		}
		if j.Config.Extract.DestinationFormat != "" {
			extractCfg["destinationFormat"] = j.Config.Extract.DestinationFormat
		}
		config["extract"] = extractCfg
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

	// Extract client-provided job ID if present
	var clientJobID string
	if req.JobReference != nil && req.JobReference.JobID != "" {
		clientJobID = req.JobReference.JobID
	}

	// Handle QUERY jobs
	if req.Configuration.Query != nil {
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

		job, err := s.jobMgr.SubmitWithID(r.Context(), projectID, clientJobID, config)
		if err != nil {
			apierror.NewInternalError("Failed to submit job: " + err.Error()).WriteResponse(w)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(jobToJSON(job))
		return
	}

	// Handle LOAD jobs
	if req.Configuration.Load != nil {
		loadCfg := req.Configuration.Load
		destTable := &metadata.TableReference{
			ProjectID: loadCfg.DestinationTable.ProjectID,
			DatasetID: loadCfg.DestinationTable.DatasetID,
			TableID:   loadCfg.DestinationTable.TableID,
		}
		if destTable.ProjectID == "" {
			destTable.ProjectID = projectID
		}

		// Create the destination table with schema if it doesn't exist
		if loadCfg.Schema != nil && len(loadCfg.Schema.Fields) > 0 {
			tbl := metadata.Table{
				ProjectID:    destTable.ProjectID,
				DatasetID:    destTable.DatasetID,
				TableID:      destTable.TableID,
				Type:         "TABLE",
				Schema:       &metadata.TableSchema{Fields: loadCfg.Schema.Fields},
				CreationTime: time.Now(),
				LastModifiedTime: time.Now(),
			}
			// Ignore already-exists errors — the table may already be there
			_ = s.repo.CreateTable(r.Context(), tbl)
		}

		metaLoadCfg := &metadata.LoadConfig{
			DestinationTable: destTable,
			SourceURIs:       loadCfg.SourceURIs,
			SourceFormat:     loadCfg.SourceFormat,
			WriteDisposition: loadCfg.WriteDisposition,
		}
		if loadCfg.Schema != nil {
			metaLoadCfg.Schema = &metadata.TableSchema{Fields: loadCfg.Schema.Fields}
		}

		config := metadata.JobConfig{
			JobType: "LOAD",
			Load:    metaLoadCfg,
		}

		job, err := s.jobMgr.SubmitWithID(r.Context(), projectID, clientJobID, config)
		if err != nil {
			apierror.NewInternalError("Failed to submit load job: " + err.Error()).WriteResponse(w)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(jobToJSON(job))
		return
	}

	// Handle EXTRACT jobs
	if req.Configuration.Extract != nil {
		extractCfg := req.Configuration.Extract

		config := metadata.JobConfig{
			JobType: "EXTRACT",
			Extract: &metadata.ExtractConfig{
				SourceTable: &metadata.TableReference{
					ProjectID: extractCfg.SourceTable.ProjectID,
					DatasetID: extractCfg.SourceTable.DatasetID,
					TableID:   extractCfg.SourceTable.TableID,
				},
				DestinationURIs:   extractCfg.DestinationURIs,
				DestinationFormat: extractCfg.DestinationFormat,
			},
		}

		job, err := s.jobMgr.SubmitWithID(r.Context(), projectID, clientJobID, config)
		if err != nil {
			apierror.NewInternalError("Failed to submit extract job: " + err.Error()).WriteResponse(w)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(jobToJSON(job))
		return
	}

	apierror.NewBadRequestError("Job configuration must include query, load, or extract").WriteResponse(w)
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

	// Apply stateFilter if provided
	if sf := r.URL.Query().Get("stateFilter"); sf != "" {
		filtered := make([]*metadata.Job, 0, len(jobs))
		for _, j := range jobs {
			if j.Status.State == sf {
				filtered = append(filtered, j)
			}
		}
		jobs = filtered
	}

	// Parse pagination parameters
	maxResults := 1000
	if mr := r.URL.Query().Get("maxResults"); mr != "" {
		if parsed, err := strconv.Atoi(mr); err == nil && parsed > 0 {
			maxResults = parsed
		}
	}

	startIndex := 0
	if pt := r.URL.Query().Get("pageToken"); pt != "" {
		startIndex = decodePageToken(pt)
	}

	// Apply pagination
	totalItems := len(jobs)
	if startIndex > totalItems {
		startIndex = totalItems
	}
	endIndex := startIndex + maxResults
	if endIndex > totalItems {
		endIndex = totalItems
	}
	pageJobs := jobs[startIndex:endIndex]

	jobList := make([]map[string]interface{}, 0, len(pageJobs))
	for _, j := range pageJobs {
		jobList = append(jobList, jobToJSON(j))
	}

	resp := map[string]interface{}{
		"kind": "bigquery#jobList",
		"jobs": jobList,
	}

	// Set next page token if there are more results
	if endIndex < totalItems {
		resp["nextPageToken"] = encodePageToken(endIndex)
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

	// Support both pageToken (opaque base64) and startIndex (integer) for backwards compat
	if pt := r.URL.Query().Get("pageToken"); pt != "" {
		startIndex = decodePageToken(pt)
	} else if v := r.URL.Query().Get("startIndex"); v != "" {
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
		"etag": generateEtag(projectID + ":" + jobID),
		"jobReference": map[string]interface{}{
			"projectId": projectID,
			"jobId":     jobID,
			"location":  "US",
		},
		"jobComplete": result.JobComplete,
		"cacheHit":    false,
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
			resp["pageToken"] = encodePageToken(nextIndex)
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
// It classifies the SQL to use the correct execution method (Query for SELECT, Execute for DDL/DML)
// and creates a completed job record without re-executing via jobMgr.Submit.
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

	// Classify the SQL to determine execution path
	classification := query.ClassifySQL(req.Query)

	config := metadata.JobConfig{
		JobType: "QUERY",
		Query:   &metadata.QueryConfig{Query: req.Query},
	}

	if classification.IsQuery {
		// SELECT — use Query() to get rows back
		result, err := s.executor.Query(r.Context(), translated)
		if err != nil {
			apierror.NewInternalError("Query failed: " + err.Error()).WriteResponse(w)
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
			"cacheHit":    false,
		}

		// Create a completed job record (no re-execution)
		job, _ := s.jobMgr.CreateCompleted(r.Context(), projectID, config, result)
		if job != nil {
			resp["jobReference"] = map[string]interface{}{
				"projectId": projectID,
				"jobId":     job.JobID,
				"location":  "US",
			}
			resp["etag"] = generateEtag(projectID + ":" + job.JobID)
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
			resp["pageToken"] = encodePageToken(maxResults)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	} else {
		// DDL or DML — use Execute() (no rows returned)
		execResult, err := s.executor.Execute(r.Context(), translated)
		if err != nil {
			apierror.NewInternalError("Statement execution failed: " + err.Error()).WriteResponse(w)
			return
		}

		// Build response for DDL/DML
		dmlResult := &query.QueryResult{
			Schema:      nil,
			Rows:        nil,
			TotalRows:   uint64(execResult.RowsAffected),
			JobComplete: true,
		}

		resp := map[string]interface{}{
			"kind":        "bigquery#queryResponse",
			"jobComplete": true,
			"totalRows":   fmt.Sprintf("%d", execResult.RowsAffected),
			"cacheHit":    false,
		}

		// Create a completed job record (no re-execution)
		job, _ := s.jobMgr.CreateCompleted(r.Context(), projectID, config, dmlResult)
		if job != nil {
			resp["jobReference"] = map[string]interface{}{
				"projectId": projectID,
				"jobId":     job.JobID,
				"location":  "US",
			}
			resp["etag"] = generateEtag(projectID + ":" + job.JobID)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}
}
