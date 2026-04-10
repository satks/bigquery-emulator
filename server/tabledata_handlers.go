package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/sathish/bigquery-emulator/server/apierror"
)

// insertAllRequest represents the JSON body for POST .../insertAll.
type insertAllRequest struct {
	Rows []insertAllRow `json:"rows"`
}

// insertAllRow represents a single row in an insertAll request.
type insertAllRow struct {
	InsertID string                 `json:"insertId,omitempty"`
	JSON     map[string]interface{} `json:"json"`
}

// listTableData handles GET /bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables/{tableId}/data
func (s *Server) listTableData(w http.ResponseWriter, r *http.Request) {
	projectID := chi.URLParam(r, "projectId")
	datasetID := chi.URLParam(r, "datasetId")
	tableID := chi.URLParam(r, "tableId")

	// Verify table exists in metadata
	_, err := s.repo.GetTable(r.Context(), projectID, datasetID, tableID)
	if err != nil {
		apierror.NewNotFoundError("Table", fmt.Sprintf("%s:%s.%s", projectID, datasetID, tableID)).WriteResponse(w)
		return
	}

	maxResults := 1000
	startIndex := 0

	if v := r.URL.Query().Get("maxResults"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
			maxResults = parsed
		}
	}
	// Support both pageToken (opaque base64) and startIndex (integer) for backwards compat
	if pt := r.URL.Query().Get("pageToken"); pt != "" {
		startIndex = decodePageToken(pt)
	} else if v := r.URL.Query().Get("startIndex"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed >= 0 {
			startIndex = parsed
		}
	}

	// Query the table with pagination
	sql := fmt.Sprintf(`SELECT * FROM "%s"."%s"`, datasetID, tableID)
	result, err := s.executor.QueryPage(r.Context(), sql, startIndex, maxResults)
	if err != nil {
		apierror.NewInternalError("Failed to read table data: " + err.Error()).WriteResponse(w)
		return
	}

	// Also get total count
	countSQL := fmt.Sprintf(`SELECT COUNT(*) AS cnt FROM "%s"."%s"`, datasetID, tableID)
	countResult, err := s.executor.Query(r.Context(), countSQL)
	if err != nil {
		apierror.NewInternalError("Failed to count table rows: " + err.Error()).WriteResponse(w)
		return
	}

	totalRows := uint64(0)
	if len(countResult.Rows) > 0 && len(countResult.Rows[0]) > 0 {
		if v, ok := countResult.Rows[0][0].(int64); ok {
			totalRows = uint64(v)
		}
	}

	// Build response
	resp := map[string]interface{}{
		"kind":      "bigquery#tableDataList",
		"totalRows": fmt.Sprintf("%d", totalRows),
	}

	rows := rowsToBQFormat(result.Rows)
	resp["rows"] = rows

	// Page token (opaque base64 for tabledata)
	nextIndex := startIndex + len(result.Rows)
	if uint64(nextIndex) < totalRows {
		resp["pageToken"] = encodePageToken(nextIndex)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// insertAll handles POST /bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables/{tableId}/insertAll
func (s *Server) insertAll(w http.ResponseWriter, r *http.Request) {
	projectID := chi.URLParam(r, "projectId")
	datasetID := chi.URLParam(r, "datasetId")
	tableID := chi.URLParam(r, "tableId")

	// Verify table exists in metadata
	_, err := s.repo.GetTable(r.Context(), projectID, datasetID, tableID)
	if err != nil {
		apierror.NewNotFoundError("Table", fmt.Sprintf("%s:%s.%s", projectID, datasetID, tableID)).WriteResponse(w)
		return
	}

	var req insertAllRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		apierror.NewBadRequestError("Invalid JSON body: " + err.Error()).WriteResponse(w)
		return
	}

	var insertErrors []map[string]interface{}

	for idx, row := range req.Rows {
		if len(row.JSON) == 0 {
			continue
		}

		// Build INSERT statement with sorted column names for deterministic output
		columns := make([]string, 0, len(row.JSON))
		for col := range row.JSON {
			columns = append(columns, col)
		}
		sort.Strings(columns)

		quotedCols := make([]string, len(columns))
		placeholders := make([]string, len(columns))
		values := make([]interface{}, len(columns))

		for i, col := range columns {
			quotedCols[i] = `"` + col + `"`
			placeholders[i] = "?"
			values[i] = row.JSON[col]
		}

		sql := fmt.Sprintf(`INSERT INTO "%s"."%s" (%s) VALUES (%s)`,
			datasetID, tableID,
			strings.Join(quotedCols, ", "),
			strings.Join(placeholders, ", "),
		)

		_, err := s.executor.Execute(r.Context(), sql, values...)
		if err != nil {
			insertErrors = append(insertErrors, map[string]interface{}{
				"index": idx,
				"errors": []map[string]interface{}{
					{
						"reason":  "insertFailed",
						"message": err.Error(),
					},
				},
			})
		}
	}

	resp := map[string]interface{}{
		"kind":         "bigquery#tableDataInsertAllResponse",
		"insertErrors": insertErrors,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
