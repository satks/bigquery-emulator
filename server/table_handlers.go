package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/sathish/bigquery-emulator/pkg/metadata"
	"github.com/sathish/bigquery-emulator/server/apierror"
	"go.uber.org/zap"
)

// tableRequest represents the JSON body for creating/updating a table.
type tableRequest struct {
	TableReference struct {
		ProjectID string `json:"projectId"`
		DatasetID string `json:"datasetId"`
		TableID   string `json:"tableId"`
	} `json:"tableReference"`
	Schema *struct {
		Fields []fieldRequest `json:"fields"`
	} `json:"schema,omitempty"`
	Description    string            `json:"description,omitempty"`
	Labels         map[string]string `json:"labels,omitempty"`
	Type           string            `json:"type,omitempty"`
	ExpirationTime string            `json:"expirationTime,omitempty"`
	View           *struct {
		Query string `json:"query"`
	} `json:"view,omitempty"`
}

// fieldRequest represents a field in the request schema.
type fieldRequest struct {
	Name        string         `json:"name"`
	Type        string         `json:"type"`
	Mode        string         `json:"mode,omitempty"`
	Description string         `json:"description,omitempty"`
	Fields      []fieldRequest `json:"fields,omitempty"`
}

// tableResponse represents the BigQuery API JSON format for a table.
type tableResponse struct {
	Kind           string                 `json:"kind"`
	ID             string                 `json:"id"`
	TableReference map[string]string      `json:"tableReference"`
	Type           string                 `json:"type"`
	Schema         *schemaResponse        `json:"schema,omitempty"`
	Description    string                 `json:"description,omitempty"`
	Labels         map[string]string      `json:"labels,omitempty"`
	CreationTime   string                 `json:"creationTime"`
	LastModified   string                 `json:"lastModifiedTime"`
	NumBytes       string                 `json:"numBytes"`
	NumRows        string                 `json:"numRows"`
	View           map[string]interface{} `json:"view,omitempty"`
}

// schemaResponse represents the schema portion of a table response.
type schemaResponse struct {
	Fields []fieldResponse `json:"fields"`
}

// fieldResponse represents a field in the response schema.
type fieldResponse struct {
	Name        string          `json:"name"`
	Type        string          `json:"type"`
	Mode        string          `json:"mode,omitempty"`
	Description string          `json:"description,omitempty"`
	Fields      []fieldResponse `json:"fields,omitempty"`
}

// tableListResponse represents the BigQuery API JSON format for listing tables.
type tableListResponse struct {
	Kind       string          `json:"kind"`
	Tables     []tableResponse `json:"tables"`
	TotalItems int             `json:"totalItems"`
}

// createTable handles POST /bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables
func (s *Server) createTable(w http.ResponseWriter, r *http.Request) {
	projectID := chi.URLParam(r, "projectId")
	datasetID := chi.URLParam(r, "datasetId")

	var req tableRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		apiErr := apierror.NewBadRequestError(fmt.Sprintf("Invalid JSON: %v", err))
		apiErr.WriteResponse(w)
		return
	}

	tableID := req.TableReference.TableID
	if tableID == "" {
		apiErr := apierror.NewBadRequestError("tableReference.tableId is required")
		apiErr.WriteResponse(w)
		return
	}

	// Determine table type
	tableType := req.Type
	if tableType == "" {
		tableType = "TABLE"
	}

	// Build metadata.Table
	now := time.Now()
	tbl := metadata.Table{
		ProjectID:        projectID,
		DatasetID:        datasetID,
		TableID:          tableID,
		Type:             tableType,
		Description:      req.Description,
		Labels:           req.Labels,
		CreationTime:     now,
		LastModifiedTime: now,
	}

	// Parse view query for VIEW type
	if tableType == "VIEW" && req.View != nil {
		tbl.ViewQuery = req.View.Query
	}

	// Parse expiration time
	if req.ExpirationTime != "" {
		// BQ API sends expiration as milliseconds since epoch string
		var ms int64
		if _, err := fmt.Sscanf(req.ExpirationTime, "%d", &ms); err == nil {
			expTime := time.UnixMilli(ms)
			tbl.ExpirationTime = &expTime
		}
	}

	// Convert schema fields
	if req.Schema != nil {
		tbl.Schema = &metadata.TableSchema{
			Fields: convertFieldsFromRequest(req.Schema.Fields),
		}
	}

	// Create table via repository
	if err := s.repo.CreateTable(r.Context(), tbl); err != nil {
		if isAlreadyExistsError(err) {
			apiErr := apierror.NewAlreadyExistsError("Table", fmt.Sprintf("%s:%s.%s", projectID, datasetID, tableID))
			apiErr.WriteResponse(w)
			return
		}
		s.logger.Error("failed to create table", zap.Error(err))
		apiErr := apierror.NewInternalError(fmt.Sprintf("Failed to create table: %v", err))
		apiErr.WriteResponse(w)
		return
	}

	resp := buildTableResponse(&tbl)
	writeJSON(w, http.StatusOK, resp)
}

// getTable handles GET /bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables/{tableId}
func (s *Server) getTable(w http.ResponseWriter, r *http.Request) {
	projectID := chi.URLParam(r, "projectId")
	datasetID := chi.URLParam(r, "datasetId")
	tableID := chi.URLParam(r, "tableId")

	tbl, err := s.repo.GetTable(r.Context(), projectID, datasetID, tableID)
	if err != nil {
		if isNotFoundError(err) {
			apiErr := apierror.NewNotFoundError("Table", fmt.Sprintf("%s:%s.%s", projectID, datasetID, tableID))
			apiErr.WriteResponse(w)
			return
		}
		s.logger.Error("failed to get table", zap.Error(err))
		apiErr := apierror.NewInternalError(fmt.Sprintf("Failed to get table: %v", err))
		apiErr.WriteResponse(w)
		return
	}

	resp := buildTableResponse(tbl)
	writeJSON(w, http.StatusOK, resp)
}

// listTables handles GET /bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables
func (s *Server) listTables(w http.ResponseWriter, r *http.Request) {
	projectID := chi.URLParam(r, "projectId")
	datasetID := chi.URLParam(r, "datasetId")

	tables, err := s.repo.ListTables(r.Context(), projectID, datasetID)
	if err != nil {
		s.logger.Error("failed to list tables", zap.Error(err))
		apiErr := apierror.NewInternalError(fmt.Sprintf("Failed to list tables: %v", err))
		apiErr.WriteResponse(w)
		return
	}

	tableResponses := make([]tableResponse, 0, len(tables))
	for i := range tables {
		tableResponses = append(tableResponses, buildTableResponse(&tables[i]))
	}

	resp := tableListResponse{
		Kind:       "bigquery#tableList",
		Tables:     tableResponses,
		TotalItems: len(tables),
	}

	writeJSON(w, http.StatusOK, resp)
}

// patchTable handles PATCH /bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables/{tableId}
func (s *Server) patchTable(w http.ResponseWriter, r *http.Request) {
	projectID := chi.URLParam(r, "projectId")
	datasetID := chi.URLParam(r, "datasetId")
	tableID := chi.URLParam(r, "tableId")

	// Get existing table
	existing, err := s.repo.GetTable(r.Context(), projectID, datasetID, tableID)
	if err != nil {
		if isNotFoundError(err) {
			apiErr := apierror.NewNotFoundError("Table", fmt.Sprintf("%s:%s.%s", projectID, datasetID, tableID))
			apiErr.WriteResponse(w)
			return
		}
		s.logger.Error("failed to get table for patch", zap.Error(err))
		apiErr := apierror.NewInternalError(fmt.Sprintf("Failed to get table: %v", err))
		apiErr.WriteResponse(w)
		return
	}

	// Parse patch body
	var req tableRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		apiErr := apierror.NewBadRequestError(fmt.Sprintf("Invalid JSON: %v", err))
		apiErr.WriteResponse(w)
		return
	}

	// Merge: only update fields that are present in the patch
	if req.Description != "" {
		existing.Description = req.Description
	}
	if req.Labels != nil {
		existing.Labels = req.Labels
	}
	if req.Schema != nil {
		existing.Schema = &metadata.TableSchema{
			Fields: convertFieldsFromRequest(req.Schema.Fields),
		}
	}
	if req.ExpirationTime != "" {
		var ms int64
		if _, err := fmt.Sscanf(req.ExpirationTime, "%d", &ms); err == nil {
			expTime := time.UnixMilli(ms)
			existing.ExpirationTime = &expTime
		}
	}

	existing.LastModifiedTime = time.Now()

	// Update via repository
	if err := s.repo.UpdateTable(r.Context(), *existing); err != nil {
		s.logger.Error("failed to update table", zap.Error(err))
		apiErr := apierror.NewInternalError(fmt.Sprintf("Failed to update table: %v", err))
		apiErr.WriteResponse(w)
		return
	}

	resp := buildTableResponse(existing)
	writeJSON(w, http.StatusOK, resp)
}

// deleteTable handles DELETE /bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables/{tableId}
func (s *Server) deleteTable(w http.ResponseWriter, r *http.Request) {
	projectID := chi.URLParam(r, "projectId")
	datasetID := chi.URLParam(r, "datasetId")
	tableID := chi.URLParam(r, "tableId")

	if err := s.repo.DeleteTable(r.Context(), projectID, datasetID, tableID); err != nil {
		if isNotFoundError(err) {
			apiErr := apierror.NewNotFoundError("Table", fmt.Sprintf("%s:%s.%s", projectID, datasetID, tableID))
			apiErr.WriteResponse(w)
			return
		}
		s.logger.Error("failed to delete table", zap.Error(err))
		apiErr := apierror.NewInternalError(fmt.Sprintf("Failed to delete table: %v", err))
		apiErr.WriteResponse(w)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// buildTableResponse converts a metadata.Table to the BigQuery API response format.
func buildTableResponse(tbl *metadata.Table) tableResponse {
	resp := tableResponse{
		Kind: "bigquery#table",
		ID:   fmt.Sprintf("%s:%s.%s", tbl.ProjectID, tbl.DatasetID, tbl.TableID),
		TableReference: map[string]string{
			"projectId": tbl.ProjectID,
			"datasetId": tbl.DatasetID,
			"tableId":   tbl.TableID,
		},
		Type:         tbl.Type,
		Description:  tbl.Description,
		Labels:       tbl.Labels,
		CreationTime: fmt.Sprintf("%d", tbl.CreationTime.UnixMilli()),
		LastModified: fmt.Sprintf("%d", tbl.LastModifiedTime.UnixMilli()),
		NumBytes:     fmt.Sprintf("%d", tbl.NumBytes),
		NumRows:      fmt.Sprintf("%d", tbl.NumRows),
	}

	if tbl.Schema != nil {
		resp.Schema = &schemaResponse{
			Fields: convertFieldsToResponse(tbl.Schema.Fields),
		}
	}

	if tbl.ViewQuery != "" {
		resp.View = map[string]interface{}{
			"query": tbl.ViewQuery,
		}
	}

	return resp
}

// convertFieldsFromRequest converts request field schemas to metadata field schemas.
func convertFieldsFromRequest(fields []fieldRequest) []metadata.FieldSchema {
	result := make([]metadata.FieldSchema, len(fields))
	for i, f := range fields {
		result[i] = metadata.FieldSchema{
			Name:        f.Name,
			Type:        f.Type,
			Mode:        f.Mode,
			Description: f.Description,
		}
		if len(f.Fields) > 0 {
			result[i].Fields = convertFieldsFromRequest(f.Fields)
		}
	}
	return result
}

// convertFieldsToResponse converts metadata field schemas to response field schemas.
func convertFieldsToResponse(fields []metadata.FieldSchema) []fieldResponse {
	result := make([]fieldResponse, len(fields))
	for i, f := range fields {
		result[i] = fieldResponse{
			Name:        f.Name,
			Type:        f.Type,
			Mode:        f.Mode,
			Description: f.Description,
		}
		if len(f.Fields) > 0 {
			result[i].Fields = convertFieldsToResponse(f.Fields)
		}
	}
	return result
}

// writeJSON, isAlreadyExistsError, and isNotFoundError are in helpers.go
