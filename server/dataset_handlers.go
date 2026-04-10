package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/sathish/bigquery-emulator/pkg/metadata"
	"github.com/sathish/bigquery-emulator/server/apierror"
	"go.uber.org/zap"
)

// createDatasetRequest represents the JSON body for creating a dataset.
type createDatasetRequest struct {
	DatasetReference struct {
		ProjectID string `json:"projectId"`
		DatasetID string `json:"datasetId"`
	} `json:"datasetReference"`
	FriendlyName           string            `json:"friendlyName"`
	Description            string            `json:"description"`
	Location               string            `json:"location"`
	Labels                 map[string]string `json:"labels"`
	Access                 []metadata.AccessEntry `json:"access"`
	DefaultTableExpirationMs string          `json:"defaultTableExpirationMs"`
}

// patchDatasetRequest represents the JSON body for patching a dataset.
// All fields are pointers so we can distinguish "not provided" from "set to zero value".
type patchDatasetRequest struct {
	FriendlyName           *string            `json:"friendlyName"`
	Description            *string            `json:"description"`
	Labels                 map[string]string  `json:"labels"`
	Access                 *[]metadata.AccessEntry `json:"access"`
	DefaultTableExpirationMs *string          `json:"defaultTableExpirationMs"`
}

// datasetResponse represents a BigQuery dataset in API response format.
type datasetResponse struct {
	Kind                     string            `json:"kind"`
	Etag                     string            `json:"etag"`
	ID                       string            `json:"id"`
	SelfLink                 string            `json:"selfLink"`
	DatasetReference         datasetRef        `json:"datasetReference"`
	FriendlyName             string            `json:"friendlyName,omitempty"`
	Description              string            `json:"description,omitempty"`
	Location                 string            `json:"location"`
	Labels                   map[string]string `json:"labels,omitempty"`
	CreationTime             string            `json:"creationTime"`
	LastModifiedTime         string            `json:"lastModifiedTime"`
	DefaultTableExpirationMs string            `json:"defaultTableExpirationMs,omitempty"`
	Access                   []metadata.AccessEntry `json:"access,omitempty"`
}

// datasetRef is the datasetReference sub-object in responses.
type datasetRef struct {
	ProjectID string `json:"projectId"`
	DatasetID string `json:"datasetId"`
}

// datasetListResponse represents the BigQuery dataset list API response.
type datasetListResponse struct {
	Kind       string             `json:"kind"`
	Datasets   []datasetResponse  `json:"datasets"`
	TotalItems int                `json:"totalItems"`
	NextPageToken string          `json:"nextPageToken,omitempty"`
}

// toDatasetResponse converts a metadata.Dataset to a BigQuery API dataset response.
func toDatasetResponse(d *metadata.Dataset) datasetResponse {
	location := d.Location
	if location == "" {
		location = "US"
	}

	resp := datasetResponse{
		Kind:     "bigquery#dataset",
		Etag:     generateEtag(d.ProjectID + ":" + d.DatasetID),
		ID:       fmt.Sprintf("%s:%s", d.ProjectID, d.DatasetID),
		SelfLink: fmt.Sprintf("http://bigquery.googleapis.com/bigquery/v2/projects/%s/datasets/%s", d.ProjectID, d.DatasetID),
		DatasetReference: datasetRef{
			ProjectID: d.ProjectID,
			DatasetID: d.DatasetID,
		},
		FriendlyName:     d.FriendlyName,
		Description:      d.Description,
		Location:         location,
		Labels:           d.Labels,
		CreationTime:     fmt.Sprintf("%d", d.CreationTime.UnixMilli()),
		LastModifiedTime: fmt.Sprintf("%d", d.LastModifiedTime.UnixMilli()),
		Access:           d.Access,
	}

	if d.DefaultTableExpiration != 0 {
		resp.DefaultTableExpirationMs = strconv.FormatInt(d.DefaultTableExpiration, 10)
	}

	return resp
}

// createDataset handles POST /bigquery/v2/projects/{projectId}/datasets
func (s *Server) createDataset(w http.ResponseWriter, r *http.Request) {
	projectID := chi.URLParam(r, "projectId")

	var req createDatasetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.logger.Debug("invalid JSON in createDataset request", zap.Error(err))
		apierror.NewBadRequestError("Invalid JSON in request body").WriteResponse(w)
		return
	}

	datasetID := req.DatasetReference.DatasetID
	if datasetID == "" {
		apierror.NewBadRequestError("datasetReference.datasetId is required").WriteResponse(w)
		return
	}

	// Parse defaultTableExpirationMs if provided
	var defaultExpMs int64
	if req.DefaultTableExpirationMs != "" {
		parsed, err := strconv.ParseInt(req.DefaultTableExpirationMs, 10, 64)
		if err != nil {
			apierror.NewBadRequestError("Invalid defaultTableExpirationMs value").WriteResponse(w)
			return
		}
		defaultExpMs = parsed
	}

	now := time.Now()
	dataset := metadata.Dataset{
		ProjectID:              projectID,
		DatasetID:              datasetID,
		FriendlyName:           req.FriendlyName,
		Description:            req.Description,
		Location:               req.Location,
		Labels:                 req.Labels,
		Access:                 req.Access,
		DefaultTableExpiration: defaultExpMs,
		CreationTime:           now,
		LastModifiedTime:       now,
	}

	if err := s.repo.CreateDataset(r.Context(), dataset); err != nil {
		if isAlreadyExistsError(err) {
			apierror.NewAlreadyExistsError("Dataset", fmt.Sprintf("%s:%s", projectID, datasetID)).WriteResponse(w)
			return
		}
		s.logger.Error("failed to create dataset", zap.Error(err))
		apierror.NewInternalError("Failed to create dataset").WriteResponse(w)
		return
	}

	resp := toDatasetResponse(&dataset)
	writeJSON(w, http.StatusOK, resp)
}

// getDataset handles GET /bigquery/v2/projects/{projectId}/datasets/{datasetId}
func (s *Server) getDataset(w http.ResponseWriter, r *http.Request) {
	projectID := chi.URLParam(r, "projectId")
	datasetID := chi.URLParam(r, "datasetId")

	dataset, err := s.repo.GetDataset(r.Context(), projectID, datasetID)
	if err != nil {
		if isNotFoundError(err) {
			apierror.NewNotFoundError("Dataset", fmt.Sprintf("%s:%s", projectID, datasetID)).WriteResponse(w)
			return
		}
		s.logger.Error("failed to get dataset", zap.Error(err))
		apierror.NewInternalError("Failed to get dataset").WriteResponse(w)
		return
	}

	resp := toDatasetResponse(dataset)
	writeJSON(w, http.StatusOK, resp)
}

// listDatasets handles GET /bigquery/v2/projects/{projectId}/datasets
func (s *Server) listDatasets(w http.ResponseWriter, r *http.Request) {
	projectID := chi.URLParam(r, "projectId")

	datasets, err := s.repo.ListDatasets(r.Context(), projectID)
	if err != nil {
		s.logger.Error("failed to list datasets", zap.Error(err))
		apierror.NewInternalError("Failed to list datasets").WriteResponse(w)
		return
	}

	// Parse pagination parameters
	maxResults := len(datasets)
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
	totalItems := len(datasets)
	if startIndex > len(datasets) {
		startIndex = len(datasets)
	}
	endIndex := startIndex + maxResults
	if endIndex > len(datasets) {
		endIndex = len(datasets)
	}
	pageDatasets := datasets[startIndex:endIndex]

	// Build response entries
	entries := make([]datasetResponse, 0, len(pageDatasets))
	for i := range pageDatasets {
		entries = append(entries, toDatasetResponse(&pageDatasets[i]))
	}

	resp := datasetListResponse{
		Kind:       "bigquery#datasetList",
		Datasets:   entries,
		TotalItems: totalItems,
	}

	// Set next page token if there are more results
	if endIndex < totalItems {
		resp.NextPageToken = encodePageToken(endIndex)
	}

	writeJSON(w, http.StatusOK, resp)
}

// patchDataset handles PATCH /bigquery/v2/projects/{projectId}/datasets/{datasetId}
func (s *Server) patchDataset(w http.ResponseWriter, r *http.Request) {
	projectID := chi.URLParam(r, "projectId")
	datasetID := chi.URLParam(r, "datasetId")

	// Fetch existing dataset
	existing, err := s.repo.GetDataset(r.Context(), projectID, datasetID)
	if err != nil {
		if isNotFoundError(err) {
			apierror.NewNotFoundError("Dataset", fmt.Sprintf("%s:%s", projectID, datasetID)).WriteResponse(w)
			return
		}
		s.logger.Error("failed to get dataset for patch", zap.Error(err))
		apierror.NewInternalError("Failed to get dataset").WriteResponse(w)
		return
	}

	// Parse patch body
	var patch patchDatasetRequest
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		apierror.NewBadRequestError("Invalid JSON in request body").WriteResponse(w)
		return
	}

	// Apply partial updates (only set fields that are present)
	if patch.FriendlyName != nil {
		existing.FriendlyName = *patch.FriendlyName
	}
	if patch.Description != nil {
		existing.Description = *patch.Description
	}
	if patch.Labels != nil {
		existing.Labels = patch.Labels
	}
	if patch.Access != nil {
		existing.Access = *patch.Access
	}
	if patch.DefaultTableExpirationMs != nil {
		if parsed, err := strconv.ParseInt(*patch.DefaultTableExpirationMs, 10, 64); err == nil {
			existing.DefaultTableExpiration = parsed
		}
	}
	existing.LastModifiedTime = time.Now()

	if err := s.repo.UpdateDataset(r.Context(), *existing); err != nil {
		s.logger.Error("failed to update dataset", zap.Error(err))
		apierror.NewInternalError("Failed to update dataset").WriteResponse(w)
		return
	}

	resp := toDatasetResponse(existing)
	writeJSON(w, http.StatusOK, resp)
}

// deleteDataset handles DELETE /bigquery/v2/projects/{projectId}/datasets/{datasetId}
func (s *Server) deleteDataset(w http.ResponseWriter, r *http.Request) {
	projectID := chi.URLParam(r, "projectId")
	datasetID := chi.URLParam(r, "datasetId")

	// Check that the dataset exists before deleting, since the repository's
	// DELETE SQL succeeds silently even when no matching row exists.
	if _, err := s.repo.GetDataset(r.Context(), projectID, datasetID); err != nil {
		if isNotFoundError(err) {
			apierror.NewNotFoundError("Dataset", fmt.Sprintf("%s:%s", projectID, datasetID)).WriteResponse(w)
			return
		}
		s.logger.Error("failed to check dataset existence", zap.Error(err))
		apierror.NewInternalError("Failed to delete dataset").WriteResponse(w)
		return
	}

	deleteContents := false
	if dc := r.URL.Query().Get("deleteContents"); dc == "true" {
		deleteContents = true
	}

	if err := s.repo.DeleteDataset(r.Context(), projectID, datasetID, deleteContents); err != nil {
		s.logger.Error("failed to delete dataset", zap.Error(err))
		apierror.NewInternalError("Failed to delete dataset").WriteResponse(w)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
