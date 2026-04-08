package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/sathish/bigquery-emulator/pkg/connection"
	"github.com/sathish/bigquery-emulator/pkg/metadata"
	"go.uber.org/zap"
)

// setupDatasetTestServer creates a Server with an in-memory DuckDB, metadata repository,
// and chi router with dataset routes registered. It returns the server; cleanup is
// handled via t.Cleanup.
func setupDatasetTestServer(t *testing.T) *Server {
	t.Helper()
	logger, _ := zap.NewDevelopment()
	connMgr, err := connection.NewManager(":memory:", logger)
	if err != nil {
		t.Fatalf("failed to create connection manager: %v", err)
	}
	t.Cleanup(func() {
		if err := connMgr.Close(); err != nil {
			t.Errorf("failed to close connection manager: %v", err)
		}
	})

	repo, err := metadata.NewRepository(connMgr, logger)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	r := chi.NewRouter()
	s := &Server{
		repo:   repo,
		router: r,
		logger: logger,
	}

	// Register dataset routes (matching the pattern in setupRoutes)
	r.Route("/bigquery/v2/projects/{projectId}", func(r chi.Router) {
		r.Get("/datasets", s.listDatasets)
		r.Post("/datasets", s.createDataset)
		r.Get("/datasets/{datasetId}", s.getDataset)
		r.Patch("/datasets/{datasetId}", s.patchDataset)
		r.Delete("/datasets/{datasetId}", s.deleteDataset)
	})

	return s
}

// doDatasetRequest is a test helper that sends an HTTP request to the server's router.
func doDatasetRequest(t *testing.T, s *Server, method, path string, body interface{}) *httptest.ResponseRecorder {
	t.Helper()
	var reqBody *bytes.Buffer
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("failed to marshal request body: %v", err)
		}
		reqBody = bytes.NewBuffer(b)
	} else {
		reqBody = &bytes.Buffer{}
	}

	req := httptest.NewRequest(method, path, reqBody)
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	s.router.ServeHTTP(rr, req)
	return rr
}

// decodeDatasetJSON is a test helper that decodes a JSON response body into a map.
func decodeDatasetJSON(t *testing.T, rr *httptest.ResponseRecorder) map[string]interface{} {
	t.Helper()
	var result map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &result); err != nil {
		t.Fatalf("failed to decode response body: %v\nbody: %s", err, rr.Body.String())
	}
	return result
}

// createTestProject creates a project in the repository for use in dataset tests.
func createTestProject(t *testing.T, repo *metadata.Repository, projectID string) {
	t.Helper()
	if err := repo.CreateProject(context.Background(), metadata.Project{ID: projectID}); err != nil {
		t.Fatalf("failed to create test project: %v", err)
	}
}

// --- CREATE DATASET ---

func TestCreateDataset_Success(t *testing.T) {
	s := setupDatasetTestServer(t)

	body := map[string]interface{}{
		"datasetReference": map[string]interface{}{
			"projectId": "test-project",
			"datasetId": "my_dataset",
		},
		"friendlyName": "My Dataset",
		"description":  "A test dataset",
		"location":     "US",
		"labels": map[string]string{
			"env": "test",
		},
	}

	rr := doDatasetRequest(t, s, http.MethodPost, "/bigquery/v2/projects/test-project/datasets", body)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", rr.Code, rr.Body.String())
	}

	resp := decodeDatasetJSON(t, rr)

	// Verify kind
	if resp["kind"] != "bigquery#dataset" {
		t.Errorf("expected kind bigquery#dataset, got %v", resp["kind"])
	}

	// Verify id format: "project_id:dataset_id"
	if resp["id"] != "test-project:my_dataset" {
		t.Errorf("expected id test-project:my_dataset, got %v", resp["id"])
	}

	// Verify datasetReference
	ref, ok := resp["datasetReference"].(map[string]interface{})
	if !ok {
		t.Fatalf("datasetReference is not a map: %v", resp["datasetReference"])
	}
	if ref["projectId"] != "test-project" {
		t.Errorf("expected projectId test-project, got %v", ref["projectId"])
	}
	if ref["datasetId"] != "my_dataset" {
		t.Errorf("expected datasetId my_dataset, got %v", ref["datasetId"])
	}

	// Verify friendlyName and description
	if resp["friendlyName"] != "My Dataset" {
		t.Errorf("expected friendlyName 'My Dataset', got %v", resp["friendlyName"])
	}
	if resp["description"] != "A test dataset" {
		t.Errorf("expected description 'A test dataset', got %v", resp["description"])
	}

	// Verify location
	if resp["location"] != "US" {
		t.Errorf("expected location US, got %v", resp["location"])
	}

	// Verify creationTime and lastModifiedTime are present and non-empty strings
	if ct, ok := resp["creationTime"].(string); !ok || ct == "" {
		t.Errorf("expected creationTime to be a non-empty string, got %v", resp["creationTime"])
	}
	if lm, ok := resp["lastModifiedTime"].(string); !ok || lm == "" {
		t.Errorf("expected lastModifiedTime to be a non-empty string, got %v", resp["lastModifiedTime"])
	}
}

func TestCreateDataset_AlreadyExists(t *testing.T) {
	s := setupDatasetTestServer(t)

	body := map[string]interface{}{
		"datasetReference": map[string]interface{}{
			"projectId": "test-project",
			"datasetId": "dup_dataset",
		},
	}

	// First create should succeed
	rr := doDatasetRequest(t, s, http.MethodPost, "/bigquery/v2/projects/test-project/datasets", body)
	if rr.Code != http.StatusOK {
		t.Fatalf("first create failed: %d %s", rr.Code, rr.Body.String())
	}

	// Second create should return 409
	rr = doDatasetRequest(t, s, http.MethodPost, "/bigquery/v2/projects/test-project/datasets", body)
	if rr.Code != http.StatusConflict {
		t.Fatalf("expected status 409, got %d: %s", rr.Code, rr.Body.String())
	}

	resp := decodeDatasetJSON(t, rr)
	errObj, ok := resp["error"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected error envelope, got %v", resp)
	}
	if errObj["status"] != "ALREADY_EXISTS" {
		t.Errorf("expected status ALREADY_EXISTS, got %v", errObj["status"])
	}
}

func TestCreateDataset_InvalidJSON(t *testing.T) {
	s := setupDatasetTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/test-project/datasets", bytes.NewBufferString("{invalid json"))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	s.router.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d: %s", rr.Code, rr.Body.String())
	}
}

func TestCreateDataset_MissingDatasetID(t *testing.T) {
	s := setupDatasetTestServer(t)

	body := map[string]interface{}{
		"datasetReference": map[string]interface{}{
			"projectId": "test-project",
			// datasetId is missing
		},
		"friendlyName": "No ID Dataset",
	}

	rr := doDatasetRequest(t, s, http.MethodPost, "/bigquery/v2/projects/test-project/datasets", body)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d: %s", rr.Code, rr.Body.String())
	}
}

// --- GET DATASET ---

func TestGetDataset_Success(t *testing.T) {
	s := setupDatasetTestServer(t)

	// Create a dataset first
	createBody := map[string]interface{}{
		"datasetReference": map[string]interface{}{
			"projectId": "test-project",
			"datasetId": "get_test",
		},
		"friendlyName": "Get Test Dataset",
		"description":  "For get testing",
		"location":     "EU",
	}
	rr := doDatasetRequest(t, s, http.MethodPost, "/bigquery/v2/projects/test-project/datasets", createBody)
	if rr.Code != http.StatusOK {
		t.Fatalf("create failed: %d %s", rr.Code, rr.Body.String())
	}

	// Now GET it
	rr = doDatasetRequest(t, s, http.MethodGet, "/bigquery/v2/projects/test-project/datasets/get_test", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", rr.Code, rr.Body.String())
	}

	resp := decodeDatasetJSON(t, rr)

	if resp["kind"] != "bigquery#dataset" {
		t.Errorf("expected kind bigquery#dataset, got %v", resp["kind"])
	}
	if resp["id"] != "test-project:get_test" {
		t.Errorf("expected id test-project:get_test, got %v", resp["id"])
	}
	if resp["friendlyName"] != "Get Test Dataset" {
		t.Errorf("expected friendlyName 'Get Test Dataset', got %v", resp["friendlyName"])
	}
	if resp["description"] != "For get testing" {
		t.Errorf("expected description 'For get testing', got %v", resp["description"])
	}
	if resp["location"] != "EU" {
		t.Errorf("expected location EU, got %v", resp["location"])
	}

	// Verify datasetReference
	ref, ok := resp["datasetReference"].(map[string]interface{})
	if !ok {
		t.Fatalf("datasetReference is not a map")
	}
	if ref["datasetId"] != "get_test" {
		t.Errorf("expected datasetId get_test, got %v", ref["datasetId"])
	}
}

func TestGetDataset_NotFound(t *testing.T) {
	s := setupDatasetTestServer(t)

	rr := doDatasetRequest(t, s, http.MethodGet, "/bigquery/v2/projects/test-project/datasets/nonexistent", nil)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected status 404, got %d: %s", rr.Code, rr.Body.String())
	}

	resp := decodeDatasetJSON(t, rr)
	errObj, ok := resp["error"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected error envelope, got %v", resp)
	}
	if errObj["status"] != "NOT_FOUND" {
		t.Errorf("expected status NOT_FOUND, got %v", errObj["status"])
	}
}

// --- LIST DATASETS ---

func TestListDatasets_Empty(t *testing.T) {
	s := setupDatasetTestServer(t)

	rr := doDatasetRequest(t, s, http.MethodGet, "/bigquery/v2/projects/test-project/datasets", nil)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", rr.Code, rr.Body.String())
	}

	resp := decodeDatasetJSON(t, rr)

	if resp["kind"] != "bigquery#datasetList" {
		t.Errorf("expected kind bigquery#datasetList, got %v", resp["kind"])
	}

	// totalItems should be 0
	totalItems, ok := resp["totalItems"].(float64)
	if !ok {
		t.Fatalf("totalItems is not a number: %v (%T)", resp["totalItems"], resp["totalItems"])
	}
	if int(totalItems) != 0 {
		t.Errorf("expected totalItems 0, got %v", totalItems)
	}
}

func TestListDatasets_WithDatasets(t *testing.T) {
	s := setupDatasetTestServer(t)

	// Create two datasets
	for _, id := range []string{"ds_one", "ds_two"} {
		body := map[string]interface{}{
			"datasetReference": map[string]interface{}{
				"projectId": "test-project",
				"datasetId": id,
			},
		}
		rr := doDatasetRequest(t, s, http.MethodPost, "/bigquery/v2/projects/test-project/datasets", body)
		if rr.Code != http.StatusOK {
			t.Fatalf("create %s failed: %d %s", id, rr.Code, rr.Body.String())
		}
	}

	// List
	rr := doDatasetRequest(t, s, http.MethodGet, "/bigquery/v2/projects/test-project/datasets", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", rr.Code, rr.Body.String())
	}

	resp := decodeDatasetJSON(t, rr)

	if resp["kind"] != "bigquery#datasetList" {
		t.Errorf("expected kind bigquery#datasetList, got %v", resp["kind"])
	}

	totalItems, ok := resp["totalItems"].(float64)
	if !ok {
		t.Fatalf("totalItems is not a number: %v", resp["totalItems"])
	}
	if int(totalItems) != 2 {
		t.Errorf("expected totalItems 2, got %v", totalItems)
	}

	datasets, ok := resp["datasets"].([]interface{})
	if !ok {
		t.Fatalf("datasets is not an array: %v", resp["datasets"])
	}
	if len(datasets) != 2 {
		t.Errorf("expected 2 datasets, got %d", len(datasets))
	}

	// Verify each dataset has datasetReference
	for _, d := range datasets {
		ds, ok := d.(map[string]interface{})
		if !ok {
			t.Errorf("dataset entry is not a map: %v", d)
			continue
		}
		ref, ok := ds["datasetReference"].(map[string]interface{})
		if !ok {
			t.Errorf("datasetReference missing in dataset entry: %v", ds)
			continue
		}
		if ref["projectId"] != "test-project" {
			t.Errorf("expected projectId test-project, got %v", ref["projectId"])
		}
	}
}

// --- PATCH DATASET ---

func TestPatchDataset_Success(t *testing.T) {
	s := setupDatasetTestServer(t)

	// Create a dataset
	createBody := map[string]interface{}{
		"datasetReference": map[string]interface{}{
			"projectId": "test-project",
			"datasetId": "patch_test",
		},
		"friendlyName": "Original Name",
		"description":  "Original description",
	}
	rr := doDatasetRequest(t, s, http.MethodPost, "/bigquery/v2/projects/test-project/datasets", createBody)
	if rr.Code != http.StatusOK {
		t.Fatalf("create failed: %d %s", rr.Code, rr.Body.String())
	}

	// Patch the description
	patchBody := map[string]interface{}{
		"description": "Updated description",
	}
	rr = doDatasetRequest(t, s, http.MethodPatch, "/bigquery/v2/projects/test-project/datasets/patch_test", patchBody)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", rr.Code, rr.Body.String())
	}

	resp := decodeDatasetJSON(t, rr)

	// Description should be updated
	if resp["description"] != "Updated description" {
		t.Errorf("expected description 'Updated description', got %v", resp["description"])
	}

	// FriendlyName should be preserved
	if resp["friendlyName"] != "Original Name" {
		t.Errorf("expected friendlyName 'Original Name', got %v", resp["friendlyName"])
	}

	// Verify via GET
	rr = doDatasetRequest(t, s, http.MethodGet, "/bigquery/v2/projects/test-project/datasets/patch_test", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("get after patch failed: %d %s", rr.Code, rr.Body.String())
	}
	resp = decodeDatasetJSON(t, rr)
	if resp["description"] != "Updated description" {
		t.Errorf("GET after patch: expected description 'Updated description', got %v", resp["description"])
	}
}

// --- DELETE DATASET ---

func TestDeleteDataset_Success(t *testing.T) {
	s := setupDatasetTestServer(t)

	// Create a dataset
	body := map[string]interface{}{
		"datasetReference": map[string]interface{}{
			"projectId": "test-project",
			"datasetId": "delete_me",
		},
	}
	rr := doDatasetRequest(t, s, http.MethodPost, "/bigquery/v2/projects/test-project/datasets", body)
	if rr.Code != http.StatusOK {
		t.Fatalf("create failed: %d %s", rr.Code, rr.Body.String())
	}

	// Delete it
	rr = doDatasetRequest(t, s, http.MethodDelete, "/bigquery/v2/projects/test-project/datasets/delete_me", nil)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("expected status 204, got %d: %s", rr.Code, rr.Body.String())
	}

	// Verify it's gone
	rr = doDatasetRequest(t, s, http.MethodGet, "/bigquery/v2/projects/test-project/datasets/delete_me", nil)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404 after delete, got %d: %s", rr.Code, rr.Body.String())
	}
}

func TestDeleteDataset_NotFound(t *testing.T) {
	s := setupDatasetTestServer(t)

	rr := doDatasetRequest(t, s, http.MethodDelete, "/bigquery/v2/projects/test-project/datasets/nonexistent", nil)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected status 404, got %d: %s", rr.Code, rr.Body.String())
	}

	resp := decodeDatasetJSON(t, rr)
	errObj, ok := resp["error"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected error envelope, got %v", resp)
	}
	if errObj["status"] != "NOT_FOUND" {
		t.Errorf("expected status NOT_FOUND, got %v", errObj["status"])
	}
}
