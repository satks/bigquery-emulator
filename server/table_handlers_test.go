package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/sathish/bigquery-emulator/pkg/connection"
	"github.com/sathish/bigquery-emulator/pkg/metadata"
	"go.uber.org/zap"
)

// setupTableTestServer creates a Server with an in-memory DuckDB, metadata repository,
// and chi router with table routes registered. It returns the server and a cleanup function.
func setupTableTestServer(t *testing.T) *Server {
	t.Helper()

	logger, err := zap.NewDevelopment()
	if err != nil {
		t.Fatalf("failed to create logger: %v", err)
	}

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
		t.Fatalf("failed to create metadata repository: %v", err)
	}

	r := chi.NewRouter()
	s := &Server{
		repo:   repo,
		router: r,
		logger: logger,
	}

	// Register table routes
	r.Route("/bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables", func(r chi.Router) {
		r.Post("/", s.createTable)
		r.Get("/", s.listTables)
		r.Get("/{tableId}", s.getTable)
		r.Patch("/{tableId}", s.patchTable)
		r.Delete("/{tableId}", s.deleteTable)
	})

	return s
}

// createTestDataset creates a dataset in the repository for use in table tests.
func createTestDataset(t *testing.T, repo *metadata.Repository, projectID, datasetID string) {
	t.Helper()
	ds := metadata.Dataset{
		ProjectID: projectID,
		DatasetID: datasetID,
	}
	if err := repo.CreateDataset(context.Background(), ds); err != nil {
		t.Fatalf("failed to create test dataset: %v", err)
	}
}

func TestCreateTable_Success(t *testing.T) {
	s := setupTableTestServer(t)
	createTestDataset(t, s.repo, "test-project", "test_dataset")

	body := map[string]interface{}{
		"tableReference": map[string]string{
			"projectId": "test-project",
			"datasetId": "test_dataset",
			"tableId":   "test_table",
		},
		"schema": map[string]interface{}{
			"fields": []map[string]interface{}{
				{"name": "id", "type": "INT64", "mode": "REQUIRED"},
				{"name": "name", "type": "STRING", "mode": "NULLABLE"},
				{"name": "email", "type": "STRING", "mode": "NULLABLE"},
			},
		},
		"description": "A test table",
		"labels":      map[string]string{"env": "test"},
	}

	b, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/test-project/datasets/test_dataset/tables", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	s.router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if resp["kind"] != "bigquery#table" {
		t.Errorf("expected kind=bigquery#table, got %v", resp["kind"])
	}
	if resp["id"] != "test-project:test_dataset.test_table" {
		t.Errorf("expected id=test-project:test_dataset.test_table, got %v", resp["id"])
	}
	if resp["type"] != "TABLE" {
		t.Errorf("expected type=TABLE, got %v", resp["type"])
	}
	if resp["description"] != "A test table" {
		t.Errorf("expected description='A test table', got %v", resp["description"])
	}

	// Verify tableReference
	ref, ok := resp["tableReference"].(map[string]interface{})
	if !ok {
		t.Fatal("tableReference missing or wrong type")
	}
	if ref["projectId"] != "test-project" {
		t.Errorf("expected projectId=test-project, got %v", ref["projectId"])
	}
	if ref["datasetId"] != "test_dataset" {
		t.Errorf("expected datasetId=test_dataset, got %v", ref["datasetId"])
	}
	if ref["tableId"] != "test_table" {
		t.Errorf("expected tableId=test_table, got %v", ref["tableId"])
	}

	// Verify schema fields
	schema, ok := resp["schema"].(map[string]interface{})
	if !ok {
		t.Fatal("schema missing or wrong type")
	}
	fields, ok := schema["fields"].([]interface{})
	if !ok {
		t.Fatal("schema.fields missing or wrong type")
	}
	if len(fields) != 3 {
		t.Fatalf("expected 3 schema fields, got %d", len(fields))
	}

	// Verify timestamps are present
	if resp["creationTime"] == nil {
		t.Error("creationTime should be present")
	}
	if resp["lastModifiedTime"] == nil {
		t.Error("lastModifiedTime should be present")
	}
}

func TestCreateTable_WithNestedStruct(t *testing.T) {
	s := setupTableTestServer(t)
	createTestDataset(t, s.repo, "test-project", "test_dataset")

	body := map[string]interface{}{
		"tableReference": map[string]string{
			"projectId": "test-project",
			"datasetId": "test_dataset",
			"tableId":   "nested_table",
		},
		"schema": map[string]interface{}{
			"fields": []map[string]interface{}{
				{"name": "id", "type": "INT64", "mode": "REQUIRED"},
				{
					"name": "address",
					"type": "RECORD",
					"mode": "NULLABLE",
					"fields": []map[string]interface{}{
						{"name": "street", "type": "STRING", "mode": "NULLABLE"},
						{"name": "city", "type": "STRING", "mode": "NULLABLE"},
						{"name": "zip", "type": "STRING", "mode": "NULLABLE"},
					},
				},
			},
		},
	}

	b, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/test-project/datasets/test_dataset/tables", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	s.router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	schema := resp["schema"].(map[string]interface{})
	fields := schema["fields"].([]interface{})
	if len(fields) != 2 {
		t.Fatalf("expected 2 top-level fields, got %d", len(fields))
	}

	// Check the nested RECORD field
	addressField := fields[1].(map[string]interface{})
	if addressField["name"] != "address" {
		t.Errorf("expected field name=address, got %v", addressField["name"])
	}
	if addressField["type"] != "RECORD" {
		t.Errorf("expected field type=RECORD, got %v", addressField["type"])
	}

	nestedFields, ok := addressField["fields"].([]interface{})
	if !ok {
		t.Fatal("nested fields missing for RECORD type")
	}
	if len(nestedFields) != 3 {
		t.Fatalf("expected 3 nested fields, got %d", len(nestedFields))
	}
}

func TestCreateTable_WithArrayType(t *testing.T) {
	s := setupTableTestServer(t)
	createTestDataset(t, s.repo, "test-project", "test_dataset")

	body := map[string]interface{}{
		"tableReference": map[string]string{
			"projectId": "test-project",
			"datasetId": "test_dataset",
			"tableId":   "array_table",
		},
		"schema": map[string]interface{}{
			"fields": []map[string]interface{}{
				{"name": "id", "type": "INT64", "mode": "REQUIRED"},
				{"name": "tags", "type": "STRING", "mode": "REPEATED"},
			},
		},
	}

	b, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/test-project/datasets/test_dataset/tables", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	s.router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	schema := resp["schema"].(map[string]interface{})
	fields := schema["fields"].([]interface{})
	if len(fields) != 2 {
		t.Fatalf("expected 2 fields, got %d", len(fields))
	}

	tagsField := fields[1].(map[string]interface{})
	if tagsField["name"] != "tags" {
		t.Errorf("expected field name=tags, got %v", tagsField["name"])
	}
	if tagsField["mode"] != "REPEATED" {
		t.Errorf("expected field mode=REPEATED, got %v", tagsField["mode"])
	}
}

func TestCreateTable_AlreadyExists(t *testing.T) {
	s := setupTableTestServer(t)
	createTestDataset(t, s.repo, "test-project", "test_dataset")

	body := map[string]interface{}{
		"tableReference": map[string]string{
			"projectId": "test-project",
			"datasetId": "test_dataset",
			"tableId":   "dup_table",
		},
		"schema": map[string]interface{}{
			"fields": []map[string]interface{}{
				{"name": "id", "type": "INT64", "mode": "REQUIRED"},
			},
		},
	}

	b, _ := json.Marshal(body)

	// First create should succeed
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/test-project/datasets/test_dataset/tables", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	s.router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("first create expected 200, got %d: %s", w.Code, w.Body.String())
	}

	// Second create should return 409
	req = httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/test-project/datasets/test_dataset/tables", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	s.router.ServeHTTP(w, req)

	if w.Code != http.StatusConflict {
		t.Fatalf("duplicate create expected 409, got %d: %s", w.Code, w.Body.String())
	}

	var errResp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("failed to unmarshal error response: %v", err)
	}
	errObj, ok := errResp["error"].(map[string]interface{})
	if !ok {
		t.Fatal("error field missing from response")
	}
	if errObj["code"].(float64) != 409 {
		t.Errorf("expected error code 409, got %v", errObj["code"])
	}
}

func TestGetTable_Success(t *testing.T) {
	s := setupTableTestServer(t)
	createTestDataset(t, s.repo, "test-project", "test_dataset")

	// Create a table first
	createBody := map[string]interface{}{
		"tableReference": map[string]string{
			"projectId": "test-project",
			"datasetId": "test_dataset",
			"tableId":   "get_table",
		},
		"schema": map[string]interface{}{
			"fields": []map[string]interface{}{
				{"name": "id", "type": "INT64", "mode": "REQUIRED"},
				{"name": "value", "type": "STRING", "mode": "NULLABLE"},
			},
		},
		"description": "Table for get test",
	}
	b, _ := json.Marshal(createBody)
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/test-project/datasets/test_dataset/tables", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	s.router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("create table failed: %d: %s", w.Code, w.Body.String())
	}

	// Now GET the table
	req = httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/test-project/datasets/test_dataset/tables/get_table", nil)
	w = httptest.NewRecorder()
	s.router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if resp["kind"] != "bigquery#table" {
		t.Errorf("expected kind=bigquery#table, got %v", resp["kind"])
	}
	if resp["id"] != "test-project:test_dataset.get_table" {
		t.Errorf("expected id=test-project:test_dataset.get_table, got %v", resp["id"])
	}
	if resp["description"] != "Table for get test" {
		t.Errorf("expected description='Table for get test', got %v", resp["description"])
	}

	// Verify full schema is returned
	schema := resp["schema"].(map[string]interface{})
	fields := schema["fields"].([]interface{})
	if len(fields) != 2 {
		t.Fatalf("expected 2 schema fields, got %d", len(fields))
	}

	// Verify numBytes and numRows are present
	if resp["numBytes"] == nil {
		t.Error("numBytes should be present")
	}
	if resp["numRows"] == nil {
		t.Error("numRows should be present")
	}
}

func TestGetTable_NotFound(t *testing.T) {
	s := setupTableTestServer(t)
	createTestDataset(t, s.repo, "test-project", "test_dataset")

	req := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/test-project/datasets/test_dataset/tables/nonexistent", nil)
	w := httptest.NewRecorder()
	s.router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected status 404, got %d: %s", w.Code, w.Body.String())
	}

	var errResp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("failed to unmarshal error response: %v", err)
	}
	errObj, ok := errResp["error"].(map[string]interface{})
	if !ok {
		t.Fatal("error field missing from response")
	}
	if errObj["code"].(float64) != 404 {
		t.Errorf("expected error code 404, got %v", errObj["code"])
	}
}

func TestListTables_Empty(t *testing.T) {
	s := setupTableTestServer(t)
	createTestDataset(t, s.repo, "test-project", "test_dataset")

	req := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/test-project/datasets/test_dataset/tables", nil)
	w := httptest.NewRecorder()
	s.router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if resp["kind"] != "bigquery#tableList" {
		t.Errorf("expected kind=bigquery#tableList, got %v", resp["kind"])
	}

	totalItems := resp["totalItems"].(float64)
	if totalItems != 0 {
		t.Errorf("expected totalItems=0, got %v", totalItems)
	}

	tables, ok := resp["tables"].([]interface{})
	if !ok {
		// tables may be null/absent for empty list, that's OK per BQ API
		if resp["tables"] != nil {
			t.Errorf("expected tables to be nil or empty array, got %v", resp["tables"])
		}
	} else if len(tables) != 0 {
		t.Errorf("expected empty tables array, got %d items", len(tables))
	}
}

func TestListTables_WithTables(t *testing.T) {
	s := setupTableTestServer(t)
	createTestDataset(t, s.repo, "test-project", "test_dataset")

	// Create 3 tables
	for i := 1; i <= 3; i++ {
		body := map[string]interface{}{
			"tableReference": map[string]string{
				"projectId": "test-project",
				"datasetId": "test_dataset",
				"tableId":   fmt.Sprintf("list_table_%d", i),
			},
			"schema": map[string]interface{}{
				"fields": []map[string]interface{}{
					{"name": "id", "type": "INT64", "mode": "REQUIRED"},
				},
			},
		}
		b, _ := json.Marshal(body)
		req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/test-project/datasets/test_dataset/tables", bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		s.router.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("create table %d failed: %d: %s", i, w.Code, w.Body.String())
		}
	}

	// List tables
	req := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/test-project/datasets/test_dataset/tables", nil)
	w := httptest.NewRecorder()
	s.router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if resp["kind"] != "bigquery#tableList" {
		t.Errorf("expected kind=bigquery#tableList, got %v", resp["kind"])
	}

	totalItems := resp["totalItems"].(float64)
	if totalItems != 3 {
		t.Errorf("expected totalItems=3, got %v", totalItems)
	}

	tables := resp["tables"].([]interface{})
	if len(tables) != 3 {
		t.Fatalf("expected 3 tables, got %d", len(tables))
	}

	// Each entry should have tableReference and type
	for _, entry := range tables {
		tbl := entry.(map[string]interface{})
		ref, ok := tbl["tableReference"].(map[string]interface{})
		if !ok {
			t.Error("table entry missing tableReference")
			continue
		}
		if ref["projectId"] != "test-project" {
			t.Errorf("expected projectId=test-project, got %v", ref["projectId"])
		}
		if tbl["type"] != "TABLE" {
			t.Errorf("expected type=TABLE, got %v", tbl["type"])
		}
	}
}

func TestListTables_Pagination(t *testing.T) {
	s := setupTableTestServer(t)
	createTestDataset(t, s.repo, "test-project", "test_dataset")

	// Create 5 tables
	for i := 1; i <= 5; i++ {
		body := map[string]interface{}{
			"tableReference": map[string]string{
				"projectId": "test-project",
				"datasetId": "test_dataset",
				"tableId":   fmt.Sprintf("page_table_%d", i),
			},
			"schema": map[string]interface{}{
				"fields": []map[string]interface{}{
					{"name": "id", "type": "INT64", "mode": "REQUIRED"},
				},
			},
		}
		b, _ := json.Marshal(body)
		req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/test-project/datasets/test_dataset/tables", bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		s.router.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("create table %d failed: %d: %s", i, w.Code, w.Body.String())
		}
	}

	// Get first page (maxResults=2)
	req := httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/test-project/datasets/test_dataset/tables?maxResults=2", nil)
	w := httptest.NewRecorder()
	s.router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	totalItems := int(resp["totalItems"].(float64))
	if totalItems != 5 {
		t.Errorf("expected totalItems=5, got %d", totalItems)
	}

	tables := resp["tables"].([]interface{})
	if len(tables) != 2 {
		t.Fatalf("first page expected 2 tables, got %d", len(tables))
	}

	// Should have a nextPageToken
	nextPageToken, ok := resp["nextPageToken"].(string)
	if !ok || nextPageToken == "" {
		t.Fatal("expected nextPageToken for first page")
	}

	// Get second page using the nextPageToken
	req = httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/test-project/datasets/test_dataset/tables?maxResults=2&pageToken="+nextPageToken, nil)
	w = httptest.NewRecorder()
	s.router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp2 map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp2); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	tables2 := resp2["tables"].([]interface{})
	if len(tables2) != 2 {
		t.Fatalf("second page expected 2 tables, got %d", len(tables2))
	}

	nextPageToken2, ok := resp2["nextPageToken"].(string)
	if !ok || nextPageToken2 == "" {
		t.Fatal("expected nextPageToken for second page")
	}

	// Get third page (last page)
	req = httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/test-project/datasets/test_dataset/tables?maxResults=2&pageToken="+nextPageToken2, nil)
	w = httptest.NewRecorder()
	s.router.ServeHTTP(w, req)

	var resp3 map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp3); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	tables3 := resp3["tables"].([]interface{})
	if len(tables3) != 1 {
		t.Fatalf("third page expected 1 table, got %d", len(tables3))
	}

	// Should NOT have a nextPageToken on the last page
	if _, has := resp3["nextPageToken"]; has {
		t.Error("expected no nextPageToken on last page")
	}
}

func TestDeleteTable_Success(t *testing.T) {
	s := setupTableTestServer(t)
	createTestDataset(t, s.repo, "test-project", "test_dataset")

	// Create a table first
	createBody := map[string]interface{}{
		"tableReference": map[string]string{
			"projectId": "test-project",
			"datasetId": "test_dataset",
			"tableId":   "delete_me",
		},
		"schema": map[string]interface{}{
			"fields": []map[string]interface{}{
				{"name": "id", "type": "INT64", "mode": "REQUIRED"},
			},
		},
	}
	b, _ := json.Marshal(createBody)
	req := httptest.NewRequest(http.MethodPost, "/bigquery/v2/projects/test-project/datasets/test_dataset/tables", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	s.router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("create table failed: %d: %s", w.Code, w.Body.String())
	}

	// Delete the table
	req = httptest.NewRequest(http.MethodDelete, "/bigquery/v2/projects/test-project/datasets/test_dataset/tables/delete_me", nil)
	w = httptest.NewRecorder()
	s.router.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected status 204, got %d: %s", w.Code, w.Body.String())
	}

	// Verify table is gone
	req = httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/test-project/datasets/test_dataset/tables/delete_me", nil)
	w = httptest.NewRecorder()
	s.router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 after delete, got %d: %s", w.Code, w.Body.String())
	}
}
