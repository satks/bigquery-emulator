package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestListTableData_Success(t *testing.T) {
	baseURL := setupJobTestServer(t)

	resp, err := http.Get(baseURL + "/bigquery/v2/projects/test-project/datasets/test_dataset/tables/users/data")
	if err != nil {
		t.Fatalf("GET /data error = %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /data status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	var body map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&body)

	if body["kind"] != "bigquery#tableDataList" {
		t.Errorf("kind = %v, want bigquery#tableDataList", body["kind"])
	}

	if body["totalRows"] != "3" {
		t.Errorf("totalRows = %v, want '3'", body["totalRows"])
	}

	rows, ok := body["rows"].([]interface{})
	if !ok || len(rows) != 3 {
		t.Fatalf("rows count = %v, want 3", len(rows))
	}

	// Verify BQ row format
	firstRow := rows[0].(map[string]interface{})
	fFields, ok := firstRow["f"].([]interface{})
	if !ok || len(fFields) < 2 {
		t.Fatalf("first row fields = %v, want >= 2", len(fFields))
	}
}

func TestListTableData_Empty(t *testing.T) {
	cfg := Config{
		Host:      "localhost",
		Port:      9050,
		ProjectID: "test-project",
		Database:  ":memory:",
		LogLevel:  "info",
	}

	srv, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { srv.Stop(nil) })

	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	// Create dataset
	dsBody := `{"datasetReference":{"projectId":"test-project","datasetId":"empty_ds"}}`
	resp, _ := http.Post(ts.URL+"/bigquery/v2/projects/test-project/datasets", "application/json", bytes.NewBufferString(dsBody))
	resp.Body.Close()

	// Create empty table
	tblBody := `{
		"tableReference":{"projectId":"test-project","datasetId":"empty_ds","tableId":"empty_tbl"},
		"schema":{"fields":[{"name":"id","type":"INT64"}]}
	}`
	resp, _ = http.Post(ts.URL+"/bigquery/v2/projects/test-project/datasets/empty_ds/tables", "application/json", bytes.NewBufferString(tblBody))
	resp.Body.Close()

	// List data from empty table
	resp, err = http.Get(ts.URL + "/bigquery/v2/projects/test-project/datasets/empty_ds/tables/empty_tbl/data")
	if err != nil {
		t.Fatalf("GET /data error = %v", err)
	}
	defer resp.Body.Close()

	var body map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&body)

	if body["totalRows"] != "0" {
		t.Errorf("totalRows = %v, want '0'", body["totalRows"])
	}

	rows, ok := body["rows"].([]interface{})
	if ok && len(rows) != 0 {
		t.Errorf("rows count = %d, want 0", len(rows))
	}
}

func TestListTableData_Pagination(t *testing.T) {
	baseURL := setupJobTestServer(t)

	// Get first page (maxResults=2)
	resp, err := http.Get(baseURL + "/bigquery/v2/projects/test-project/datasets/test_dataset/tables/users/data?maxResults=2")
	if err != nil {
		t.Fatalf("GET /data error = %v", err)
	}
	defer resp.Body.Close()

	var body map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&body)

	if body["totalRows"] != "3" {
		t.Errorf("totalRows = %v, want '3'", body["totalRows"])
	}

	rows, ok := body["rows"].([]interface{})
	if !ok || len(rows) != 2 {
		t.Fatalf("first page rows = %v, want 2", len(rows))
	}

	// Should have a page token
	if _, ok := body["pageToken"]; !ok {
		t.Error("expected pageToken for first page")
	}
}

func TestInsertAll_Success(t *testing.T) {
	baseURL := setupJobTestServer(t)

	// Insert more rows
	insertBody := `{
		"rows": [
			{"insertId": "r4", "json": {"id": 4, "name": "Diana"}},
			{"insertId": "r5", "json": {"id": 5, "name": "Eve"}}
		]
	}`
	resp, err := http.Post(baseURL+"/bigquery/v2/projects/test-project/datasets/test_dataset/tables/users/insertAll", "application/json", bytes.NewBufferString(insertBody))
	if err != nil {
		t.Fatalf("POST /insertAll error = %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /insertAll status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	var body map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&body)

	if body["kind"] != "bigquery#tableDataInsertAllResponse" {
		t.Errorf("kind = %v, want bigquery#tableDataInsertAllResponse", body["kind"])
	}

	// Should have no insert errors
	if errors, ok := body["insertErrors"].([]interface{}); ok && len(errors) > 0 {
		t.Errorf("insertErrors = %v, want empty", errors)
	}

	// Verify by reading table data (should now have 5 rows: 3 from setup + 2 new)
	dataResp, err := http.Get(baseURL + "/bigquery/v2/projects/test-project/datasets/test_dataset/tables/users/data")
	if err != nil {
		t.Fatalf("GET /data error = %v", err)
	}
	defer dataResp.Body.Close()

	var dataBody map[string]interface{}
	json.NewDecoder(dataResp.Body).Decode(&dataBody)

	if dataBody["totalRows"] != "5" {
		t.Errorf("totalRows after insert = %v, want '5'", dataBody["totalRows"])
	}
}

func TestInsertAll_InvalidTable(t *testing.T) {
	baseURL := setupJobTestServer(t)

	insertBody := `{"rows": [{"json": {"id": 1}}]}`
	resp, err := http.Post(baseURL+"/bigquery/v2/projects/test-project/datasets/test_dataset/tables/nonexistent_table/insertAll", "application/json", bytes.NewBufferString(insertBody))
	if err != nil {
		t.Fatalf("POST /insertAll error = %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("POST /insertAll to nonexistent table status = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}
