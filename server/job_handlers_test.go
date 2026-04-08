package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// setupJobTestServer creates a fully wired test server with an in-memory DuckDB,
// a test project, dataset, and table pre-populated with data.
// It returns the httptest.Server URL and a cleanup function.
func setupJobTestServer(t *testing.T) string {
	t.Helper()

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
	dsBody := `{"datasetReference":{"projectId":"test-project","datasetId":"test_dataset"}}`
	resp, err := http.Post(ts.URL+"/bigquery/v2/projects/test-project/datasets", "application/json", bytes.NewBufferString(dsBody))
	if err != nil {
		t.Fatalf("create dataset error = %v", err)
	}
	resp.Body.Close()

	// Create table
	tblBody := `{
		"tableReference":{"projectId":"test-project","datasetId":"test_dataset","tableId":"users"},
		"schema":{"fields":[
			{"name":"id","type":"INT64","mode":"REQUIRED"},
			{"name":"name","type":"STRING"}
		]}
	}`
	resp, err = http.Post(ts.URL+"/bigquery/v2/projects/test-project/datasets/test_dataset/tables", "application/json", bytes.NewBufferString(tblBody))
	if err != nil {
		t.Fatalf("create table error = %v", err)
	}
	resp.Body.Close()

	// Insert data via insertAll
	insertBody := `{
		"rows": [
			{"json": {"id": 1, "name": "Alice"}},
			{"json": {"id": 2, "name": "Bob"}},
			{"json": {"id": 3, "name": "Charlie"}}
		]
	}`
	resp, err = http.Post(ts.URL+"/bigquery/v2/projects/test-project/datasets/test_dataset/tables/users/insertAll", "application/json", bytes.NewBufferString(insertBody))
	if err != nil {
		t.Fatalf("insertAll error = %v", err)
	}
	resp.Body.Close()

	return ts.URL
}

func TestInsertJob_Query(t *testing.T) {
	baseURL := setupJobTestServer(t)

	body := `{
		"configuration": {
			"query": {
				"query": "SELECT * FROM \"test_dataset\".\"users\" ORDER BY id",
				"useLegacySql": false
			}
		}
	}`

	resp, err := http.Post(baseURL+"/bigquery/v2/projects/test-project/jobs", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("POST /jobs error = %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /jobs status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	var jobResp map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&jobResp); err != nil {
		t.Fatalf("decode response error = %v", err)
	}

	if jobResp["kind"] != "bigquery#job" {
		t.Errorf("kind = %v, want bigquery#job", jobResp["kind"])
	}

	jobRef, ok := jobResp["jobReference"].(map[string]interface{})
	if !ok {
		t.Fatal("jobReference missing from response")
	}
	jobID, ok := jobRef["jobId"].(string)
	if !ok || jobID == "" {
		t.Fatal("jobId missing from response")
	}

	// Poll until done
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		getResp, err := http.Get(baseURL + "/bigquery/v2/projects/test-project/jobs/" + jobID)
		if err != nil {
			t.Fatalf("GET /jobs/{id} error = %v", err)
		}

		var getBody map[string]interface{}
		json.NewDecoder(getResp.Body).Decode(&getBody)
		getResp.Body.Close()

		status, _ := getBody["status"].(map[string]interface{})
		if status["state"] == "DONE" {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("job did not complete within timeout")
}

func TestInsertJob_InvalidSQL(t *testing.T) {
	baseURL := setupJobTestServer(t)

	body := `{
		"configuration": {
			"query": {
				"query": "SELECT * FROM totally_nonexistent_table_xyz"
			}
		}
	}`

	resp, err := http.Post(baseURL+"/bigquery/v2/projects/test-project/jobs", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("POST /jobs error = %v", err)
	}
	defer resp.Body.Close()

	var jobResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&jobResp)

	jobRef := jobResp["jobReference"].(map[string]interface{})
	jobID := jobRef["jobId"].(string)

	// Poll until done
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		getResp, _ := http.Get(baseURL + "/bigquery/v2/projects/test-project/jobs/" + jobID)
		var getBody map[string]interface{}
		json.NewDecoder(getResp.Body).Decode(&getBody)
		getResp.Body.Close()

		status := getBody["status"].(map[string]interface{})
		if status["state"] == "DONE" {
			if status["errorResult"] == nil {
				t.Fatal("expected errorResult for invalid SQL job")
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("job did not complete within timeout")
}

func TestGetJob_Success(t *testing.T) {
	baseURL := setupJobTestServer(t)

	// Submit a job first
	body := `{"configuration": {"query": {"query": "SELECT 1 AS num"}}}`
	resp, err := http.Post(baseURL+"/bigquery/v2/projects/test-project/jobs", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("POST /jobs error = %v", err)
	}
	var jobResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&jobResp)
	resp.Body.Close()

	jobRef := jobResp["jobReference"].(map[string]interface{})
	jobID := jobRef["jobId"].(string)

	// Get the job
	getResp, err := http.Get(baseURL + "/bigquery/v2/projects/test-project/jobs/" + jobID)
	if err != nil {
		t.Fatalf("GET /jobs/{id} error = %v", err)
	}
	defer getResp.Body.Close()

	if getResp.StatusCode != http.StatusOK {
		t.Errorf("GET /jobs/{id} status = %d, want %d", getResp.StatusCode, http.StatusOK)
	}

	var getBody map[string]interface{}
	json.NewDecoder(getResp.Body).Decode(&getBody)

	if getBody["kind"] != "bigquery#job" {
		t.Errorf("kind = %v, want bigquery#job", getBody["kind"])
	}

	ref := getBody["jobReference"].(map[string]interface{})
	if ref["jobId"] != jobID {
		t.Errorf("jobId = %v, want %v", ref["jobId"], jobID)
	}
}

func TestListJobs(t *testing.T) {
	baseURL := setupJobTestServer(t)

	// Submit 2 jobs
	for i := 0; i < 2; i++ {
		body := fmt.Sprintf(`{"configuration": {"query": {"query": "SELECT %d"}}}`, i)
		resp, err := http.Post(baseURL+"/bigquery/v2/projects/test-project/jobs", "application/json", bytes.NewBufferString(body))
		if err != nil {
			t.Fatalf("POST /jobs error = %v", err)
		}
		resp.Body.Close()
	}

	// List jobs
	resp, err := http.Get(baseURL + "/bigquery/v2/projects/test-project/jobs")
	if err != nil {
		t.Fatalf("GET /jobs error = %v", err)
	}
	defer resp.Body.Close()

	var listResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&listResp)

	if listResp["kind"] != "bigquery#jobList" {
		t.Errorf("kind = %v, want bigquery#jobList", listResp["kind"])
	}

	jobs, ok := listResp["jobs"].([]interface{})
	if !ok {
		t.Fatal("jobs field missing or wrong type")
	}
	if len(jobs) < 2 {
		t.Errorf("jobs count = %d, want >= 2", len(jobs))
	}
}

func TestGetQueryResults_Complete(t *testing.T) {
	baseURL := setupJobTestServer(t)

	// Submit a query job
	body := `{"configuration": {"query": {"query": "SELECT * FROM \"test_dataset\".\"users\" ORDER BY id"}}}`
	resp, err := http.Post(baseURL+"/bigquery/v2/projects/test-project/jobs", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("POST /jobs error = %v", err)
	}
	var jobResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&jobResp)
	resp.Body.Close()

	jobRef := jobResp["jobReference"].(map[string]interface{})
	jobID := jobRef["jobId"].(string)

	// Wait for completion
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		getResp, _ := http.Get(baseURL + "/bigquery/v2/projects/test-project/jobs/" + jobID)
		var getBody map[string]interface{}
		json.NewDecoder(getResp.Body).Decode(&getBody)
		getResp.Body.Close()
		status := getBody["status"].(map[string]interface{})
		if status["state"] == "DONE" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Get query results
	qResp, err := http.Get(baseURL + "/bigquery/v2/projects/test-project/queries/" + jobID)
	if err != nil {
		t.Fatalf("GET /queries/{id} error = %v", err)
	}
	defer qResp.Body.Close()

	var qBody map[string]interface{}
	json.NewDecoder(qResp.Body).Decode(&qBody)

	if qBody["kind"] != "bigquery#getQueryResultsResponse" {
		t.Errorf("kind = %v, want bigquery#getQueryResultsResponse", qBody["kind"])
	}
	if qBody["jobComplete"] != true {
		t.Errorf("jobComplete = %v, want true", qBody["jobComplete"])
	}
	if qBody["totalRows"] != "3" {
		t.Errorf("totalRows = %v, want '3'", qBody["totalRows"])
	}

	// Verify schema
	schema, ok := qBody["schema"].(map[string]interface{})
	if !ok {
		t.Fatal("schema missing from response")
	}
	fields, ok := schema["fields"].([]interface{})
	if !ok || len(fields) < 2 {
		t.Fatalf("schema fields count = %d, want >= 2", len(fields))
	}

	// Verify rows in BQ format
	rows, ok := qBody["rows"].([]interface{})
	if !ok || len(rows) != 3 {
		t.Fatalf("rows count = %v, want 3", len(rows))
	}

	// Verify row structure: {"f": [{"v": "value"}]}
	firstRow, ok := rows[0].(map[string]interface{})
	if !ok {
		t.Fatal("first row is not an object")
	}
	fFields, ok := firstRow["f"].([]interface{})
	if !ok {
		t.Fatal("first row missing 'f' field")
	}
	if len(fFields) < 2 {
		t.Fatalf("first row fields count = %d, want >= 2", len(fFields))
	}
	firstField, ok := fFields[0].(map[string]interface{})
	if !ok {
		t.Fatal("first field is not an object")
	}
	if _, hasV := firstField["v"]; !hasV {
		t.Error("first field missing 'v' key")
	}
}

func TestGetQueryResults_Pagination(t *testing.T) {
	baseURL := setupJobTestServer(t)

	// Submit a query job
	body := `{"configuration": {"query": {"query": "SELECT * FROM \"test_dataset\".\"users\" ORDER BY id"}}}`
	resp, err := http.Post(baseURL+"/bigquery/v2/projects/test-project/jobs", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("POST /jobs error = %v", err)
	}
	var jobResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&jobResp)
	resp.Body.Close()

	jobRef := jobResp["jobReference"].(map[string]interface{})
	jobID := jobRef["jobId"].(string)

	// Wait for completion
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		getResp, _ := http.Get(baseURL + "/bigquery/v2/projects/test-project/jobs/" + jobID)
		var getBody map[string]interface{}
		json.NewDecoder(getResp.Body).Decode(&getBody)
		getResp.Body.Close()
		status := getBody["status"].(map[string]interface{})
		if status["state"] == "DONE" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Get first page (maxResults=2)
	qResp, err := http.Get(baseURL + "/bigquery/v2/projects/test-project/queries/" + jobID + "?maxResults=2&startIndex=0")
	if err != nil {
		t.Fatalf("GET /queries/{id} error = %v", err)
	}
	var qBody map[string]interface{}
	json.NewDecoder(qResp.Body).Decode(&qBody)
	qResp.Body.Close()

	if qBody["totalRows"] != "3" {
		t.Errorf("totalRows = %v, want '3'", qBody["totalRows"])
	}

	rows, ok := qBody["rows"].([]interface{})
	if !ok || len(rows) != 2 {
		t.Fatalf("first page rows = %v, want 2", len(rows))
	}

	// Should have a page token
	pageToken, ok := qBody["pageToken"].(string)
	if !ok || pageToken == "" {
		t.Fatal("expected pageToken for first page")
	}

	// Get second page
	qResp2, err := http.Get(baseURL + "/bigquery/v2/projects/test-project/queries/" + jobID + "?maxResults=2&startIndex=" + pageToken)
	if err != nil {
		t.Fatalf("GET /queries/{id} page 2 error = %v", err)
	}
	var qBody2 map[string]interface{}
	json.NewDecoder(qResp2.Body).Decode(&qBody2)
	qResp2.Body.Close()

	rows2, ok := qBody2["rows"].([]interface{})
	if !ok || len(rows2) != 1 {
		t.Fatalf("second page rows = %v, want 1", len(rows2))
	}

	// No more pages
	if _, has := qBody2["pageToken"]; has {
		t.Error("expected no pageToken for last page")
	}
}
