package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestDataset_Response_HasEtag(t *testing.T) {
	s := setupDatasetTestServer(t)

	body := map[string]interface{}{
		"datasetReference": map[string]interface{}{
			"projectId": "test-project",
			"datasetId": "etag_dataset",
		},
		"location": "US",
	}

	rr := doDatasetRequest(t, s, http.MethodPost, "/bigquery/v2/projects/test-project/datasets", body)
	if rr.Code != http.StatusOK {
		t.Fatalf("create dataset failed: %d %s", rr.Code, rr.Body.String())
	}

	resp := decodeDatasetJSON(t, rr)

	// Verify etag is present and non-empty
	etag, ok := resp["etag"].(string)
	if !ok || etag == "" {
		t.Errorf("expected non-empty etag, got %v", resp["etag"])
	}

	// Verify selfLink is present
	selfLink, ok := resp["selfLink"].(string)
	if !ok || selfLink == "" {
		t.Errorf("expected non-empty selfLink, got %v", resp["selfLink"])
	}

	// Verify location defaults to US
	if resp["location"] != "US" {
		t.Errorf("expected location US, got %v", resp["location"])
	}

	// Now GET and verify etag is consistent
	rr = doDatasetRequest(t, s, http.MethodGet, "/bigquery/v2/projects/test-project/datasets/etag_dataset", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("get dataset failed: %d %s", rr.Code, rr.Body.String())
	}

	getResp := decodeDatasetJSON(t, rr)
	getEtag, ok := getResp["etag"].(string)
	if !ok || getEtag == "" {
		t.Errorf("GET: expected non-empty etag, got %v", getResp["etag"])
	}

	// Etag should be deterministic for same resource
	if etag != getEtag {
		t.Errorf("etag mismatch: create=%q, get=%q", etag, getEtag)
	}
}

func TestDataset_Response_LocationDefaultsToUS(t *testing.T) {
	s := setupDatasetTestServer(t)

	// Create dataset without location
	body := map[string]interface{}{
		"datasetReference": map[string]interface{}{
			"projectId": "test-project",
			"datasetId": "no_loc_dataset",
		},
	}

	rr := doDatasetRequest(t, s, http.MethodPost, "/bigquery/v2/projects/test-project/datasets", body)
	if rr.Code != http.StatusOK {
		t.Fatalf("create dataset failed: %d %s", rr.Code, rr.Body.String())
	}

	resp := decodeDatasetJSON(t, rr)

	// Location should default to "US" even when not provided
	if resp["location"] != "US" {
		t.Errorf("expected location US (default), got %v", resp["location"])
	}
}

func TestTable_Response_HasEtag(t *testing.T) {
	s := setupTableTestServer(t)
	createTestDataset(t, s.repo, "test-project", "test_dataset")

	body := map[string]interface{}{
		"tableReference": map[string]string{
			"projectId": "test-project",
			"datasetId": "test_dataset",
			"tableId":   "etag_table",
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
		t.Fatalf("create table failed: %d %s", w.Code, w.Body.String())
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	// Verify etag is present and non-empty
	etag, ok := resp["etag"].(string)
	if !ok || etag == "" {
		t.Errorf("expected non-empty etag, got %v", resp["etag"])
	}

	// Verify selfLink is present
	selfLink, ok := resp["selfLink"].(string)
	if !ok || selfLink == "" {
		t.Errorf("expected non-empty selfLink, got %v", resp["selfLink"])
	}

	// Verify location is present
	if resp["location"] != "US" {
		t.Errorf("expected location US, got %v", resp["location"])
	}

	// Now GET and verify etag is consistent
	req = httptest.NewRequest(http.MethodGet, "/bigquery/v2/projects/test-project/datasets/test_dataset/tables/etag_table", nil)
	w = httptest.NewRecorder()
	s.router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("get table failed: %d %s", w.Code, w.Body.String())
	}

	var getResp map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &getResp)

	getEtag, ok := getResp["etag"].(string)
	if !ok || getEtag == "" {
		t.Errorf("GET: expected non-empty etag, got %v", getResp["etag"])
	}

	if etag != getEtag {
		t.Errorf("etag mismatch: create=%q, get=%q", etag, getEtag)
	}
}

func TestJob_Response_HasLocation(t *testing.T) {
	baseURL := setupJobTestServer(t)

	body := `{"configuration": {"query": {"query": "SELECT 1 AS num"}}}`
	resp, err := http.Post(baseURL+"/bigquery/v2/projects/test-project/jobs", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("POST /jobs error = %v", err)
	}
	defer resp.Body.Close()

	var jobResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&jobResp)

	// Verify etag is present
	etag, ok := jobResp["etag"].(string)
	if !ok || etag == "" {
		t.Errorf("expected non-empty etag, got %v", jobResp["etag"])
	}

	// Verify user_email is present
	if jobResp["user_email"] != "emulator@localhost" {
		t.Errorf("expected user_email emulator@localhost, got %v", jobResp["user_email"])
	}

	// Verify jobReference has location
	jobRef, ok := jobResp["jobReference"].(map[string]interface{})
	if !ok {
		t.Fatal("jobReference missing")
	}
	if jobRef["location"] != "US" {
		t.Errorf("expected jobReference.location US, got %v", jobRef["location"])
	}
}

func TestQueryResults_HasCacheHit(t *testing.T) {
	baseURL := setupJobTestServer(t)

	// Submit a query job
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

	// Verify cacheHit is present
	cacheHit, ok := qBody["cacheHit"].(bool)
	if !ok {
		t.Fatalf("cacheHit missing or wrong type, got %v (%T)", qBody["cacheHit"], qBody["cacheHit"])
	}
	if cacheHit != false {
		t.Errorf("expected cacheHit false, got %v", cacheHit)
	}

	// Verify etag is present
	etag, ok := qBody["etag"].(string)
	if !ok || etag == "" {
		t.Errorf("expected non-empty etag, got %v", qBody["etag"])
	}

	// Verify jobReference has location
	qJobRef, ok := qBody["jobReference"].(map[string]interface{})
	if !ok {
		t.Fatal("jobReference missing from query results")
	}
	if qJobRef["location"] != "US" {
		t.Errorf("expected jobReference.location US, got %v", qJobRef["location"])
	}

	// Test queriesInsert (synchronous query) also has cacheHit
	syncBody := `{"query": "SELECT 1 AS num"}`
	syncResp, err := http.Post(baseURL+"/bigquery/v2/projects/test-project/queries", "application/json", bytes.NewBufferString(syncBody))
	if err != nil {
		t.Fatalf("POST /queries error = %v", err)
	}
	defer syncResp.Body.Close()

	var syncResult map[string]interface{}
	json.NewDecoder(syncResp.Body).Decode(&syncResult)

	syncCacheHit, ok := syncResult["cacheHit"].(bool)
	if !ok {
		t.Fatalf("queriesInsert: cacheHit missing or wrong type, got %v (%T)", syncResult["cacheHit"], syncResult["cacheHit"])
	}
	if syncCacheHit != false {
		t.Errorf("queriesInsert: expected cacheHit false, got %v", syncCacheHit)
	}
}
