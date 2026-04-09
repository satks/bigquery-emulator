package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// setupStorageTestServer creates a fully wired test server with an in-memory DuckDB,
// a test project, dataset, and table pre-populated with data for storage API tests.
// It returns the httptest.Server URL.
func setupStorageTestServer(t *testing.T) string {
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

	// Create table with schema
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
		t.Fatalf("insert data error = %v", err)
	}
	resp.Body.Close()

	return ts.URL
}

func TestServer_StorageRoutes_CreateReadSession(t *testing.T) {
	baseURL := setupStorageTestServer(t)

	// POST to create read session (no body required - table is in URL)
	resp, err := http.Post(
		baseURL+"/v1/projects/test-project/datasets/test_dataset/tables/users:createReadSession",
		"application/json",
		bytes.NewBufferString("{}"),
	)
	if err != nil {
		t.Fatalf("POST createReadSession error = %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST createReadSession status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	var session map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&session); err != nil {
		t.Fatalf("decode response error = %v", err)
	}

	// Verify session has a name
	sessionName, ok := session["name"].(string)
	if !ok || sessionName == "" {
		t.Error("session response missing 'name' field")
	}

	// Verify session has streams
	streams, ok := session["streams"].([]interface{})
	if !ok || len(streams) == 0 {
		t.Error("session response missing or empty 'streams' field")
	}

	// Verify session has schema info
	if _, ok := session["schema"]; !ok {
		t.Error("session response missing 'schema' field")
	}

	// Verify data format is ARROW
	dataFormat, _ := session["dataFormat"].(string)
	if dataFormat != "ARROW" {
		t.Errorf("session dataFormat = %q, want %q", dataFormat, "ARROW")
	}

	// Verify estimated row count (we inserted 3 rows)
	rowCount, _ := session["estimatedRowCount"].(float64) // JSON numbers decode as float64
	if rowCount != 3 {
		t.Errorf("session estimatedRowCount = %v, want 3", rowCount)
	}
}

func TestServer_StorageRoutes_ReadRows(t *testing.T) {
	baseURL := setupStorageTestServer(t)

	// Step 1: Create read session
	resp, err := http.Post(
		baseURL+"/v1/projects/test-project/datasets/test_dataset/tables/users:createReadSession",
		"application/json",
		bytes.NewBufferString("{}"),
	)
	if err != nil {
		t.Fatalf("POST createReadSession error = %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST createReadSession status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	var session map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&session); err != nil {
		t.Fatalf("decode session error = %v", err)
	}

	// Get the first stream name
	streams := session["streams"].([]interface{})
	if len(streams) == 0 {
		t.Fatal("no streams in session")
	}
	stream := streams[0].(map[string]interface{})
	streamName := stream["name"].(string)

	// Step 2: Read rows from the stream using /v1/readStreams/{streamName}:readRows
	readResp, err := http.Get(baseURL + "/v1/readStreams/" + streamName + ":readRows")
	if err != nil {
		t.Fatalf("GET readRows error = %v", err)
	}
	defer readResp.Body.Close()

	if readResp.StatusCode != http.StatusOK {
		t.Fatalf("GET readRows status = %d, want %d", readResp.StatusCode, http.StatusOK)
	}

	// Verify the response contains Arrow IPC data
	ct := readResp.Header.Get("Content-Type")
	if ct != "application/vnd.apache.arrow.stream" {
		t.Errorf("readRows Content-Type = %q, want %q", ct, "application/vnd.apache.arrow.stream")
	}
}

func TestServer_StorageRoutes_WriteFlow(t *testing.T) {
	baseURL := setupStorageTestServer(t)

	// Create an empty table for writing
	tblBody := `{
		"tableReference":{"projectId":"test-project","datasetId":"test_dataset","tableId":"write_target"},
		"schema":{"fields":[
			{"name":"id","type":"INT64","mode":"REQUIRED"},
			{"name":"value","type":"STRING"}
		]}
	}`
	resp, err := http.Post(
		baseURL+"/bigquery/v2/projects/test-project/datasets/test_dataset/tables",
		"application/json",
		bytes.NewBufferString(tblBody),
	)
	if err != nil {
		t.Fatalf("create write_target table error = %v", err)
	}
	resp.Body.Close()

	// Step 1: Create write stream
	writeStreamReq := map[string]interface{}{
		"type": "COMMITTED",
	}
	bodyBytes, _ := json.Marshal(writeStreamReq)

	resp, err = http.Post(
		baseURL+"/v1/projects/test-project/datasets/test_dataset/tables/write_target:createWriteStream",
		"application/json",
		bytes.NewBuffer(bodyBytes),
	)
	if err != nil {
		t.Fatalf("POST createWriteStream error = %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST createWriteStream status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	var writeStreamResp map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&writeStreamResp); err != nil {
		t.Fatalf("decode write stream error = %v", err)
	}

	streamName, ok := writeStreamResp["name"].(string)
	if !ok || streamName == "" {
		t.Fatal("write stream response missing 'name' field")
	}

	// Verify type is COMMITTED
	wsType, _ := writeStreamResp["type"].(string)
	if wsType != "COMMITTED" {
		t.Errorf("write stream type = %q, want %q", wsType, "COMMITTED")
	}

	// Step 2: Append rows using the AppendRowsRequest format (rows is []map[string]interface{})
	appendReq := map[string]interface{}{
		"rows": []map[string]interface{}{
			{"id": 100, "value": "test-one"},
			{"id": 200, "value": "test-two"},
		},
	}
	appendBytes, _ := json.Marshal(appendReq)

	resp2, err := http.Post(
		baseURL+"/v1/writeStreams/"+streamName+":appendRows",
		"application/json",
		bytes.NewBuffer(appendBytes),
	)
	if err != nil {
		t.Fatalf("POST appendRows error = %v", err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("POST appendRows status = %d, want %d", resp2.StatusCode, http.StatusOK)
	}

	// Step 3: Verify data via regular query (list table data)
	dataResp, err := http.Get(baseURL + "/bigquery/v2/projects/test-project/datasets/test_dataset/tables/write_target/data")
	if err != nil {
		t.Fatalf("GET table data error = %v", err)
	}
	defer dataResp.Body.Close()

	var dataResult map[string]interface{}
	if err := json.NewDecoder(dataResp.Body).Decode(&dataResult); err != nil {
		t.Fatalf("decode data result error = %v", err)
	}

	totalRows, ok := dataResult["totalRows"].(string)
	if !ok {
		t.Fatal("missing totalRows in data response")
	}
	if totalRows != "2" {
		t.Errorf("totalRows = %q, want %q (rows should have been written via Storage API)", totalRows, "2")
	}
}

func TestServer_StorageRoutes_Registered(t *testing.T) {
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
	defer srv.Stop(nil)

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// Verify storage routes are registered (should NOT return chi's default 404)
	routes := []struct {
		method string
		path   string
	}{
		{"POST", "/v1/projects/test-project/datasets/ds1/tables/t1:createReadSession"},
		{"POST", "/v1/projects/test-project/datasets/ds1/tables/t1:createWriteStream"},
	}

	for _, rt := range routes {
		req, err := http.NewRequest(rt.method, ts.URL+rt.path, bytes.NewBufferString("{}"))
		if err != nil {
			t.Fatalf("NewRequest(%s %s) error = %v", rt.method, rt.path, err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Do(%s %s) error = %v", rt.method, rt.path, err)
		}

		ct := resp.Header.Get("Content-Type")
		resp.Body.Close()

		// Our handlers return application/json or application/vnd.apache.arrow.stream
		// Chi's default 404 returns text/plain
		if ct == "text/plain; charset=utf-8" || ct == "text/plain" {
			t.Errorf("%s %s returned Content-Type %q (route not registered?)", rt.method, rt.path, ct)
		}
	}
}

func TestServer_StorageService_NotNil(t *testing.T) {
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
	defer srv.Stop(nil)

	if srv.StorageService() == nil {
		t.Error("StorageService() returned nil, expected non-nil")
	}
}
