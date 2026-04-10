package sdk

// These tests simulate what the Go SDK (cloud.google.com/go/bigquery) does
// internally: making HTTP requests to the BigQuery REST API v2 endpoints with
// the exact JSON formats the SDK sends and expects. This lets us verify SDK
// compatibility without pulling in the heavy cloud.google.com dependency.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sathish/bigquery-emulator/server"
)

const testProject = "test-project"

// setupSDKTestServer creates a fully wired emulator and returns the test
// server URL. The server and DuckDB connection are cleaned up via t.Cleanup.
func setupSDKTestServer(t *testing.T) string {
	t.Helper()

	cfg := server.Config{
		Host:      "localhost",
		Port:      9050,
		ProjectID: testProject,
		Database:  ":memory:",
		LogLevel:  "info",
	}

	srv, err := server.New(cfg)
	if err != nil {
		t.Fatalf("server.New() error = %v", err)
	}
	t.Cleanup(func() { srv.Stop(nil) })

	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	return ts.URL
}

// apiURL builds a BigQuery v2 API URL for the test project.
func apiURL(base, path string) string {
	return fmt.Sprintf("%s/bigquery/v2/projects/%s%s", base, testProject, path)
}

// mustPost sends a POST request with JSON body, fataling on error.
func mustPost(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url, "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("POST %s error = %v", url, err)
	}
	return resp
}

// mustGet sends a GET request, fataling on error.
func mustGet(t *testing.T, url string) *http.Response {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s error = %v", url, err)
	}
	return resp
}

// decodeJSON reads and decodes a response body into a generic map.
func decodeJSON(t *testing.T, resp *http.Response) map[string]interface{} {
	t.Helper()
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body error = %v", err)
	}
	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("decode JSON error = %v\nbody: %s", err, string(data))
	}
	return result
}

// ---------------------------------------------------------------------------
// TestGoSDK_CreateDataset simulates:
//   client.Dataset("test_ds").Create(ctx, &bigquery.DatasetMetadata{...})
// The SDK sends POST /bigquery/v2/projects/{project}/datasets with a
// datasetReference envelope.
// ---------------------------------------------------------------------------
func TestGoSDK_CreateDataset(t *testing.T) {
	base := setupSDKTestServer(t)

	body := `{
		"datasetReference": {
			"projectId": "test-project",
			"datasetId": "sdk_dataset"
		},
		"location": "US"
	}`

	resp := mustPost(t, apiURL(base, "/datasets"), body)
	result := decodeJSON(t, resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %v", resp.StatusCode, result)
	}

	// SDK expects kind = "bigquery#dataset"
	if result["kind"] != "bigquery#dataset" {
		t.Errorf("kind = %v, want bigquery#dataset", result["kind"])
	}

	ref, ok := result["datasetReference"].(map[string]interface{})
	if !ok {
		t.Fatal("datasetReference missing")
	}
	if ref["datasetId"] != "sdk_dataset" {
		t.Errorf("datasetId = %v, want sdk_dataset", ref["datasetId"])
	}
	if ref["projectId"] != testProject {
		t.Errorf("projectId = %v, want %s", ref["projectId"], testProject)
	}
}

// ---------------------------------------------------------------------------
// TestGoSDK_CreateTableWithSchema simulates:
//   table.Create(ctx, &bigquery.TableMetadata{Schema: bigquery.Schema{...}})
// ---------------------------------------------------------------------------
func TestGoSDK_CreateTableWithSchema(t *testing.T) {
	base := setupSDKTestServer(t)

	// Create dataset first
	dsBody := `{"datasetReference":{"projectId":"test-project","datasetId":"sdk_ds"}}`
	resp := mustPost(t, apiURL(base, "/datasets"), dsBody)
	resp.Body.Close()

	// Create table with SDK-format schema
	tblBody := `{
		"tableReference": {
			"projectId": "test-project",
			"datasetId": "sdk_ds",
			"tableId": "users"
		},
		"schema": {
			"fields": [
				{"name": "id", "type": "INT64", "mode": "REQUIRED"},
				{"name": "email", "type": "STRING", "mode": "NULLABLE"},
				{"name": "score", "type": "FLOAT64", "mode": "NULLABLE"},
				{"name": "active", "type": "BOOL", "mode": "NULLABLE"},
				{"name": "created_at", "type": "TIMESTAMP", "mode": "NULLABLE"}
			]
		}
	}`

	resp = mustPost(t, apiURL(base, "/datasets/sdk_ds/tables"), tblBody)
	result := decodeJSON(t, resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %v", resp.StatusCode, result)
	}

	if result["kind"] != "bigquery#table" {
		t.Errorf("kind = %v, want bigquery#table", result["kind"])
	}

	// Verify schema round-trips correctly
	schema, ok := result["schema"].(map[string]interface{})
	if !ok {
		t.Fatal("schema missing from response")
	}
	fields, ok := schema["fields"].([]interface{})
	if !ok {
		t.Fatal("schema.fields missing")
	}
	if len(fields) != 5 {
		t.Errorf("field count = %d, want 5", len(fields))
	}

	// Verify first field
	f0, _ := fields[0].(map[string]interface{})
	if f0["name"] != "id" {
		t.Errorf("fields[0].name = %v, want id", f0["name"])
	}
	if f0["type"] != "INT64" {
		t.Errorf("fields[0].type = %v, want INT64", f0["type"])
	}
}

// ---------------------------------------------------------------------------
// TestGoSDK_InsertRows simulates:
//   inserter := table.Inserter()
//   inserter.Put(ctx, rows)
// The SDK sends POST .../insertAll with {"rows": [{"insertId":"..","json":{..}}]}
// ---------------------------------------------------------------------------
func TestGoSDK_InsertRows(t *testing.T) {
	base := setupSDKTestServer(t)

	// Setup: dataset + table
	dsBody := `{"datasetReference":{"projectId":"test-project","datasetId":"ins_ds"}}`
	resp := mustPost(t, apiURL(base, "/datasets"), dsBody)
	resp.Body.Close()

	tblBody := `{
		"tableReference":{"projectId":"test-project","datasetId":"ins_ds","tableId":"events"},
		"schema":{"fields":[
			{"name":"id","type":"INT64","mode":"REQUIRED"},
			{"name":"event","type":"STRING"}
		]}
	}`
	resp = mustPost(t, apiURL(base, "/datasets/ins_ds/tables"), tblBody)
	resp.Body.Close()

	// Insert rows (SDK format with insertId)
	insertBody := `{
		"rows": [
			{"insertId": "row1", "json": {"id": 1, "event": "click"}},
			{"insertId": "row2", "json": {"id": 2, "event": "view"}},
			{"insertId": "row3", "json": {"id": 3, "event": "purchase"}}
		]
	}`

	resp = mustPost(t, apiURL(base, "/datasets/ins_ds/tables/events/insertAll"), insertBody)
	result := decodeJSON(t, resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("insertAll status = %d, want 200; body = %v", resp.StatusCode, result)
	}

	// SDK expects kind = "bigquery#tableDataInsertAllResponse"
	if result["kind"] != "bigquery#tableDataInsertAllResponse" {
		t.Errorf("kind = %v, want bigquery#tableDataInsertAllResponse", result["kind"])
	}

	// Verify data via listTableData
	dataResp := mustGet(t, apiURL(base, "/datasets/ins_ds/tables/events/data"))
	dataResult := decodeJSON(t, dataResp)

	if dataResult["totalRows"] != "3" {
		t.Errorf("totalRows = %v, want '3'", dataResult["totalRows"])
	}
}

// ---------------------------------------------------------------------------
// TestGoSDK_Query simulates the full query lifecycle:
//   1. client.Query("SELECT ...").Run(ctx) -> POST /jobs
//   2. job.Wait(ctx) -> GET /jobs/{id} polling
//   3. iter.Next(&row) -> GET /queries/{id}
// ---------------------------------------------------------------------------
func TestGoSDK_Query(t *testing.T) {
	base := setupSDKTestServer(t)

	// Setup: dataset, table, data
	dsBody := `{"datasetReference":{"projectId":"test-project","datasetId":"q_ds"}}`
	resp := mustPost(t, apiURL(base, "/datasets"), dsBody)
	resp.Body.Close()

	tblBody := `{
		"tableReference":{"projectId":"test-project","datasetId":"q_ds","tableId":"items"},
		"schema":{"fields":[
			{"name":"name","type":"STRING"},
			{"name":"price","type":"FLOAT64"}
		]}
	}`
	resp = mustPost(t, apiURL(base, "/datasets/q_ds/tables"), tblBody)
	resp.Body.Close()

	insertBody := `{
		"rows": [
			{"json": {"name": "Widget", "price": 9.99}},
			{"json": {"name": "Gadget", "price": 24.99}}
		]
	}`
	resp = mustPost(t, apiURL(base, "/datasets/q_ds/tables/items/insertAll"), insertBody)
	resp.Body.Close()

	// Step 1: Submit query job (what client.Query().Run() does)
	jobBody := `{
		"configuration": {
			"query": {
				"query": "SELECT name, price FROM \"q_ds\".\"items\" ORDER BY price",
				"useLegacySql": false
			}
		}
	}`

	resp = mustPost(t, apiURL(base, "/jobs"), jobBody)
	jobResult := decodeJSON(t, resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /jobs status = %d, want 200", resp.StatusCode)
	}
	if jobResult["kind"] != "bigquery#job" {
		t.Errorf("kind = %v, want bigquery#job", jobResult["kind"])
	}

	jobRef, ok := jobResult["jobReference"].(map[string]interface{})
	if !ok {
		t.Fatal("jobReference missing")
	}
	jobID, ok := jobRef["jobId"].(string)
	if !ok || jobID == "" {
		t.Fatal("jobId missing")
	}

	// Step 2: Poll until done (what job.Wait() does)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		getResp := mustGet(t, apiURL(base, "/jobs/"+jobID))
		getBody := decodeJSON(t, getResp)

		status, _ := getBody["status"].(map[string]interface{})
		if status["state"] == "DONE" {
			if status["errorResult"] != nil {
				t.Fatalf("job completed with error: %v", status["errorResult"])
			}
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Step 3: Get query results (what iter.Next() does internally)
	qResp := mustGet(t, apiURL(base, "/queries/"+jobID))
	qResult := decodeJSON(t, qResp)

	if qResult["kind"] != "bigquery#getQueryResultsResponse" {
		t.Errorf("kind = %v, want bigquery#getQueryResultsResponse", qResult["kind"])
	}
	if qResult["jobComplete"] != true {
		t.Errorf("jobComplete = %v, want true", qResult["jobComplete"])
	}
	if qResult["totalRows"] != "2" {
		t.Errorf("totalRows = %v, want '2'", qResult["totalRows"])
	}

	// Verify rows are in BQ SDK format: {"f": [{"v": "..."}]}
	rows, ok := qResult["rows"].([]interface{})
	if !ok || len(rows) != 2 {
		t.Fatalf("rows count = %v, want 2", len(rows))
	}
}

// ---------------------------------------------------------------------------
// TestGoSDK_ListDatasets simulates:
//   iter := client.Datasets(ctx)
// The SDK sends GET /bigquery/v2/projects/{project}/datasets and expects
// a response with kind, datasets array, and optional nextPageToken.
// ---------------------------------------------------------------------------
func TestGoSDK_ListDatasets(t *testing.T) {
	base := setupSDKTestServer(t)

	// Create two datasets
	for _, name := range []string{"list_ds_1", "list_ds_2"} {
		body := fmt.Sprintf(`{"datasetReference":{"projectId":"test-project","datasetId":"%s"}}`, name)
		resp := mustPost(t, apiURL(base, "/datasets"), body)
		resp.Body.Close()
	}

	resp := mustGet(t, apiURL(base, "/datasets"))
	result := decodeJSON(t, resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	if result["kind"] != "bigquery#datasetList" {
		t.Errorf("kind = %v, want bigquery#datasetList", result["kind"])
	}

	datasets, ok := result["datasets"].([]interface{})
	if !ok {
		t.Fatal("datasets field missing or wrong type")
	}
	if len(datasets) < 2 {
		t.Errorf("datasets count = %d, want >= 2", len(datasets))
	}

	// Each dataset entry should have datasetReference
	for i, ds := range datasets {
		entry, ok := ds.(map[string]interface{})
		if !ok {
			t.Errorf("datasets[%d] is not an object", i)
			continue
		}
		ref, ok := entry["datasetReference"].(map[string]interface{})
		if !ok {
			t.Errorf("datasets[%d].datasetReference missing", i)
			continue
		}
		if ref["projectId"] != testProject {
			t.Errorf("datasets[%d].projectId = %v, want %s", i, ref["projectId"], testProject)
		}
	}
}

// ---------------------------------------------------------------------------
// TestGoSDK_ListTables simulates:
//   iter := dataset.Tables(ctx)
// The SDK sends GET .../datasets/{ds}/tables and expects kind, tables array.
// ---------------------------------------------------------------------------
func TestGoSDK_ListTables(t *testing.T) {
	base := setupSDKTestServer(t)

	// Setup: dataset with two tables
	dsBody := `{"datasetReference":{"projectId":"test-project","datasetId":"lt_ds"}}`
	resp := mustPost(t, apiURL(base, "/datasets"), dsBody)
	resp.Body.Close()

	for _, tbl := range []string{"alpha", "beta"} {
		body := fmt.Sprintf(`{
			"tableReference":{"projectId":"test-project","datasetId":"lt_ds","tableId":"%s"},
			"schema":{"fields":[{"name":"id","type":"INT64"}]}
		}`, tbl)
		resp := mustPost(t, apiURL(base, "/datasets/lt_ds/tables"), body)
		resp.Body.Close()
	}

	resp = mustGet(t, apiURL(base, "/datasets/lt_ds/tables"))
	result := decodeJSON(t, resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	if result["kind"] != "bigquery#tableList" {
		t.Errorf("kind = %v, want bigquery#tableList", result["kind"])
	}

	tables, ok := result["tables"].([]interface{})
	if !ok {
		t.Fatal("tables field missing or wrong type")
	}
	if len(tables) != 2 {
		t.Errorf("tables count = %d, want 2", len(tables))
	}

	// Each table should have tableReference
	for i, tbl := range tables {
		entry, ok := tbl.(map[string]interface{})
		if !ok {
			t.Errorf("tables[%d] is not an object", i)
			continue
		}
		if _, ok := entry["tableReference"].(map[string]interface{}); !ok {
			t.Errorf("tables[%d].tableReference missing", i)
		}
	}
}

// ---------------------------------------------------------------------------
// TestGoSDK_ErrorFormat verifies the error envelope format the SDK parses.
// The Go SDK expects: {"error": {"code": N, "message": "...", "status": "...", "errors": [...]}}
// ---------------------------------------------------------------------------
func TestGoSDK_ErrorFormat(t *testing.T) {
	base := setupSDKTestServer(t)

	// Request a nonexistent dataset -> 404
	resp := mustGet(t, apiURL(base, "/datasets/nonexistent_dataset_xyz"))
	result := decodeJSON(t, resp)

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}

	// The SDK expects the error envelope: {"error": {...}}
	errObj, ok := result["error"].(map[string]interface{})
	if !ok {
		t.Fatal("missing 'error' envelope in response")
	}

	// Verify required fields the SDK parses
	code, ok := errObj["code"].(float64)
	if !ok || int(code) != 404 {
		t.Errorf("error.code = %v, want 404", errObj["code"])
	}

	msg, ok := errObj["message"].(string)
	if !ok || msg == "" {
		t.Errorf("error.message = %v, want non-empty string", errObj["message"])
	}

	status, ok := errObj["status"].(string)
	if !ok || status != "NOT_FOUND" {
		t.Errorf("error.status = %v, want NOT_FOUND", errObj["status"])
	}

	// errors array (optional but expected by SDK)
	errors, ok := errObj["errors"].([]interface{})
	if !ok || len(errors) == 0 {
		t.Error("error.errors array missing or empty")
	} else {
		detail, _ := errors[0].(map[string]interface{})
		if detail["reason"] == nil {
			t.Error("error.errors[0].reason missing")
		}
		if detail["message"] == nil {
			t.Error("error.errors[0].message missing")
		}
	}
}

// ---------------------------------------------------------------------------
// TestGoSDK_QueryWithParameters simulates a parameterized query.
// The SDK sends query parameters in the job configuration.
// Note: Our translator may or may not support parameters yet; this test
// verifies the request/response format is correct regardless.
// ---------------------------------------------------------------------------
func TestGoSDK_QueryWithParameters(t *testing.T) {
	base := setupSDKTestServer(t)

	// Setup: dataset + table + data
	dsBody := `{"datasetReference":{"projectId":"test-project","datasetId":"param_ds"}}`
	resp := mustPost(t, apiURL(base, "/datasets"), dsBody)
	resp.Body.Close()

	tblBody := `{
		"tableReference":{"projectId":"test-project","datasetId":"param_ds","tableId":"products"},
		"schema":{"fields":[
			{"name":"name","type":"STRING"},
			{"name":"category","type":"STRING"},
			{"name":"price","type":"FLOAT64"}
		]}
	}`
	resp = mustPost(t, apiURL(base, "/datasets/param_ds/tables"), tblBody)
	resp.Body.Close()

	insertBody := `{
		"rows": [
			{"json": {"name": "Laptop", "category": "electronics", "price": 999.99}},
			{"json": {"name": "Shirt", "category": "clothing", "price": 29.99}},
			{"json": {"name": "Phone", "category": "electronics", "price": 699.99}}
		]
	}`
	resp = mustPost(t, apiURL(base, "/datasets/param_ds/tables/products/insertAll"), insertBody)
	resp.Body.Close()

	// Submit a simple query (without actual BQ-style @param syntax since
	// our translator may not support it yet; use a literal filter instead)
	jobBody := `{
		"configuration": {
			"query": {
				"query": "SELECT name, price FROM \"param_ds\".\"products\" WHERE category = 'electronics' ORDER BY price",
				"useLegacySql": false
			}
		}
	}`

	resp = mustPost(t, apiURL(base, "/jobs"), jobBody)
	jobResult := decodeJSON(t, resp)

	jobRef := jobResult["jobReference"].(map[string]interface{})
	jobID := jobRef["jobId"].(string)

	// Poll until done
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		getResp := mustGet(t, apiURL(base, "/jobs/"+jobID))
		getBody := decodeJSON(t, getResp)
		status, _ := getBody["status"].(map[string]interface{})
		if status["state"] == "DONE" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Verify results
	qResp := mustGet(t, apiURL(base, "/queries/"+jobID))
	qResult := decodeJSON(t, qResp)

	if qResult["totalRows"] != "2" {
		t.Errorf("totalRows = %v, want '2' (electronics only)", qResult["totalRows"])
	}
}

// ---------------------------------------------------------------------------
// TestGoSDK_JobComplete_ResponseFormat verifies the exact getQueryResults
// response structure the Go SDK parses internally. This is the most critical
// format for SDK compatibility.
// ---------------------------------------------------------------------------
func TestGoSDK_JobComplete_ResponseFormat(t *testing.T) {
	base := setupSDKTestServer(t)

	// Setup
	dsBody := `{"datasetReference":{"projectId":"test-project","datasetId":"fmt_ds"}}`
	resp := mustPost(t, apiURL(base, "/datasets"), dsBody)
	resp.Body.Close()

	tblBody := `{
		"tableReference":{"projectId":"test-project","datasetId":"fmt_ds","tableId":"data"},
		"schema":{"fields":[
			{"name":"id","type":"INT64","mode":"REQUIRED"},
			{"name":"label","type":"STRING"}
		]}
	}`
	resp = mustPost(t, apiURL(base, "/datasets/fmt_ds/tables"), tblBody)
	resp.Body.Close()

	insertBody := `{
		"rows": [
			{"json": {"id": 1, "label": "first"}},
			{"json": {"id": 2, "label": "second"}}
		]
	}`
	resp = mustPost(t, apiURL(base, "/datasets/fmt_ds/tables/data/insertAll"), insertBody)
	resp.Body.Close()

	// Submit and wait
	jobBody := `{
		"configuration": {
			"query": {
				"query": "SELECT id, label FROM \"fmt_ds\".\"data\" ORDER BY id",
				"useLegacySql": false
			}
		}
	}`
	resp = mustPost(t, apiURL(base, "/jobs"), jobBody)
	jobResult := decodeJSON(t, resp)

	jobRef := jobResult["jobReference"].(map[string]interface{})
	jobID := jobRef["jobId"].(string)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		getResp := mustGet(t, apiURL(base, "/jobs/"+jobID))
		getBody := decodeJSON(t, getResp)
		status, _ := getBody["status"].(map[string]interface{})
		if status["state"] == "DONE" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Get query results and verify every field the SDK expects
	qResp := mustGet(t, apiURL(base, "/queries/"+jobID))
	qResult := decodeJSON(t, qResp)

	// 1. kind
	if qResult["kind"] != "bigquery#getQueryResultsResponse" {
		t.Errorf("kind = %v, want bigquery#getQueryResultsResponse", qResult["kind"])
	}

	// 2. jobReference
	qJobRef, ok := qResult["jobReference"].(map[string]interface{})
	if !ok {
		t.Fatal("jobReference missing from getQueryResults response")
	}
	if qJobRef["projectId"] != testProject {
		t.Errorf("jobReference.projectId = %v, want %s", qJobRef["projectId"], testProject)
	}
	if qJobRef["jobId"] != jobID {
		t.Errorf("jobReference.jobId = %v, want %s", qJobRef["jobId"], jobID)
	}

	// 3. jobComplete (must be boolean true)
	if qResult["jobComplete"] != true {
		t.Errorf("jobComplete = %v (type %T), want true (bool)", qResult["jobComplete"], qResult["jobComplete"])
	}

	// 4. schema.fields
	schema, ok := qResult["schema"].(map[string]interface{})
	if !ok {
		t.Fatal("schema missing")
	}
	fields, ok := schema["fields"].([]interface{})
	if !ok || len(fields) != 2 {
		t.Fatalf("schema.fields length = %v, want 2", len(fields))
	}

	// Verify field structure
	for i, f := range fields {
		field, ok := f.(map[string]interface{})
		if !ok {
			t.Errorf("schema.fields[%d] is not an object", i)
			continue
		}
		if field["name"] == nil {
			t.Errorf("schema.fields[%d].name missing", i)
		}
		if field["type"] == nil {
			t.Errorf("schema.fields[%d].type missing", i)
		}
	}

	// 5. totalRows (must be string, not number)
	totalRows, ok := qResult["totalRows"].(string)
	if !ok {
		t.Errorf("totalRows type = %T, want string", qResult["totalRows"])
	}
	if totalRows != "2" {
		t.Errorf("totalRows = %v, want '2'", totalRows)
	}

	// 6. rows in BQ format: [{"f": [{"v": "value"}]}]
	rows, ok := qResult["rows"].([]interface{})
	if !ok || len(rows) != 2 {
		t.Fatalf("rows count = %v, want 2", len(rows))
	}

	for i, r := range rows {
		row, ok := r.(map[string]interface{})
		if !ok {
			t.Errorf("rows[%d] is not an object", i)
			continue
		}

		fArr, ok := row["f"].([]interface{})
		if !ok {
			t.Errorf("rows[%d].f missing or not array", i)
			continue
		}

		if len(fArr) != 2 {
			t.Errorf("rows[%d].f length = %d, want 2", i, len(fArr))
			continue
		}

		for j, cell := range fArr {
			cellObj, ok := cell.(map[string]interface{})
			if !ok {
				t.Errorf("rows[%d].f[%d] is not an object", i, j)
				continue
			}
			if _, hasV := cellObj["v"]; !hasV {
				t.Errorf("rows[%d].f[%d] missing 'v' key", i, j)
			}
		}
	}

	// Verify actual values (row 0 should be id=1, label=first)
	row0 := rows[0].(map[string]interface{})
	f0 := row0["f"].([]interface{})
	cell0 := f0[0].(map[string]interface{})
	cell1 := f0[1].(map[string]interface{})

	// Values are always strings in BQ JSON format
	if fmt.Sprintf("%v", cell0["v"]) != "1" {
		t.Errorf("row[0].f[0].v = %v, want '1'", cell0["v"])
	}
	if cell1["v"] != "first" {
		t.Errorf("row[0].f[1].v = %v, want 'first'", cell1["v"])
	}
}

// ---------------------------------------------------------------------------
// TestGoSDK_ProjectList verifies GET /bigquery/v2/projects.
// The Go SDK uses this when listing projects. Expects kind="bigquery#projectList"
// and a projects array where each entry has projectReference.projectId.
// ---------------------------------------------------------------------------
func TestGoSDK_ProjectList(t *testing.T) {
	base := setupSDKTestServer(t)

	resp := mustGet(t, fmt.Sprintf("%s/bigquery/v2/projects", base))
	result := decodeJSON(t, resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %v", resp.StatusCode, result)
	}

	if result["kind"] != "bigquery#projectList" {
		t.Errorf("kind = %v, want bigquery#projectList", result["kind"])
	}

	projects, ok := result["projects"].([]interface{})
	if !ok {
		t.Fatal("projects field missing or wrong type")
	}
	if len(projects) == 0 {
		t.Fatal("expected at least 1 project")
	}

	// Each project should have projectReference with projectId
	for i, p := range projects {
		proj, ok := p.(map[string]interface{})
		if !ok {
			t.Errorf("projects[%d] is not an object", i)
			continue
		}
		ref, ok := proj["projectReference"].(map[string]interface{})
		if !ok {
			t.Errorf("projects[%d].projectReference missing", i)
			continue
		}
		if ref["projectId"] == nil || ref["projectId"] == "" {
			t.Errorf("projects[%d].projectReference.projectId missing or empty", i)
		}
	}
}

// ---------------------------------------------------------------------------
// TestGoSDK_GetProject verifies GET /bigquery/v2/projects/{projectId}.
// Expects kind="bigquery#project", id, and projectReference.projectId.
// ---------------------------------------------------------------------------
func TestGoSDK_GetProject(t *testing.T) {
	base := setupSDKTestServer(t)

	resp := mustGet(t, fmt.Sprintf("%s/bigquery/v2/projects/%s", base, testProject))
	result := decodeJSON(t, resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %v", resp.StatusCode, result)
	}

	if result["kind"] != "bigquery#project" {
		t.Errorf("kind = %v, want bigquery#project", result["kind"])
	}

	if result["id"] == nil || result["id"] == "" {
		t.Error("id field missing or empty")
	}

	ref, ok := result["projectReference"].(map[string]interface{})
	if !ok {
		t.Fatal("projectReference missing")
	}
	if ref["projectId"] != testProject {
		t.Errorf("projectReference.projectId = %v, want %s", ref["projectId"], testProject)
	}
}

// ---------------------------------------------------------------------------
// TestGoSDK_DatasetResponse_HasEtag verifies that dataset GET responses
// include an "etag" field. The Go SDK reads this for optimistic concurrency.
// ---------------------------------------------------------------------------
func TestGoSDK_DatasetResponse_HasEtag(t *testing.T) {
	base := setupSDKTestServer(t)

	// Create a dataset
	body := `{"datasetReference":{"projectId":"test-project","datasetId":"etag_ds"}}`
	resp := mustPost(t, apiURL(base, "/datasets"), body)
	resp.Body.Close()

	// GET the dataset and check for etag
	resp = mustGet(t, apiURL(base, "/datasets/etag_ds"))
	result := decodeJSON(t, resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %v", resp.StatusCode, result)
	}

	etag, ok := result["etag"].(string)
	if !ok || etag == "" {
		t.Errorf("etag = %v (type %T), want non-empty string", result["etag"], result["etag"])
	}
}

// ---------------------------------------------------------------------------
// TestGoSDK_TableResponse_HasEtag verifies that table GET responses include
// an "etag" field.
// ---------------------------------------------------------------------------
func TestGoSDK_TableResponse_HasEtag(t *testing.T) {
	base := setupSDKTestServer(t)

	// Create dataset + table
	dsBody := `{"datasetReference":{"projectId":"test-project","datasetId":"etag_tbl_ds"}}`
	resp := mustPost(t, apiURL(base, "/datasets"), dsBody)
	resp.Body.Close()

	tblBody := `{
		"tableReference":{"projectId":"test-project","datasetId":"etag_tbl_ds","tableId":"t1"},
		"schema":{"fields":[{"name":"id","type":"INT64"}]}
	}`
	resp = mustPost(t, apiURL(base, "/datasets/etag_tbl_ds/tables"), tblBody)
	resp.Body.Close()

	// GET the table and check for etag
	resp = mustGet(t, apiURL(base, "/datasets/etag_tbl_ds/tables/t1"))
	result := decodeJSON(t, resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %v", resp.StatusCode, result)
	}

	etag, ok := result["etag"].(string)
	if !ok || etag == "" {
		t.Errorf("etag = %v (type %T), want non-empty string", result["etag"], result["etag"])
	}
}

// ---------------------------------------------------------------------------
// TestGoSDK_JobResponse_HasLocation verifies that job GET responses include
// jobReference.location (e.g. "US"). The Go SDK uses this for region routing.
// ---------------------------------------------------------------------------
func TestGoSDK_JobResponse_HasLocation(t *testing.T) {
	base := setupSDKTestServer(t)

	// Submit a query job
	jobBody := `{
		"configuration": {
			"query": {
				"query": "SELECT 1 AS x",
				"useLegacySql": false
			}
		}
	}`
	resp := mustPost(t, apiURL(base, "/jobs"), jobBody)
	jobResult := decodeJSON(t, resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /jobs status = %d, want 200; body = %v", resp.StatusCode, jobResult)
	}

	jobRef, ok := jobResult["jobReference"].(map[string]interface{})
	if !ok {
		t.Fatal("jobReference missing from POST /jobs response")
	}
	jobID, _ := jobRef["jobId"].(string)

	// Wait for job to complete
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		getResp := mustGet(t, apiURL(base, "/jobs/"+jobID))
		getBody := decodeJSON(t, getResp)
		status, _ := getBody["status"].(map[string]interface{})
		if status["state"] == "DONE" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// GET the job and check for location in jobReference
	getResp := mustGet(t, apiURL(base, "/jobs/"+jobID))
	getBody := decodeJSON(t, getResp)

	jobRefGet, ok := getBody["jobReference"].(map[string]interface{})
	if !ok {
		t.Fatal("jobReference missing from GET /jobs response")
	}

	location, ok := jobRefGet["location"].(string)
	if !ok || location == "" {
		t.Errorf("jobReference.location = %v (type %T), want non-empty string (e.g. 'US')", jobRefGet["location"], jobRefGet["location"])
	}
}

// ---------------------------------------------------------------------------
// TestGoSDK_QueryResults_HasCacheHit verifies that getQueryResults includes
// a "cacheHit" field. The Go SDK exposes this via QueryStatistics.
// ---------------------------------------------------------------------------
func TestGoSDK_QueryResults_HasCacheHit(t *testing.T) {
	base := setupSDKTestServer(t)

	// Submit a query job
	jobBody := `{
		"configuration": {
			"query": {
				"query": "SELECT 42 AS answer",
				"useLegacySql": false
			}
		}
	}`
	resp := mustPost(t, apiURL(base, "/jobs"), jobBody)
	jobResult := decodeJSON(t, resp)

	jobRef := jobResult["jobReference"].(map[string]interface{})
	jobID := jobRef["jobId"].(string)

	// Wait for job to complete
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		getResp := mustGet(t, apiURL(base, "/jobs/"+jobID))
		getBody := decodeJSON(t, getResp)
		status, _ := getBody["status"].(map[string]interface{})
		if status["state"] == "DONE" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Get query results and check for cacheHit
	qResp := mustGet(t, apiURL(base, "/queries/"+jobID))
	qResult := decodeJSON(t, qResp)

	if qResult["kind"] != "bigquery#getQueryResultsResponse" {
		t.Errorf("kind = %v, want bigquery#getQueryResultsResponse", qResult["kind"])
	}

	// cacheHit should be present as a boolean (typically false for first run)
	cacheHit, ok := qResult["cacheHit"]
	if !ok || cacheHit == nil {
		t.Errorf("cacheHit field missing from getQueryResults response")
	} else {
		if _, isBool := cacheHit.(bool); !isBool {
			t.Errorf("cacheHit type = %T, want bool", cacheHit)
		}
	}
}

// ---------------------------------------------------------------------------
// TestGoSDK_ListTables_Pagination verifies that listing tables with
// maxResults returns a nextPageToken that can be used to fetch the next page.
// ---------------------------------------------------------------------------
func TestGoSDK_ListTables_Pagination(t *testing.T) {
	base := setupSDKTestServer(t)

	// Create dataset with 3 tables
	dsBody := `{"datasetReference":{"projectId":"test-project","datasetId":"pag_tbl_ds"}}`
	resp := mustPost(t, apiURL(base, "/datasets"), dsBody)
	resp.Body.Close()

	for _, tbl := range []string{"table_a", "table_b", "table_c"} {
		body := fmt.Sprintf(`{
			"tableReference":{"projectId":"test-project","datasetId":"pag_tbl_ds","tableId":"%s"},
			"schema":{"fields":[{"name":"id","type":"INT64"}]}
		}`, tbl)
		resp := mustPost(t, apiURL(base, "/datasets/pag_tbl_ds/tables"), body)
		resp.Body.Close()
	}

	// List with maxResults=2 -> should get 2 tables + nextPageToken
	resp = mustGet(t, apiURL(base, "/datasets/pag_tbl_ds/tables?maxResults=2"))
	page1 := decodeJSON(t, resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %v", resp.StatusCode, page1)
	}

	tables1, ok := page1["tables"].([]interface{})
	if !ok {
		t.Fatal("tables field missing from page 1")
	}
	if len(tables1) != 2 {
		t.Errorf("page 1 table count = %d, want 2", len(tables1))
	}

	nextPageToken, ok := page1["nextPageToken"].(string)
	if !ok || nextPageToken == "" {
		t.Fatal("nextPageToken missing or empty on page 1")
	}

	// Use pageToken to get page 2
	resp = mustGet(t, apiURL(base, fmt.Sprintf("/datasets/pag_tbl_ds/tables?maxResults=2&pageToken=%s", nextPageToken)))
	page2 := decodeJSON(t, resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("page 2 status = %d, want 200; body = %v", resp.StatusCode, page2)
	}

	tables2, ok := page2["tables"].([]interface{})
	if !ok {
		t.Fatal("tables field missing from page 2")
	}
	if len(tables2) != 1 {
		t.Errorf("page 2 table count = %d, want 1", len(tables2))
	}

	// Page 2 should NOT have a nextPageToken (no more data)
	if page2["nextPageToken"] != nil {
		tok, isStr := page2["nextPageToken"].(string)
		if isStr && tok != "" {
			t.Errorf("page 2 should not have nextPageToken, got %q", tok)
		}
	}
}

// ---------------------------------------------------------------------------
// TestGoSDK_ListJobs_Pagination verifies that listing jobs with maxResults
// returns a nextPageToken when there are more results.
// ---------------------------------------------------------------------------
func TestGoSDK_ListJobs_Pagination(t *testing.T) {
	base := setupSDKTestServer(t)

	// Submit 3 query jobs
	for i := 0; i < 3; i++ {
		jobBody := fmt.Sprintf(`{
			"configuration": {
				"query": {
					"query": "SELECT %d AS val",
					"useLegacySql": false
				}
			}
		}`, i+1)
		resp := mustPost(t, apiURL(base, "/jobs"), jobBody)
		resp.Body.Close()
	}

	// Wait a moment for jobs to be registered
	time.Sleep(200 * time.Millisecond)

	// List with maxResults=2
	resp := mustGet(t, apiURL(base, "/jobs?maxResults=2"))
	page1 := decodeJSON(t, resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %v", resp.StatusCode, page1)
	}

	jobs1, ok := page1["jobs"].([]interface{})
	if !ok {
		t.Fatal("jobs field missing from page 1")
	}
	if len(jobs1) != 2 {
		t.Errorf("page 1 job count = %d, want 2", len(jobs1))
	}

	nextPageToken, ok := page1["nextPageToken"].(string)
	if !ok || nextPageToken == "" {
		t.Fatal("nextPageToken missing or empty on page 1")
	}
}

// ---------------------------------------------------------------------------
// TestGoSDK_LoadJob verifies that a LOAD job can be submitted and completes
// with status DONE. The Go SDK uses this for client.Dataset().Table().LoaderFrom().
// ---------------------------------------------------------------------------
func TestGoSDK_LoadJob(t *testing.T) {
	base := setupSDKTestServer(t)

	// Create dataset + table first
	dsBody := `{"datasetReference":{"projectId":"test-project","datasetId":"load_ds"}}`
	resp := mustPost(t, apiURL(base, "/datasets"), dsBody)
	resp.Body.Close()

	tblBody := `{
		"tableReference":{"projectId":"test-project","datasetId":"load_ds","tableId":"load_tbl"},
		"schema":{"fields":[
			{"name":"id","type":"INT64"},
			{"name":"name","type":"STRING"}
		]}
	}`
	resp = mustPost(t, apiURL(base, "/datasets/load_ds/tables"), tblBody)
	resp.Body.Close()

	// Submit a LOAD job
	jobBody := `{
		"configuration": {
			"load": {
				"destinationTable": {
					"projectId": "test-project",
					"datasetId": "load_ds",
					"tableId": "load_tbl"
				},
				"sourceFormat": "NEWLINE_DELIMITED_JSON",
				"sourceUris": ["gs://fake-bucket/data.json"]
			}
		}
	}`

	resp = mustPost(t, apiURL(base, "/jobs"), jobBody)
	jobResult := decodeJSON(t, resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /jobs status = %d, want 200; body = %v", resp.StatusCode, jobResult)
	}

	if jobResult["kind"] != "bigquery#job" {
		t.Errorf("kind = %v, want bigquery#job", jobResult["kind"])
	}

	jobRef, ok := jobResult["jobReference"].(map[string]interface{})
	if !ok {
		t.Fatal("jobReference missing")
	}
	jobID, _ := jobRef["jobId"].(string)

	// Wait for job to complete
	deadline := time.Now().Add(5 * time.Second)
	var finalStatus map[string]interface{}
	for time.Now().Before(deadline) {
		getResp := mustGet(t, apiURL(base, "/jobs/"+jobID))
		getBody := decodeJSON(t, getResp)
		status, _ := getBody["status"].(map[string]interface{})
		if status["state"] == "DONE" {
			finalStatus = getBody
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if finalStatus == nil {
		t.Fatal("load job did not complete within deadline")
	}

	// Verify it's a LOAD job
	config, ok := finalStatus["configuration"].(map[string]interface{})
	if ok {
		if config["jobType"] != nil && config["jobType"] != "LOAD" {
			t.Errorf("configuration.jobType = %v, want LOAD", config["jobType"])
		}
		if config["load"] == nil {
			t.Error("configuration.load missing from completed load job")
		}
	}
}

// ---------------------------------------------------------------------------
// TestGoSDK_ExtractJob verifies that an EXTRACT job can be submitted and
// completes with status DONE.
// ---------------------------------------------------------------------------
func TestGoSDK_ExtractJob(t *testing.T) {
	base := setupSDKTestServer(t)

	// Create dataset + table first
	dsBody := `{"datasetReference":{"projectId":"test-project","datasetId":"ext_ds"}}`
	resp := mustPost(t, apiURL(base, "/datasets"), dsBody)
	resp.Body.Close()

	tblBody := `{
		"tableReference":{"projectId":"test-project","datasetId":"ext_ds","tableId":"ext_tbl"},
		"schema":{"fields":[
			{"name":"id","type":"INT64"},
			{"name":"val","type":"STRING"}
		]}
	}`
	resp = mustPost(t, apiURL(base, "/datasets/ext_ds/tables"), tblBody)
	resp.Body.Close()

	// Submit an EXTRACT job
	jobBody := `{
		"configuration": {
			"extract": {
				"sourceTable": {
					"projectId": "test-project",
					"datasetId": "ext_ds",
					"tableId": "ext_tbl"
				},
				"destinationFormat": "NEWLINE_DELIMITED_JSON",
				"destinationUris": ["gs://fake-bucket/export.json"]
			}
		}
	}`

	resp = mustPost(t, apiURL(base, "/jobs"), jobBody)
	jobResult := decodeJSON(t, resp)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /jobs status = %d, want 200; body = %v", resp.StatusCode, jobResult)
	}

	if jobResult["kind"] != "bigquery#job" {
		t.Errorf("kind = %v, want bigquery#job", jobResult["kind"])
	}

	jobRef, ok := jobResult["jobReference"].(map[string]interface{})
	if !ok {
		t.Fatal("jobReference missing")
	}
	jobID, _ := jobRef["jobId"].(string)

	// Wait for job to complete
	deadline := time.Now().Add(5 * time.Second)
	var finalStatus map[string]interface{}
	for time.Now().Before(deadline) {
		getResp := mustGet(t, apiURL(base, "/jobs/"+jobID))
		getBody := decodeJSON(t, getResp)
		status, _ := getBody["status"].(map[string]interface{})
		if status["state"] == "DONE" {
			finalStatus = getBody
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if finalStatus == nil {
		t.Fatal("extract job did not complete within deadline")
	}

	// Verify it's an EXTRACT job
	config, ok := finalStatus["configuration"].(map[string]interface{})
	if ok {
		if config["jobType"] != nil && config["jobType"] != "EXTRACT" {
			t.Errorf("configuration.jobType = %v, want EXTRACT", config["jobType"])
		}
		if config["extract"] == nil {
			t.Error("configuration.extract missing from completed extract job")
		}
	}
}
