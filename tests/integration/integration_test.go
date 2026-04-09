package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sathish/bigquery-emulator/server"
)

// ---------------------------------------------------------------------------
// Test Helpers
// ---------------------------------------------------------------------------

func setupIntegrationServer(t *testing.T) (*httptest.Server, func()) {
	t.Helper()
	cfg := server.Config{
		Host:      "localhost",
		Port:      0,
		ProjectID: "test-project",
		Database:  ":memory:",
	}
	s, err := server.New(cfg)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	ts := httptest.NewServer(s.Handler())
	return ts, func() { ts.Close() }
}

func doRequest(t *testing.T, ts *httptest.Server, method, path string, body interface{}) *http.Response {
	t.Helper()
	var reqBody io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("failed to marshal request body: %v", err)
		}
		reqBody = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, ts.URL+path, reqBody)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	return resp
}

func readJSON(t *testing.T, resp *http.Response) map[string]interface{} {
	t.Helper()
	defer resp.Body.Close()
	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	return result
}

// bqPath returns the BigQuery v2 API prefix for the test project.
func bqPath(parts ...string) string {
	return "/bigquery/v2/projects/test-project/" + strings.Join(parts, "/")
}

// createDatasetHelper creates a dataset and fails the test on error.
func createDatasetHelper(t *testing.T, ts *httptest.Server, datasetID string) {
	t.Helper()
	body := map[string]interface{}{
		"datasetReference": map[string]string{
			"projectId": "test-project",
			"datasetId": datasetID,
		},
	}
	resp := doRequest(t, ts, http.MethodPost, bqPath("datasets"), body)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("createDataset %s failed: %d %s", datasetID, resp.StatusCode, string(b))
	}
	// drain body
	io.ReadAll(resp.Body)
}

// createTableHelper creates a table with the given schema fields.
func createTableHelper(t *testing.T, ts *httptest.Server, datasetID, tableID string, fields []map[string]interface{}) {
	t.Helper()
	body := map[string]interface{}{
		"tableReference": map[string]string{
			"projectId": "test-project",
			"datasetId": datasetID,
			"tableId":   tableID,
		},
		"schema": map[string]interface{}{
			"fields": fields,
		},
	}
	resp := doRequest(t, ts, http.MethodPost, bqPath("datasets", datasetID, "tables"), body)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("createTable %s.%s failed: %d %s", datasetID, tableID, resp.StatusCode, string(b))
	}
	io.ReadAll(resp.Body)
}

// insertRowsHelper inserts rows via insertAll and fails on error.
func insertRowsHelper(t *testing.T, ts *httptest.Server, datasetID, tableID string, rows []map[string]interface{}) {
	t.Helper()
	insertRows := make([]map[string]interface{}, len(rows))
	for i, r := range rows {
		insertRows[i] = map[string]interface{}{
			"json": r,
		}
	}
	body := map[string]interface{}{
		"rows": insertRows,
	}
	resp := doRequest(t, ts, http.MethodPost, bqPath("datasets", datasetID, "tables", tableID, "insertAll"), body)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("insertAll %s.%s failed: %d %s", datasetID, tableID, resp.StatusCode, string(b))
	}
	result := readJSON(t, resp)
	if errs, ok := result["insertErrors"]; ok && errs != nil {
		errArr, _ := errs.([]interface{})
		if len(errArr) > 0 {
			t.Fatalf("insertAll returned errors: %v", errs)
		}
	}
}

// submitQueryJob submits a query job and returns the job ID.
func submitQueryJob(t *testing.T, ts *httptest.Server, sql string) string {
	t.Helper()
	body := map[string]interface{}{
		"configuration": map[string]interface{}{
			"query": map[string]interface{}{
				"query":        sql,
				"useLegacySql": false,
			},
		},
	}
	resp := doRequest(t, ts, http.MethodPost, bqPath("jobs"), body)
	result := readJSON(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("submitJob failed: %d %v", resp.StatusCode, result)
	}
	jobRef, ok := result["jobReference"].(map[string]interface{})
	if !ok {
		t.Fatalf("missing jobReference in response: %v", result)
	}
	jobID, ok := jobRef["jobId"].(string)
	if !ok || jobID == "" {
		t.Fatalf("missing jobId in jobReference: %v", jobRef)
	}
	return jobID
}

// waitForJob polls the job until DONE or timeout.
func waitForJob(t *testing.T, ts *httptest.Server, jobID string) map[string]interface{} {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp := doRequest(t, ts, http.MethodGet, bqPath("jobs", jobID), nil)
		result := readJSON(t, resp)
		status, _ := result["status"].(map[string]interface{})
		if status != nil && status["state"] == "DONE" {
			return result
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("job %s did not complete within 5s", jobID)
	return nil
}

// getQueryResults fetches the query results for a completed job.
func getQueryResults(t *testing.T, ts *httptest.Server, jobID string) map[string]interface{} {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp := doRequest(t, ts, http.MethodGet, bqPath("queries", jobID), nil)
		result := readJSON(t, resp)
		if complete, ok := result["jobComplete"].(bool); ok && complete {
			return result
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("query results not ready for job %s within 5s", jobID)
	return nil
}

// ---------------------------------------------------------------------------
// Integration Tests
// ---------------------------------------------------------------------------

func TestIntegration_DatasetLifecycle(t *testing.T) {
	ts, cleanup := setupIntegrationServer(t)
	defer cleanup()

	// 1. Create dataset "ds1"
	body := map[string]interface{}{
		"datasetReference": map[string]string{
			"projectId": "test-project",
			"datasetId": "ds1",
		},
		"friendlyName": "Dataset One",
		"description":  "First dataset",
		"location":     "US",
	}
	resp := doRequest(t, ts, http.MethodPost, bqPath("datasets"), body)
	result := readJSON(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if result["kind"] != "bigquery#dataset" {
		t.Fatalf("expected kind=bigquery#dataset, got %v", result["kind"])
	}

	// 2. Get dataset "ds1" -> verify fields
	resp = doRequest(t, ts, http.MethodGet, bqPath("datasets", "ds1"), nil)
	result = readJSON(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get dataset: expected 200, got %d", resp.StatusCode)
	}
	dsRef, ok := result["datasetReference"].(map[string]interface{})
	if !ok {
		t.Fatalf("missing datasetReference")
	}
	if dsRef["datasetId"] != "ds1" {
		t.Fatalf("expected datasetId=ds1, got %v", dsRef["datasetId"])
	}
	if dsRef["projectId"] != "test-project" {
		t.Fatalf("expected projectId=test-project, got %v", dsRef["projectId"])
	}
	if result["friendlyName"] != "Dataset One" {
		t.Fatalf("expected friendlyName=Dataset One, got %v", result["friendlyName"])
	}
	if result["description"] != "First dataset" {
		t.Fatalf("expected description=First dataset, got %v", result["description"])
	}

	// 3. List datasets -> verify ds1 in list
	resp = doRequest(t, ts, http.MethodGet, bqPath("datasets"), nil)
	result = readJSON(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list datasets: expected 200, got %d", resp.StatusCode)
	}
	datasets, ok := result["datasets"].([]interface{})
	if !ok || len(datasets) < 1 {
		t.Fatalf("expected at least 1 dataset, got %v", result["datasets"])
	}
	found := false
	for _, d := range datasets {
		ds := d.(map[string]interface{})
		ref := ds["datasetReference"].(map[string]interface{})
		if ref["datasetId"] == "ds1" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("ds1 not found in list")
	}

	// 4. Patch dataset (update description)
	patchBody := map[string]interface{}{
		"description": "Updated description",
	}
	resp = doRequest(t, ts, http.MethodPatch, bqPath("datasets", "ds1"), patchBody)
	result = readJSON(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("patch dataset: expected 200, got %d", resp.StatusCode)
	}
	if result["description"] != "Updated description" {
		t.Fatalf("expected description=Updated description, got %v", result["description"])
	}
	// friendlyName should remain unchanged
	if result["friendlyName"] != "Dataset One" {
		t.Fatalf("friendlyName was unexpectedly changed to %v", result["friendlyName"])
	}

	// 5. Delete dataset -> verify 204
	resp = doRequest(t, ts, http.MethodDelete, bqPath("datasets", "ds1"), nil)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("delete dataset: expected 204, got %d", resp.StatusCode)
	}

	// 6. Get dataset -> verify 404
	resp = doRequest(t, ts, http.MethodGet, bqPath("datasets", "ds1"), nil)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("get deleted dataset: expected 404, got %d", resp.StatusCode)
	}
}

func TestIntegration_TableLifecycle(t *testing.T) {
	ts, cleanup := setupIntegrationServer(t)
	defer cleanup()

	// 1. Create dataset
	createDatasetHelper(t, ts, "ds1")

	// 2. Create table with schema
	fields := []map[string]interface{}{
		{"name": "id", "type": "INT64", "mode": "REQUIRED"},
		{"name": "name", "type": "STRING", "mode": "NULLABLE"},
		{"name": "active", "type": "BOOL", "mode": "NULLABLE"},
	}
	createTableHelper(t, ts, "ds1", "t1", fields)

	// 3. Get table -> verify schema
	resp := doRequest(t, ts, http.MethodGet, bqPath("datasets", "ds1", "tables", "t1"), nil)
	result := readJSON(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get table: expected 200, got %d", resp.StatusCode)
	}
	if result["kind"] != "bigquery#table" {
		t.Fatalf("expected kind=bigquery#table, got %v", result["kind"])
	}
	tblRef := result["tableReference"].(map[string]interface{})
	if tblRef["tableId"] != "t1" {
		t.Fatalf("expected tableId=t1, got %v", tblRef["tableId"])
	}
	schema, ok := result["schema"].(map[string]interface{})
	if !ok {
		t.Fatal("missing schema in table response")
	}
	schFields, ok := schema["fields"].([]interface{})
	if !ok || len(schFields) != 3 {
		t.Fatalf("expected 3 schema fields, got %v", schema["fields"])
	}

	// Verify field names
	fieldNames := make([]string, len(schFields))
	for i, f := range schFields {
		field := f.(map[string]interface{})
		fieldNames[i] = field["name"].(string)
	}
	if fieldNames[0] != "id" || fieldNames[1] != "name" || fieldNames[2] != "active" {
		t.Fatalf("unexpected field names: %v", fieldNames)
	}

	// 4. List tables -> verify t1 in list
	resp = doRequest(t, ts, http.MethodGet, bqPath("datasets", "ds1", "tables"), nil)
	result = readJSON(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list tables: expected 200, got %d", resp.StatusCode)
	}
	tables := result["tables"].([]interface{})
	if len(tables) != 1 {
		t.Fatalf("expected 1 table, got %d", len(tables))
	}
	listedRef := tables[0].(map[string]interface{})["tableReference"].(map[string]interface{})
	if listedRef["tableId"] != "t1" {
		t.Fatalf("expected tableId=t1 in list, got %v", listedRef["tableId"])
	}

	// 5. Delete table -> verify 204
	resp = doRequest(t, ts, http.MethodDelete, bqPath("datasets", "ds1", "tables", "t1"), nil)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("delete table: expected 204, got %d", resp.StatusCode)
	}

	// 6. Get table -> verify 404
	resp = doRequest(t, ts, http.MethodGet, bqPath("datasets", "ds1", "tables", "t1"), nil)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("get deleted table: expected 404, got %d", resp.StatusCode)
	}
}

func TestIntegration_QueryJob_E2E(t *testing.T) {
	ts, cleanup := setupIntegrationServer(t)
	defer cleanup()

	// 1. Create dataset + table + insert data
	createDatasetHelper(t, ts, "ds1")
	fields := []map[string]interface{}{
		{"name": "id", "type": "INT64"},
		{"name": "name", "type": "STRING"},
	}
	createTableHelper(t, ts, "ds1", "t1", fields)
	insertRowsHelper(t, ts, "ds1", "t1", []map[string]interface{}{
		{"id": 1, "name": "Alice"},
		{"id": 2, "name": "Bob"},
		{"id": 3, "name": "Charlie"},
	})

	// 2. Submit query job
	jobID := submitQueryJob(t, ts, `SELECT * FROM ds1.t1 WHERE id > 1 ORDER BY id`)

	// 3. Poll job status until DONE
	jobResult := waitForJob(t, ts, jobID)
	status := jobResult["status"].(map[string]interface{})
	if status["state"] != "DONE" {
		t.Fatalf("expected DONE, got %v", status["state"])
	}
	// Verify no error
	if status["errorResult"] != nil {
		t.Fatalf("job failed with error: %v", status["errorResult"])
	}

	// 4. Get query results -> verify rows
	qr := getQueryResults(t, ts, jobID)
	totalRows := qr["totalRows"].(string)
	if totalRows != "2" {
		t.Fatalf("expected totalRows=2, got %s", totalRows)
	}
	rows := qr["rows"].([]interface{})
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}

	// The rows should contain Bob (id=2) and Charlie (id=3)
	row0 := rows[0].(map[string]interface{})["f"].([]interface{})
	row1 := rows[1].(map[string]interface{})["f"].([]interface{})
	id0 := row0[0].(map[string]interface{})["v"].(string)
	id1 := row1[0].(map[string]interface{})["v"].(string)
	if id0 != "2" || id1 != "3" {
		t.Fatalf("expected ids [2,3], got [%s,%s]", id0, id1)
	}
}

func TestIntegration_StreamingInsert_And_Query(t *testing.T) {
	ts, cleanup := setupIntegrationServer(t)
	defer cleanup()

	// 1. Create dataset + table
	createDatasetHelper(t, ts, "ds1")
	fields := []map[string]interface{}{
		{"name": "id", "type": "INT64"},
		{"name": "value", "type": "STRING"},
	}
	createTableHelper(t, ts, "ds1", "t1", fields)

	// 2. InsertAll with 5 rows
	rows := make([]map[string]interface{}, 5)
	for i := 0; i < 5; i++ {
		rows[i] = map[string]interface{}{
			"id":    i + 1,
			"value": fmt.Sprintf("val_%d", i+1),
		}
	}
	insertRowsHelper(t, ts, "ds1", "t1", rows)

	// 3. List table data -> verify 5 rows
	resp := doRequest(t, ts, http.MethodGet, bqPath("datasets", "ds1", "tables", "t1", "data"), nil)
	result := readJSON(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("listTableData: expected 200, got %d", resp.StatusCode)
	}
	totalRows := result["totalRows"].(string)
	if totalRows != "5" {
		t.Fatalf("expected totalRows=5, got %s", totalRows)
	}
	dataRows := result["rows"].([]interface{})
	if len(dataRows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(dataRows))
	}

	// 4. Submit query job: SELECT COUNT(*) as cnt FROM ds1.t1
	jobID := submitQueryJob(t, ts, "SELECT COUNT(*) as cnt FROM ds1.t1")
	waitForJob(t, ts, jobID)

	// 5. Get results -> verify cnt = 5
	qr := getQueryResults(t, ts, jobID)
	qrRows := qr["rows"].([]interface{})
	if len(qrRows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(qrRows))
	}
	cntField := qrRows[0].(map[string]interface{})["f"].([]interface{})
	cntVal := cntField[0].(map[string]interface{})["v"].(string)
	if cntVal != "5" {
		t.Fatalf("expected cnt=5, got %s", cntVal)
	}
}

func TestIntegration_MultipleDatasets(t *testing.T) {
	ts, cleanup := setupIntegrationServer(t)
	defer cleanup()

	// 1. Create ds1 and ds2
	createDatasetHelper(t, ts, "ds1")
	createDatasetHelper(t, ts, "ds2")

	// 2. Create table in each
	fields := []map[string]interface{}{
		{"name": "id", "type": "INT64"},
		{"name": "tag", "type": "STRING"},
	}
	createTableHelper(t, ts, "ds1", "t1", fields)
	createTableHelper(t, ts, "ds2", "t1", fields)

	// 3. Insert data in each
	insertRowsHelper(t, ts, "ds1", "t1", []map[string]interface{}{
		{"id": 1, "tag": "ds1_row1"},
		{"id": 2, "tag": "ds1_row2"},
	})
	insertRowsHelper(t, ts, "ds2", "t1", []map[string]interface{}{
		{"id": 10, "tag": "ds2_row1"},
		{"id": 20, "tag": "ds2_row2"},
		{"id": 30, "tag": "ds2_row3"},
	})

	// 4. Query from ds1 -> verify only ds1 data
	jobID1 := submitQueryJob(t, ts, "SELECT COUNT(*) as cnt FROM ds1.t1")
	waitForJob(t, ts, jobID1)
	qr1 := getQueryResults(t, ts, jobID1)
	cnt1 := qr1["rows"].([]interface{})[0].(map[string]interface{})["f"].([]interface{})[0].(map[string]interface{})["v"].(string)
	if cnt1 != "2" {
		t.Fatalf("ds1 expected cnt=2, got %s", cnt1)
	}

	// 5. Query from ds2 -> verify only ds2 data
	jobID2 := submitQueryJob(t, ts, "SELECT COUNT(*) as cnt FROM ds2.t1")
	waitForJob(t, ts, jobID2)
	qr2 := getQueryResults(t, ts, jobID2)
	cnt2 := qr2["rows"].([]interface{})[0].(map[string]interface{})["f"].([]interface{})[0].(map[string]interface{})["v"].(string)
	if cnt2 != "3" {
		t.Fatalf("ds2 expected cnt=3, got %s", cnt2)
	}
}

func TestIntegration_BQTypeRoundtrip(t *testing.T) {
	ts, cleanup := setupIntegrationServer(t)
	defer cleanup()

	createDatasetHelper(t, ts, "ds1")

	// 1. Create table with all major BQ types
	fields := []map[string]interface{}{
		{"name": "int_col", "type": "INT64"},
		{"name": "float_col", "type": "FLOAT64"},
		{"name": "bool_col", "type": "BOOL"},
		{"name": "str_col", "type": "STRING"},
		{"name": "ts_col", "type": "TIMESTAMP"},
	}
	createTableHelper(t, ts, "ds1", "types_table", fields)

	// 2. InsertAll with values of each type
	insertRowsHelper(t, ts, "ds1", "types_table", []map[string]interface{}{
		{
			"int_col":   42,
			"float_col": 3.14,
			"bool_col":  true,
			"str_col":   "hello",
			"ts_col":    "2024-01-15 10:30:00",
		},
	})

	// 3. Query back -> verify types preserved
	jobID := submitQueryJob(t, ts, "SELECT int_col, float_col, bool_col, str_col FROM ds1.types_table")
	waitForJob(t, ts, jobID)
	qr := getQueryResults(t, ts, jobID)

	rows := qr["rows"].([]interface{})
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	fields2 := rows[0].(map[string]interface{})["f"].([]interface{})

	intVal := fields2[0].(map[string]interface{})["v"].(string)
	if intVal != "42" {
		t.Fatalf("expected int_col=42, got %s", intVal)
	}

	floatVal := fields2[1].(map[string]interface{})["v"].(string)
	if floatVal != "3.14" {
		t.Fatalf("expected float_col=3.14, got %s", floatVal)
	}

	boolVal := fields2[2].(map[string]interface{})["v"].(string)
	if boolVal != "true" {
		t.Fatalf("expected bool_col=true, got %s", boolVal)
	}

	strVal := fields2[3].(map[string]interface{})["v"].(string)
	if strVal != "hello" {
		t.Fatalf("expected str_col=hello, got %s", strVal)
	}

	// Verify schema has the right types
	schemaFields := qr["schema"].(map[string]interface{})["fields"].([]interface{})
	if len(schemaFields) != 4 {
		t.Fatalf("expected 4 schema fields, got %d", len(schemaFields))
	}
}

func TestIntegration_Pagination(t *testing.T) {
	ts, cleanup := setupIntegrationServer(t)
	defer cleanup()

	createDatasetHelper(t, ts, "ds1")
	fields := []map[string]interface{}{
		{"name": "id", "type": "INT64"},
		{"name": "label", "type": "STRING"},
	}
	createTableHelper(t, ts, "ds1", "t1", fields)

	// 1. Insert 10 rows
	rows := make([]map[string]interface{}, 10)
	for i := 0; i < 10; i++ {
		rows[i] = map[string]interface{}{
			"id":    i + 1,
			"label": fmt.Sprintf("item_%d", i+1),
		}
	}
	insertRowsHelper(t, ts, "ds1", "t1", rows)

	// 2. List table data with maxResults=3
	resp := doRequest(t, ts, http.MethodGet, bqPath("datasets", "ds1", "tables", "t1", "data")+"?maxResults=3", nil)
	result := readJSON(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	totalRows := result["totalRows"].(string)
	if totalRows != "10" {
		t.Fatalf("expected totalRows=10, got %s", totalRows)
	}
	dataRows := result["rows"].([]interface{})
	if len(dataRows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(dataRows))
	}
	// Should have a pageToken for the next page
	pageToken, hasPT := result["pageToken"].(string)
	if !hasPT || pageToken == "" {
		t.Fatal("expected a pageToken for pagination")
	}

	// 3. Request with startIndex=3, maxResults=3
	resp = doRequest(t, ts, http.MethodGet, bqPath("datasets", "ds1", "tables", "t1", "data")+"?startIndex=3&maxResults=3", nil)
	result = readJSON(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	dataRows = result["rows"].([]interface{})
	if len(dataRows) != 3 {
		t.Fatalf("expected 3 rows on second page, got %d", len(dataRows))
	}
	// totalRows should still be 10
	if result["totalRows"].(string) != "10" {
		t.Fatalf("expected totalRows=10 on second page, got %s", result["totalRows"])
	}
}

func TestIntegration_ErrorHandling(t *testing.T) {
	ts, cleanup := setupIntegrationServer(t)
	defer cleanup()

	// 1. Get nonexistent dataset -> 404 with BQ error format
	resp := doRequest(t, ts, http.MethodGet, bqPath("datasets", "nonexistent"), nil)
	result := readJSON(t, resp)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
	errObj, ok := result["error"].(map[string]interface{})
	if !ok {
		t.Fatal("expected error envelope in response")
	}
	if errObj["code"].(float64) != 404 {
		t.Fatalf("expected error code=404, got %v", errObj["code"])
	}
	if errObj["status"] != "NOT_FOUND" {
		t.Fatalf("expected status=NOT_FOUND, got %v", errObj["status"])
	}

	// 2. Create duplicate dataset -> 409
	createDatasetHelper(t, ts, "dup_ds")
	dupBody := map[string]interface{}{
		"datasetReference": map[string]string{
			"projectId": "test-project",
			"datasetId": "dup_ds",
		},
	}
	resp = doRequest(t, ts, http.MethodPost, bqPath("datasets"), dupBody)
	resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("duplicate dataset: expected 409, got %d", resp.StatusCode)
	}

	// 3. Submit query with invalid SQL -> job completes with error
	jobID := submitQueryJob(t, ts, "SELEKT * FORM nothing")
	jobResult := waitForJob(t, ts, jobID)
	status := jobResult["status"].(map[string]interface{})
	if status["errorResult"] == nil {
		t.Fatal("expected errorResult for invalid SQL job")
	}
	errResult := status["errorResult"].(map[string]interface{})
	if errResult["reason"] == nil || errResult["reason"] == "" {
		t.Fatal("expected error reason")
	}

	// 4. Get nonexistent table -> 404
	createDatasetHelper(t, ts, "err_ds")
	resp = doRequest(t, ts, http.MethodGet, bqPath("datasets", "err_ds", "tables", "nonexistent"), nil)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("get nonexistent table: expected 404, got %d", resp.StatusCode)
	}
}

func TestIntegration_JobList(t *testing.T) {
	ts, cleanup := setupIntegrationServer(t)
	defer cleanup()

	createDatasetHelper(t, ts, "ds1")
	fields := []map[string]interface{}{
		{"name": "id", "type": "INT64"},
	}
	createTableHelper(t, ts, "ds1", "t1", fields)
	insertRowsHelper(t, ts, "ds1", "t1", []map[string]interface{}{
		{"id": 1},
		{"id": 2},
	})

	// 1. Submit 3 query jobs
	jobIDs := make([]string, 3)
	jobIDs[0] = submitQueryJob(t, ts, "SELECT COUNT(*) FROM ds1.t1")
	jobIDs[1] = submitQueryJob(t, ts, "SELECT * FROM ds1.t1 ORDER BY id")
	jobIDs[2] = submitQueryJob(t, ts, "SELECT id FROM ds1.t1 WHERE id = 1")

	// Wait for all to complete
	for _, id := range jobIDs {
		waitForJob(t, ts, id)
	}

	// 2. List jobs -> verify all 3 present
	resp := doRequest(t, ts, http.MethodGet, bqPath("jobs"), nil)
	result := readJSON(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list jobs: expected 200, got %d", resp.StatusCode)
	}
	jobs := result["jobs"].([]interface{})
	if len(jobs) < 3 {
		t.Fatalf("expected at least 3 jobs, got %d", len(jobs))
	}

	// 3. Get each job -> verify status DONE
	for _, id := range jobIDs {
		resp = doRequest(t, ts, http.MethodGet, bqPath("jobs", id), nil)
		jr := readJSON(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("get job %s: expected 200, got %d", id, resp.StatusCode)
		}
		st := jr["status"].(map[string]interface{})
		if st["state"] != "DONE" {
			t.Fatalf("job %s expected DONE, got %v", id, st["state"])
		}
	}
}

func TestIntegration_StorageAPI_ReadFlow(t *testing.T) {
	ts, cleanup := setupIntegrationServer(t)
	defer cleanup()

	// 1. Create dataset + table + insert data
	createDatasetHelper(t, ts, "ds1")
	fields := []map[string]interface{}{
		{"name": "id", "type": "INT64"},
		{"name": "name", "type": "STRING"},
	}
	createTableHelper(t, ts, "ds1", "t1", fields)
	insertRowsHelper(t, ts, "ds1", "t1", []map[string]interface{}{
		{"id": 1, "name": "Alpha"},
		{"id": 2, "name": "Beta"},
	})

	// 2. Create read session
	resp := doRequest(t, ts, http.MethodPost, "/v1/projects/test-project/datasets/ds1/tables/t1:createReadSession", map[string]interface{}{})
	result := readJSON(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("createReadSession: expected 200, got %d", resp.StatusCode)
	}

	// Verify session fields
	if result["dataFormat"] != "ARROW" {
		t.Fatalf("expected dataFormat=ARROW, got %v", result["dataFormat"])
	}
	streams, ok := result["streams"].([]interface{})
	if !ok || len(streams) == 0 {
		t.Fatal("expected at least 1 stream in read session")
	}
	streamObj := streams[0].(map[string]interface{})
	streamName, ok := streamObj["name"].(string)
	if !ok || streamName == "" {
		t.Fatal("missing stream name")
	}

	// Verify row count
	rowCount := result["estimatedRowCount"].(float64)
	if rowCount != 2 {
		t.Fatalf("expected estimatedRowCount=2, got %v", rowCount)
	}

	// 3. Read rows from stream -> verify Content-Type is arrow
	readResp := doRequest(t, ts, http.MethodGet, fmt.Sprintf("/v1/readStreams/%s:readRows", streamName), nil)
	defer readResp.Body.Close()
	if readResp.StatusCode != http.StatusOK {
		t.Fatalf("readRows: expected 200, got %d", readResp.StatusCode)
	}
	ct := readResp.Header.Get("Content-Type")
	if ct != "application/vnd.apache.arrow.stream" {
		t.Fatalf("expected Content-Type=application/vnd.apache.arrow.stream, got %s", ct)
	}
	// Read the body to ensure it's non-empty Arrow IPC data
	arrowData, err := io.ReadAll(readResp.Body)
	if err != nil {
		t.Fatalf("failed to read arrow data: %v", err)
	}
	if len(arrowData) == 0 {
		t.Fatal("expected non-empty arrow data")
	}
}

func TestIntegration_TableData_InsertAll_And_List(t *testing.T) {
	ts, cleanup := setupIntegrationServer(t)
	defer cleanup()

	// 1. Create dataset + table
	createDatasetHelper(t, ts, "ds1")
	fields := []map[string]interface{}{
		{"name": "id", "type": "INT64"},
		{"name": "city", "type": "STRING"},
	}
	createTableHelper(t, ts, "ds1", "t1", fields)

	// 2. InsertAll 3 rows
	insertRowsHelper(t, ts, "ds1", "t1", []map[string]interface{}{
		{"id": 1, "city": "NYC"},
		{"id": 2, "city": "LA"},
		{"id": 3, "city": "CHI"},
	})

	// 3. List table data -> verify rows in BQ format
	resp := doRequest(t, ts, http.MethodGet, bqPath("datasets", "ds1", "tables", "t1", "data"), nil)
	result := readJSON(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("listTableData: expected 200, got %d", resp.StatusCode)
	}
	if result["kind"] != "bigquery#tableDataList" {
		t.Fatalf("expected kind=bigquery#tableDataList, got %v", result["kind"])
	}
	if result["totalRows"].(string) != "3" {
		t.Fatalf("expected totalRows=3, got %v", result["totalRows"])
	}

	rows := result["rows"].([]interface{})
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	// Verify BQ row format: {"f": [{"v": "..."}, {"v": "..."}]}
	for i, row := range rows {
		rowMap := row.(map[string]interface{})
		fArr, ok := rowMap["f"].([]interface{})
		if !ok {
			t.Fatalf("row %d missing 'f' array", i)
		}
		if len(fArr) != 2 {
			t.Fatalf("row %d: expected 2 fields, got %d", i, len(fArr))
		}
		// Each field should have a "v" key
		for j, field := range fArr {
			fieldMap := field.(map[string]interface{})
			if _, hasV := fieldMap["v"]; !hasV {
				t.Fatalf("row %d field %d: missing 'v' key", i, j)
			}
		}
	}
}

func TestIntegration_Health(t *testing.T) {
	ts, cleanup := setupIntegrationServer(t)
	defer cleanup()

	resp := doRequest(t, ts, http.MethodGet, "/health", nil)
	result := readJSON(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health: expected 200, got %d", resp.StatusCode)
	}
	if result["status"] != "ok" {
		t.Fatalf("expected status=ok, got %v", result["status"])
	}
}
