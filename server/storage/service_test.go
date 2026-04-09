package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/go-chi/chi/v5"
	"github.com/sathish/bigquery-emulator/pkg/connection"
	"github.com/sathish/bigquery-emulator/pkg/metadata"
	"github.com/sathish/bigquery-emulator/pkg/query"
	"go.uber.org/zap"
)

// setupStorageTestServer sets up a test environment with DuckDB, metadata repo,
// query executor, storage service, and chi router. It creates a test project,
// dataset, table, and optionally inserts test rows.
func setupStorageTestServer(t *testing.T, insertRows bool) (*Service, chi.Router) {
	t.Helper()

	logger, _ := zap.NewDevelopment()

	connMgr, err := connection.NewManager(":memory:", logger)
	if err != nil {
		t.Fatalf("create connection manager: %v", err)
	}
	t.Cleanup(func() { connMgr.Close() })

	repo, err := metadata.NewRepository(connMgr, logger)
	if err != nil {
		t.Fatalf("create repository: %v", err)
	}

	executor := query.NewExecutor(connMgr, logger)

	ctx := context.Background()

	// Create test project
	if err := repo.CreateProject(ctx, metadata.Project{ID: "test-project"}); err != nil {
		t.Fatalf("create project: %v", err)
	}

	// Create test dataset
	if err := repo.CreateDataset(ctx, metadata.Dataset{
		ProjectID:    "test-project",
		DatasetID:    "test_dataset",
		CreationTime: time.Now(),
	}); err != nil {
		t.Fatalf("create dataset: %v", err)
	}

	// Create test table with schema
	if err := repo.CreateTable(ctx, metadata.Table{
		ProjectID: "test-project",
		DatasetID: "test_dataset",
		TableID:   "test_table",
		Type:      "TABLE",
		Schema: &metadata.TableSchema{
			Fields: []metadata.FieldSchema{
				{Name: "id", Type: "INT64", Mode: "REQUIRED"},
				{Name: "name", Type: "STRING"},
				{Name: "active", Type: "BOOL"},
			},
		},
		CreationTime: time.Now(),
	}); err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Insert test rows if requested
	if insertRows {
		_, err := connMgr.Exec(ctx, `INSERT INTO "test_dataset"."test_table" (id, name, active) VALUES (1, 'alice', true), (2, 'bob', false), (3, 'charlie', true)`)
		if err != nil {
			t.Fatalf("insert test rows: %v", err)
		}
	}

	svc := NewService(connMgr, repo, executor, logger)

	r := chi.NewRouter()
	svc.RegisterRoutes(r)

	return svc, r
}

func TestService_CreateReadSession(t *testing.T) {
	_, router := setupStorageTestServer(t, true)

	reqBody := map[string]interface{}{
		"dataFormat": "ARROW",
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPost,
		"/v1/projects/test-project/datasets/test_dataset/tables/test_table:createReadSession",
		bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var session ReadSession
	if err := json.NewDecoder(w.Body).Decode(&session); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if session.Name == "" {
		t.Error("expected non-empty session name")
	}
	if session.DataFormat != "ARROW" {
		t.Errorf("expected dataFormat ARROW, got %q", session.DataFormat)
	}
	if len(session.Streams) != 1 {
		t.Fatalf("expected 1 stream, got %d", len(session.Streams))
	}
	if session.Streams[0].Name == "" {
		t.Error("expected non-empty stream name")
	}
	if session.Schema == nil {
		t.Fatal("expected schema in session")
	}
	if len(session.Schema.Fields) != 3 {
		t.Errorf("expected 3 schema fields, got %d", len(session.Schema.Fields))
	}
}

func TestService_CreateReadSession_TableNotFound(t *testing.T) {
	_, router := setupStorageTestServer(t, false)

	reqBody := map[string]interface{}{
		"dataFormat": "ARROW",
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPost,
		"/v1/projects/test-project/datasets/test_dataset/tables/nonexistent:createReadSession",
		bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestService_ReadRows_Success(t *testing.T) {
	_, router := setupStorageTestServer(t, true)

	// First, create a read session
	reqBody := map[string]interface{}{
		"dataFormat": "ARROW",
	}
	body, _ := json.Marshal(reqBody)

	createReq := httptest.NewRequest(http.MethodPost,
		"/v1/projects/test-project/datasets/test_dataset/tables/test_table:createReadSession",
		bytes.NewReader(body))
	createReq.Header.Set("Content-Type", "application/json")
	createW := httptest.NewRecorder()

	router.ServeHTTP(createW, createReq)

	if createW.Code != http.StatusOK {
		t.Fatalf("create session: expected 200, got %d: %s", createW.Code, createW.Body.String())
	}

	var session ReadSession
	if err := json.NewDecoder(createW.Body).Decode(&session); err != nil {
		t.Fatalf("decode session: %v", err)
	}

	// Now read rows from the stream
	streamName := session.Streams[0].Name
	readReq := httptest.NewRequest(http.MethodGet,
		"/v1/readStreams/"+streamName+":readRows", nil)
	readW := httptest.NewRecorder()

	router.ServeHTTP(readW, readReq)

	if readW.Code != http.StatusOK {
		t.Fatalf("read rows: expected 200, got %d: %s", readW.Code, readW.Body.String())
	}

	contentType := readW.Header().Get("Content-Type")
	if contentType != "application/vnd.apache.arrow.stream" {
		t.Errorf("expected Content-Type 'application/vnd.apache.arrow.stream', got %q", contentType)
	}

	// Parse Arrow IPC response
	reader, err := ipc.NewReader(bytes.NewReader(readW.Body.Bytes()))
	if err != nil {
		t.Fatalf("create arrow reader: %v", err)
	}
	defer reader.Release()

	schema := reader.Schema()
	if schema.NumFields() != 3 {
		t.Fatalf("expected 3 fields in schema, got %d", schema.NumFields())
	}
	if schema.Field(0).Name != "id" {
		t.Errorf("expected field 0 name 'id', got %q", schema.Field(0).Name)
	}
	if schema.Field(1).Name != "name" {
		t.Errorf("expected field 1 name 'name', got %q", schema.Field(1).Name)
	}
	if schema.Field(2).Name != "active" {
		t.Errorf("expected field 2 name 'active', got %q", schema.Field(2).Name)
	}

	// Read all records
	totalRows := int64(0)
	for reader.Next() {
		rec := reader.Record()
		totalRows += rec.NumRows()
	}
	if totalRows != 3 {
		t.Errorf("expected 3 rows, got %d", totalRows)
	}
}

func TestService_ReadRows_EmptyTable(t *testing.T) {
	_, router := setupStorageTestServer(t, false) // no rows inserted

	// Create read session
	reqBody := map[string]interface{}{"dataFormat": "ARROW"}
	body, _ := json.Marshal(reqBody)
	createReq := httptest.NewRequest(http.MethodPost,
		"/v1/projects/test-project/datasets/test_dataset/tables/test_table:createReadSession",
		bytes.NewReader(body))
	createReq.Header.Set("Content-Type", "application/json")
	createW := httptest.NewRecorder()
	router.ServeHTTP(createW, createReq)

	if createW.Code != http.StatusOK {
		t.Fatalf("create session: expected 200, got %d", createW.Code)
	}

	var session ReadSession
	json.NewDecoder(createW.Body).Decode(&session)

	// Read rows
	streamName := session.Streams[0].Name
	readReq := httptest.NewRequest(http.MethodGet,
		"/v1/readStreams/"+streamName+":readRows", nil)
	readW := httptest.NewRecorder()
	router.ServeHTTP(readW, readReq)

	if readW.Code != http.StatusOK {
		t.Fatalf("read rows: expected 200, got %d: %s", readW.Code, readW.Body.String())
	}

	// Parse Arrow IPC response - should be valid but with 0 rows
	reader, err := ipc.NewReader(bytes.NewReader(readW.Body.Bytes()))
	if err != nil {
		t.Fatalf("create arrow reader: %v", err)
	}
	defer reader.Release()

	totalRows := int64(0)
	for reader.Next() {
		rec := reader.Record()
		totalRows += rec.NumRows()
	}
	if totalRows != 0 {
		t.Errorf("expected 0 rows for empty table, got %d", totalRows)
	}
}

func TestService_ReadRows_InvalidStream(t *testing.T) {
	_, router := setupStorageTestServer(t, false)

	req := httptest.NewRequest(http.MethodGet,
		"/v1/readStreams/nonexistent-stream:readRows", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}

func TestService_CreateWriteStream(t *testing.T) {
	_, router := setupStorageTestServer(t, false)

	reqBody := map[string]interface{}{
		"type": "COMMITTED",
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest(http.MethodPost,
		"/v1/projects/test-project/datasets/test_dataset/tables/test_table:createWriteStream",
		bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var ws WriteStream
	if err := json.NewDecoder(w.Body).Decode(&ws); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if ws.Name == "" {
		t.Error("expected non-empty write stream name")
	}
	if ws.Type != "COMMITTED" {
		t.Errorf("expected type COMMITTED, got %q", ws.Type)
	}
	if ws.Table == "" {
		t.Error("expected non-empty table reference")
	}
}

func TestService_AppendRows_Success(t *testing.T) {
	_, router := setupStorageTestServer(t, false)

	// Create write stream
	wsBody := map[string]interface{}{"type": "COMMITTED"}
	body, _ := json.Marshal(wsBody)
	createReq := httptest.NewRequest(http.MethodPost,
		"/v1/projects/test-project/datasets/test_dataset/tables/test_table:createWriteStream",
		bytes.NewReader(body))
	createReq.Header.Set("Content-Type", "application/json")
	createW := httptest.NewRecorder()
	router.ServeHTTP(createW, createReq)

	if createW.Code != http.StatusOK {
		t.Fatalf("create write stream: expected 200, got %d: %s", createW.Code, createW.Body.String())
	}

	var ws WriteStream
	json.NewDecoder(createW.Body).Decode(&ws)

	// Append rows
	appendBody := AppendRowsRequest{
		Rows: []map[string]interface{}{
			{"id": 1, "name": "alice", "active": true},
			{"id": 2, "name": "bob", "active": false},
		},
	}
	appendBytes, _ := json.Marshal(appendBody)
	appendReq := httptest.NewRequest(http.MethodPost,
		"/v1/writeStreams/"+ws.Name+":appendRows",
		bytes.NewReader(appendBytes))
	appendReq.Header.Set("Content-Type", "application/json")
	appendW := httptest.NewRecorder()
	router.ServeHTTP(appendW, appendReq)

	if appendW.Code != http.StatusOK {
		t.Fatalf("append rows: expected 200, got %d: %s", appendW.Code, appendW.Body.String())
	}

	// Verify rows were inserted via a read session
	rsBody := map[string]interface{}{"dataFormat": "ARROW"}
	rsBytes, _ := json.Marshal(rsBody)
	rsReq := httptest.NewRequest(http.MethodPost,
		"/v1/projects/test-project/datasets/test_dataset/tables/test_table:createReadSession",
		bytes.NewReader(rsBytes))
	rsReq.Header.Set("Content-Type", "application/json")
	rsW := httptest.NewRecorder()
	router.ServeHTTP(rsW, rsReq)

	var session ReadSession
	json.NewDecoder(rsW.Body).Decode(&session)

	// Read and verify row count
	readReq := httptest.NewRequest(http.MethodGet,
		"/v1/readStreams/"+session.Streams[0].Name+":readRows", nil)
	readW := httptest.NewRecorder()
	router.ServeHTTP(readW, readReq)

	reader, err := ipc.NewReader(bytes.NewReader(readW.Body.Bytes()))
	if err != nil {
		t.Fatalf("create arrow reader: %v", err)
	}
	defer reader.Release()

	totalRows := int64(0)
	for reader.Next() {
		totalRows += reader.Record().NumRows()
	}
	if totalRows != 2 {
		t.Errorf("expected 2 rows after append, got %d", totalRows)
	}
}

func TestService_AppendRows_InvalidStream(t *testing.T) {
	_, router := setupStorageTestServer(t, false)

	appendBody := AppendRowsRequest{
		Rows: []map[string]interface{}{
			{"id": 1, "name": "alice", "active": true},
		},
	}
	body, _ := json.Marshal(appendBody)

	req := httptest.NewRequest(http.MethodPost,
		"/v1/writeStreams/nonexistent-stream:appendRows",
		bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", w.Code, w.Body.String())
	}
}
