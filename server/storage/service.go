package storage

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/sathish/bigquery-emulator/pkg/connection"
	"github.com/sathish/bigquery-emulator/pkg/metadata"
	"github.com/sathish/bigquery-emulator/pkg/query"
	"github.com/sathish/bigquery-emulator/pkg/types"
	"go.uber.org/zap"
)

// ReadSession represents a BigQuery Storage read session.
type ReadSession struct {
	Name       string             `json:"name"`
	Table      string             `json:"table"`
	DataFormat string             `json:"dataFormat"`
	Streams    []ReadStream       `json:"streams"`
	Schema     *types.TableSchema `json:"schema,omitempty"`
	RowCount   int64              `json:"estimatedRowCount"`
}

// ReadStream represents a single stream within a read session.
type ReadStream struct {
	Name string `json:"name"`
}

// WriteStream represents a write stream for appending data.
type WriteStream struct {
	Name  string `json:"name"`
	Table string `json:"table"`
	Type  string `json:"type"` // COMMITTED, PENDING
}

// AppendRowsRequest represents the request to append rows.
type AppendRowsRequest struct {
	Rows []map[string]interface{} `json:"rows"`
}

// readSessionInfo stores internal info for a read session, used to map
// stream names back to the table they should read from.
type readSessionInfo struct {
	session   ReadSession
	projectID string
	datasetID string
	tableID   string
}

// writeStreamInfo stores internal info for a write stream.
type writeStreamInfo struct {
	stream    WriteStream
	projectID string
	datasetID string
	tableID   string
}

// ColumnMeta mirrors query.ColumnMeta for use in BuildArrowRecord.
// Re-exported here so the storage package can construct it from query results.
type ColumnMeta = types.ColumnMeta

// Service implements the BigQuery Storage API endpoints.
type Service struct {
	connMgr  *connection.Manager
	repo     *metadata.Repository
	executor *query.Executor
	logger   *zap.Logger

	readSessions map[string]*readSessionInfo
	readMu       sync.RWMutex

	writeStreams map[string]*writeStreamInfo
	writeMu      sync.RWMutex
}

// NewService creates a new storage API service.
func NewService(connMgr *connection.Manager, repo *metadata.Repository, exec *query.Executor, logger *zap.Logger) *Service {
	return &Service{
		connMgr:      connMgr,
		repo:         repo,
		executor:     exec,
		logger:       logger,
		readSessions: make(map[string]*readSessionInfo),
		writeStreams:  make(map[string]*writeStreamInfo),
	}
}

// RegisterRoutes adds storage API routes to a chi router.
func (s *Service) RegisterRoutes(r chi.Router) {
	// Read API
	r.Post("/v1/projects/{projectId}/datasets/{datasetId}/tables/{tableId}:createReadSession", s.CreateReadSession)
	r.Get("/v1/readStreams/{streamName}:readRows", s.ReadRows)

	// Write API
	r.Post("/v1/projects/{projectId}/datasets/{datasetId}/tables/{tableId}:createWriteStream", s.CreateWriteStream)
	r.Post("/v1/writeStreams/{streamName}:appendRows", s.AppendRows)
}

// CreateReadSession creates a new read session for a table.
func (s *Service) CreateReadSession(w http.ResponseWriter, r *http.Request) {
	projectID := chi.URLParam(r, "projectId")
	datasetID := chi.URLParam(r, "datasetId")
	tableID := chi.URLParam(r, "tableId")

	ctx := r.Context()

	// Look up the table in metadata
	tbl, err := s.repo.GetTable(ctx, projectID, datasetID, tableID)
	if err != nil {
		if isNotFound(err) {
			writeErrorJSON(w, http.StatusNotFound, fmt.Sprintf("Table %s.%s.%s not found", projectID, datasetID, tableID))
			return
		}
		writeErrorJSON(w, http.StatusInternalServerError, fmt.Sprintf("lookup table: %v", err))
		return
	}

	// Convert metadata schema to types.TableSchema
	var schema *types.TableSchema
	if tbl.Schema != nil && len(tbl.Schema.Fields) > 0 {
		ts := convertMetadataSchema(tbl.Schema)
		schema = &ts
	}

	// Get row count estimate
	rowCount := int64(0)
	countResult, err := s.executor.Query(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM "%s"."%s"`, datasetID, tableID))
	if err == nil && len(countResult.Rows) > 0 && len(countResult.Rows[0]) > 0 {
		if v, ok := countResult.Rows[0][0].(int64); ok {
			rowCount = v
		}
	}

	// Generate session and stream IDs
	sessionID := uuid.New().String()
	streamID := uuid.New().String()
	sessionName := fmt.Sprintf("projects/%s/datasets/%s/tables/%s/sessions/%s", projectID, datasetID, tableID, sessionID)
	streamName := fmt.Sprintf("stream_%s", streamID)

	session := ReadSession{
		Name:       sessionName,
		Table:      fmt.Sprintf("projects/%s/datasets/%s/tables/%s", projectID, datasetID, tableID),
		DataFormat: "ARROW",
		Streams:    []ReadStream{{Name: streamName}},
		Schema:     schema,
		RowCount:   rowCount,
	}

	// Store session info so ReadRows can find it by stream name
	info := &readSessionInfo{
		session:   session,
		projectID: projectID,
		datasetID: datasetID,
		tableID:   tableID,
	}

	s.readMu.Lock()
	s.readSessions[streamName] = info
	s.readMu.Unlock()

	writeJSON(w, http.StatusOK, session)
}

// ReadRows reads Arrow-encoded rows from a stream.
func (s *Service) ReadRows(w http.ResponseWriter, r *http.Request) {
	streamName := chi.URLParam(r, "streamName")

	s.readMu.RLock()
	info, ok := s.readSessions[streamName]
	s.readMu.RUnlock()

	if !ok {
		writeErrorJSON(w, http.StatusNotFound, fmt.Sprintf("Stream %q not found", streamName))
		return
	}

	ctx := r.Context()

	// Execute SELECT * on the table
	selectSQL := fmt.Sprintf(`SELECT * FROM "%s"."%s"`, info.datasetID, info.tableID)
	result, err := s.executor.Query(ctx, selectSQL)
	if err != nil {
		writeErrorJSON(w, http.StatusInternalServerError, fmt.Sprintf("query failed: %v", err))
		return
	}

	// Build Arrow record batch from query results
	alloc := memory.NewGoAllocator()

	colMeta := make([]ColumnMeta, len(result.Schema))
	for i, c := range result.Schema {
		colMeta[i] = ColumnMeta{Name: c.Name, Type: c.Type}
	}

	record, err := types.BuildArrowRecord(alloc, colMeta, result.Rows)
	if err != nil {
		writeErrorJSON(w, http.StatusInternalServerError, fmt.Sprintf("build arrow record: %v", err))
		return
	}
	defer record.Release()

	// Serialize to Arrow IPC format
	w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
	w.WriteHeader(http.StatusOK)

	writer := ipc.NewWriter(w, ipc.WithSchema(record.Schema()))
	if len(result.Rows) > 0 {
		if err := writer.Write(record); err != nil {
			s.logger.Error("write arrow record", zap.Error(err))
			return
		}
	}
	if err := writer.Close(); err != nil {
		s.logger.Error("close arrow writer", zap.Error(err))
	}
}

// CreateWriteStream creates a new write stream for a table.
func (s *Service) CreateWriteStream(w http.ResponseWriter, r *http.Request) {
	projectID := chi.URLParam(r, "projectId")
	datasetID := chi.URLParam(r, "datasetId")
	tableID := chi.URLParam(r, "tableId")

	ctx := r.Context()

	// Verify table exists
	_, err := s.repo.GetTable(ctx, projectID, datasetID, tableID)
	if err != nil {
		if isNotFound(err) {
			writeErrorJSON(w, http.StatusNotFound, fmt.Sprintf("Table %s.%s.%s not found", projectID, datasetID, tableID))
			return
		}
		writeErrorJSON(w, http.StatusInternalServerError, fmt.Sprintf("lookup table: %v", err))
		return
	}

	// Parse request body for stream type
	var reqBody struct {
		Type string `json:"type"`
	}
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		writeErrorJSON(w, http.StatusBadRequest, fmt.Sprintf("invalid request body: %v", err))
		return
	}
	if reqBody.Type == "" {
		reqBody.Type = "COMMITTED"
	}

	streamID := uuid.New().String()
	streamName := fmt.Sprintf("writestream_%s", streamID)

	ws := WriteStream{
		Name:  streamName,
		Table: fmt.Sprintf("projects/%s/datasets/%s/tables/%s", projectID, datasetID, tableID),
		Type:  reqBody.Type,
	}

	info := &writeStreamInfo{
		stream:    ws,
		projectID: projectID,
		datasetID: datasetID,
		tableID:   tableID,
	}

	s.writeMu.Lock()
	s.writeStreams[streamName] = info
	s.writeMu.Unlock()

	writeJSON(w, http.StatusOK, ws)
}

// AppendRows appends rows to a write stream.
func (s *Service) AppendRows(w http.ResponseWriter, r *http.Request) {
	streamName := chi.URLParam(r, "streamName")

	s.writeMu.RLock()
	info, ok := s.writeStreams[streamName]
	s.writeMu.RUnlock()

	if !ok {
		writeErrorJSON(w, http.StatusNotFound, fmt.Sprintf("Write stream %q not found", streamName))
		return
	}

	var req AppendRowsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErrorJSON(w, http.StatusBadRequest, fmt.Sprintf("invalid request body: %v", err))
		return
	}

	if len(req.Rows) == 0 {
		writeJSON(w, http.StatusOK, map[string]interface{}{"appendResult": map[string]interface{}{"rowCount": 0}})
		return
	}

	ctx := r.Context()

	// Get table schema to know column order
	tbl, err := s.repo.GetTable(ctx, info.projectID, info.datasetID, info.tableID)
	if err != nil {
		writeErrorJSON(w, http.StatusInternalServerError, fmt.Sprintf("lookup table: %v", err))
		return
	}

	// Build column names from schema
	var colNames []string
	if tbl.Schema != nil {
		for _, f := range tbl.Schema.Fields {
			colNames = append(colNames, f.Name)
		}
	}

	// If no schema available, derive columns from the first row
	if len(colNames) == 0 {
		for k := range req.Rows[0] {
			colNames = append(colNames, k)
		}
		sort.Strings(colNames) // deterministic order
	}

	// Build INSERT statements for each row
	for _, row := range req.Rows {
		placeholders := make([]string, len(colNames))
		values := make([]interface{}, len(colNames))
		for i, col := range colNames {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
			values[i] = row[col]
		}

		quotedCols := make([]string, len(colNames))
		for i, c := range colNames {
			quotedCols[i] = fmt.Sprintf(`"%s"`, c)
		}

		insertSQL := fmt.Sprintf(`INSERT INTO "%s"."%s" (%s) VALUES (%s)`,
			info.datasetID, info.tableID,
			strings.Join(quotedCols, ", "),
			strings.Join(placeholders, ", "),
		)

		if _, err := s.connMgr.Exec(ctx, insertSQL, values...); err != nil {
			writeErrorJSON(w, http.StatusInternalServerError, fmt.Sprintf("insert row: %v", err))
			return
		}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"appendResult": map[string]interface{}{
			"rowCount": len(req.Rows),
		},
	})
}

// convertMetadataSchema converts metadata.TableSchema to types.TableSchema.
func convertMetadataSchema(schema *metadata.TableSchema) types.TableSchema {
	return types.TableSchema{
		Fields: convertMetadataFields(schema.Fields),
	}
}

// convertMetadataFields recursively converts metadata.FieldSchema to types.FieldSchema.
func convertMetadataFields(fields []metadata.FieldSchema) []types.FieldSchema {
	result := make([]types.FieldSchema, len(fields))
	for i, f := range fields {
		result[i] = types.FieldSchema{
			Name:        f.Name,
			Type:        f.Type,
			Mode:        f.Mode,
			Description: f.Description,
			Fields:      convertMetadataFields(f.Fields),
		}
	}
	return result
}

// isNotFound checks if an error indicates a resource was not found.
func isNotFound(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "not found")
}

// writeJSON writes a JSON response with the given status code.
func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeErrorJSON writes a JSON error response.
func writeErrorJSON(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]interface{}{
		"error": map[string]interface{}{
			"code":    status,
			"message": message,
		},
	})
}
