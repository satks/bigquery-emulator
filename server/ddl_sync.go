package server

import (
	"context"
	"regexp"
	"strings"
	"time"

	"github.com/sathish/bigquery-emulator/pkg/metadata"
	"github.com/sathish/bigquery-emulator/pkg/query"
	"go.uber.org/zap"
)

// DDL regex patterns to extract schema/table names from SQL.
var (
	createSchemaRe = regexp.MustCompile(`(?i)CREATE\s+SCHEMA\s+(?:IF\s+NOT\s+EXISTS\s+)?` + identPattern)
	dropSchemaRe   = regexp.MustCompile(`(?i)DROP\s+SCHEMA\s+(?:IF\s+EXISTS\s+)?` + identPattern)
	createTableRe  = regexp.MustCompile(`(?i)CREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMP(?:ORARY)?\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?` + twoPartPattern)
	dropTableRe    = regexp.MustCompile(`(?i)DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?` + twoPartPattern)
)

// identPattern matches a single identifier (quoted or unquoted).
const identPattern = `["']?([a-zA-Z_][a-zA-Z0-9_]*)["']?`

// twoPartPattern matches schema.table or just table.
const twoPartPattern = `["']?([a-zA-Z_][a-zA-Z0-9_]*)["']?(?:\.["']?([a-zA-Z_][a-zA-Z0-9_]*)["']?)?`

// syncDDLMetadata detects DDL in the original SQL and syncs metadata.
// This ensures datasets/tables created via SQL are visible through the REST API.
func (s *Server) syncDDLMetadata(ctx context.Context, projectID, originalSQL string) {
	classification := query.ClassifySQL(originalSQL)

	switch classification.Type {
	case query.StatementDDLCreate:
		s.syncCreateDDL(ctx, projectID, originalSQL)
	case query.StatementDDLDrop:
		s.syncDropDDL(ctx, projectID, originalSQL)
	}
}

func (s *Server) syncCreateDDL(ctx context.Context, projectID, sql string) {
	// CREATE SCHEMA
	if m := createSchemaRe.FindStringSubmatch(sql); len(m) >= 2 {
		schemaName := unquote(m[1])
		ds := metadata.Dataset{
			ProjectID:        projectID,
			DatasetID:        schemaName,
			Location:         "US",
			CreationTime:     time.Now(),
			LastModifiedTime: time.Now(),
		}
		if err := s.repo.CreateDataset(ctx, ds); err != nil {
			// Ignore already-exists — the schema was created by DuckDB,
			// we just need the metadata row
			s.logger.Debug("DDL sync: dataset metadata already exists or error",
				zap.String("dataset", schemaName), zap.Error(err))
		} else {
			s.logger.Debug("DDL sync: registered dataset from SQL",
				zap.String("dataset", schemaName))
		}
		return
	}

	// CREATE TABLE
	if m := createTableRe.FindStringSubmatch(sql); len(m) >= 2 {
		var schemaName, tableName string
		if m[2] != "" {
			schemaName = unquote(m[1])
			tableName = unquote(m[2])
		} else {
			// Single identifier — table in default schema
			tableName = unquote(m[1])
			schemaName = "main"
		}

		tbl := metadata.Table{
			ProjectID:        projectID,
			DatasetID:        schemaName,
			TableID:          tableName,
			Type:             "TABLE",
			CreationTime:     time.Now(),
			LastModifiedTime: time.Now(),
		}
		// Don't call repo.CreateTable as the DuckDB table already exists.
		// Just insert the metadata row directly.
		if err := s.repo.RegisterTableMetadata(ctx, tbl); err != nil {
			s.logger.Debug("DDL sync: table metadata already exists or error",
				zap.String("table", schemaName+"."+tableName), zap.Error(err))
		} else {
			s.logger.Debug("DDL sync: registered table from SQL",
				zap.String("table", schemaName+"."+tableName))
		}
		return
	}
}

func (s *Server) syncDropDDL(ctx context.Context, projectID, sql string) {
	// DROP SCHEMA
	if m := dropSchemaRe.FindStringSubmatch(sql); len(m) >= 2 {
		schemaName := unquote(m[1])
		// Remove metadata — the DuckDB schema is already dropped
		_ = s.repo.DeleteDatasetMetadataOnly(ctx, projectID, schemaName)
		s.logger.Debug("DDL sync: removed dataset metadata",
			zap.String("dataset", schemaName))
		return
	}

	// DROP TABLE
	if m := dropTableRe.FindStringSubmatch(sql); len(m) >= 2 {
		var schemaName, tableName string
		if m[2] != "" {
			schemaName = unquote(m[1])
			tableName = unquote(m[2])
		} else {
			tableName = unquote(m[1])
			schemaName = "main"
		}
		_ = s.repo.DeleteTableMetadataOnly(ctx, projectID, schemaName, tableName)
		s.logger.Debug("DDL sync: removed table metadata",
			zap.String("table", schemaName+"."+tableName))
		return
	}
}

// unquote removes surrounding double quotes if present.
func unquote(s string) string {
	return strings.Trim(s, `"'`)
}
