package metadata

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/sathish/bigquery-emulator/pkg/connection"
	"github.com/sathish/bigquery-emulator/pkg/types"
	"go.uber.org/zap"
)

// Repository stores metadata (projects, datasets, tables, jobs) in DuckDB
// _bq_metadata tables and manages the actual DuckDB schemas/tables.
type Repository struct {
	connMgr    *connection.Manager
	typeMapper *types.TypeMapper
	logger     *zap.Logger
}

// NewRepository creates a new metadata repository and initializes the
// metadata tables if they don't already exist.
func NewRepository(connMgr *connection.Manager, logger *zap.Logger) (*Repository, error) {
	r := &Repository{
		connMgr:    connMgr,
		typeMapper: types.NewTypeMapper(),
		logger:     logger,
	}

	if err := r.initTables(); err != nil {
		return nil, fmt.Errorf("init metadata tables: %w", err)
	}

	logger.Info("metadata repository initialized")
	return r, nil
}

// initTables creates the metadata tables if they don't exist.
func (r *Repository) initTables() error {
	ctx := context.Background()

	ddls := []string{
		`CREATE TABLE IF NOT EXISTS _bq_projects (
			id TEXT PRIMARY KEY,
			friendly_name TEXT,
			numeric_id BIGINT
		)`,
		`CREATE TABLE IF NOT EXISTS _bq_datasets (
			project_id TEXT,
			dataset_id TEXT,
			metadata JSON,
			PRIMARY KEY (project_id, dataset_id)
		)`,
		`CREATE TABLE IF NOT EXISTS _bq_tables (
			project_id TEXT,
			dataset_id TEXT,
			table_id TEXT,
			metadata JSON,
			PRIMARY KEY (project_id, dataset_id, table_id)
		)`,
		`CREATE TABLE IF NOT EXISTS _bq_jobs (
			project_id TEXT,
			job_id TEXT,
			metadata JSON,
			PRIMARY KEY (project_id, job_id)
		)`,
	}

	return r.connMgr.ExecTx(ctx, func(tx *sql.Tx) error {
		for _, ddl := range ddls {
			if _, err := tx.ExecContext(ctx, ddl); err != nil {
				return fmt.Errorf("exec %q: %w", ddl[:40], err)
			}
		}
		return nil
	})
}

// --- Projects ---

// CreateProject inserts a new project into the metadata store.
func (r *Repository) CreateProject(ctx context.Context, project Project) error {
	_, err := r.connMgr.Exec(ctx,
		`INSERT INTO _bq_projects (id, friendly_name, numeric_id) VALUES ($1, $2, $3)`,
		project.ID, project.FriendlyName, project.NumericID,
	)
	if err != nil {
		return fmt.Errorf("create project %q: %w", project.ID, err)
	}
	r.logger.Debug("created project", zap.String("id", project.ID))
	return nil
}

// GetProject retrieves a project by ID.
func (r *Repository) GetProject(ctx context.Context, projectID string) (*Project, error) {
	rows, err := r.connMgr.Query(ctx,
		`SELECT id, friendly_name, numeric_id FROM _bq_projects WHERE id = $1`,
		projectID,
	)
	if err != nil {
		return nil, fmt.Errorf("get project %q: %w", projectID, err)
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, fmt.Errorf("project %q not found", projectID)
	}

	var p Project
	var friendlyName sql.NullString
	var numericID sql.NullInt64
	if err := rows.Scan(&p.ID, &friendlyName, &numericID); err != nil {
		return nil, fmt.Errorf("scan project: %w", err)
	}
	if friendlyName.Valid {
		p.FriendlyName = friendlyName.String
	}
	if numericID.Valid {
		p.NumericID = numericID.Int64
	}
	return &p, nil
}

// ListProjects returns all projects.
func (r *Repository) ListProjects(ctx context.Context) ([]Project, error) {
	rows, err := r.connMgr.Query(ctx,
		`SELECT id, friendly_name, numeric_id FROM _bq_projects ORDER BY id`,
	)
	if err != nil {
		return nil, fmt.Errorf("list projects: %w", err)
	}
	defer rows.Close()

	var projects []Project
	for rows.Next() {
		var p Project
		var friendlyName sql.NullString
		var numericID sql.NullInt64
		if err := rows.Scan(&p.ID, &friendlyName, &numericID); err != nil {
			return nil, fmt.Errorf("scan project: %w", err)
		}
		if friendlyName.Valid {
			p.FriendlyName = friendlyName.String
		}
		if numericID.Valid {
			p.NumericID = numericID.Int64
		}
		projects = append(projects, p)
	}
	if projects == nil {
		projects = []Project{}
	}
	return projects, rows.Err()
}

// --- Datasets ---

// CreateDataset inserts a new dataset into the metadata store and creates
// the corresponding DuckDB schema.
func (r *Repository) CreateDataset(ctx context.Context, ds Dataset) error {
	data, err := json.Marshal(ds)
	if err != nil {
		return fmt.Errorf("marshal dataset: %w", err)
	}

	_, err = r.connMgr.Exec(ctx,
		`INSERT INTO _bq_datasets (project_id, dataset_id, metadata) VALUES ($1, $2, $3)`,
		ds.ProjectID, ds.DatasetID, string(data),
	)
	if err != nil {
		return fmt.Errorf("create dataset %q: %w", ds.DatasetID, err)
	}

	// Create DuckDB schema
	_, err = r.connMgr.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS "%s"`, ds.DatasetID))
	if err != nil {
		return fmt.Errorf("create schema %q: %w", ds.DatasetID, err)
	}

	r.logger.Debug("created dataset", zap.String("project", ds.ProjectID), zap.String("dataset", ds.DatasetID))
	return nil
}

// GetDataset retrieves a dataset by project and dataset ID.
func (r *Repository) GetDataset(ctx context.Context, projectID, datasetID string) (*Dataset, error) {
	rows, err := r.connMgr.Query(ctx,
		`SELECT CAST(metadata AS TEXT) FROM _bq_datasets WHERE project_id = $1 AND dataset_id = $2`,
		projectID, datasetID,
	)
	if err != nil {
		return nil, fmt.Errorf("get dataset %q.%q: %w", projectID, datasetID, err)
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, fmt.Errorf("dataset %q.%q not found", projectID, datasetID)
	}

	var data string
	if err := rows.Scan(&data); err != nil {
		return nil, fmt.Errorf("scan dataset: %w", err)
	}

	var ds Dataset
	if err := json.Unmarshal([]byte(data), &ds); err != nil {
		return nil, fmt.Errorf("unmarshal dataset: %w", err)
	}
	return &ds, nil
}

// ListDatasets returns all datasets for a project.
func (r *Repository) ListDatasets(ctx context.Context, projectID string) ([]Dataset, error) {
	rows, err := r.connMgr.Query(ctx,
		`SELECT CAST(metadata AS TEXT) FROM _bq_datasets WHERE project_id = $1 ORDER BY dataset_id`,
		projectID,
	)
	if err != nil {
		return nil, fmt.Errorf("list datasets: %w", err)
	}
	defer rows.Close()

	var datasets []Dataset
	for rows.Next() {
		var data string
		if err := rows.Scan(&data); err != nil {
			return nil, fmt.Errorf("scan dataset: %w", err)
		}
		var ds Dataset
		if err := json.Unmarshal([]byte(data), &ds); err != nil {
			return nil, fmt.Errorf("unmarshal dataset: %w", err)
		}
		datasets = append(datasets, ds)
	}
	if datasets == nil {
		datasets = []Dataset{}
	}
	return datasets, rows.Err()
}

// UpdateDataset updates an existing dataset's metadata.
func (r *Repository) UpdateDataset(ctx context.Context, ds Dataset) error {
	data, err := json.Marshal(ds)
	if err != nil {
		return fmt.Errorf("marshal dataset: %w", err)
	}

	result, err := r.connMgr.Exec(ctx,
		`UPDATE _bq_datasets SET metadata = $1 WHERE project_id = $2 AND dataset_id = $3`,
		string(data), ds.ProjectID, ds.DatasetID,
	)
	if err != nil {
		return fmt.Errorf("update dataset %q: %w", ds.DatasetID, err)
	}

	affected, _ := result.RowsAffected()
	if affected == 0 {
		return fmt.Errorf("dataset %q.%q not found", ds.ProjectID, ds.DatasetID)
	}
	return nil
}

// DeleteDataset removes a dataset from metadata and optionally drops the DuckDB schema.
func (r *Repository) DeleteDataset(ctx context.Context, projectID, datasetID string, deleteContents bool) error {
	// Delete from metadata
	_, err := r.connMgr.Exec(ctx,
		`DELETE FROM _bq_datasets WHERE project_id = $1 AND dataset_id = $2`,
		projectID, datasetID,
	)
	if err != nil {
		return fmt.Errorf("delete dataset metadata %q: %w", datasetID, err)
	}

	// Also remove all tables metadata for this dataset
	_, err = r.connMgr.Exec(ctx,
		`DELETE FROM _bq_tables WHERE project_id = $1 AND dataset_id = $2`,
		projectID, datasetID,
	)
	if err != nil {
		return fmt.Errorf("delete dataset tables metadata %q: %w", datasetID, err)
	}

	// Drop DuckDB schema
	if deleteContents {
		_, err = r.connMgr.Exec(ctx, fmt.Sprintf(`DROP SCHEMA IF EXISTS "%s" CASCADE`, datasetID))
	} else {
		_, err = r.connMgr.Exec(ctx, fmt.Sprintf(`DROP SCHEMA IF EXISTS "%s"`, datasetID))
	}
	if err != nil {
		return fmt.Errorf("drop schema %q: %w", datasetID, err)
	}

	r.logger.Debug("deleted dataset", zap.String("project", projectID), zap.String("dataset", datasetID))
	return nil
}

// --- Tables ---

// CreateTable inserts a new table into the metadata store and creates the
// corresponding DuckDB table using the TypeMapper for column definitions.
func (r *Repository) CreateTable(ctx context.Context, tbl Table) error {
	data, err := json.Marshal(tbl)
	if err != nil {
		return fmt.Errorf("marshal table: %w", err)
	}

	_, err = r.connMgr.Exec(ctx,
		`INSERT INTO _bq_tables (project_id, dataset_id, table_id, metadata) VALUES ($1, $2, $3, $4)`,
		tbl.ProjectID, tbl.DatasetID, tbl.TableID, string(data),
	)
	if err != nil {
		return fmt.Errorf("create table metadata %q: %w", tbl.TableID, err)
	}

	// Create actual DuckDB table if schema is provided
	if tbl.Schema != nil && len(tbl.Schema.Fields) > 0 {
		// Convert metadata FieldSchema to types.FieldSchema for TypeMapper
		typesSchema := r.convertToTypesSchema(tbl.Schema)
		cols := r.typeMapper.SchemaToDuckDBColumns(typesSchema)
		createSQL := fmt.Sprintf(`CREATE TABLE "%s"."%s" (%s)`, tbl.DatasetID, tbl.TableID, strings.Join(cols, ", "))
		if _, err := r.connMgr.Exec(ctx, createSQL); err != nil {
			return fmt.Errorf("create duckdb table %q.%q: %w", tbl.DatasetID, tbl.TableID, err)
		}
	}

	r.logger.Debug("created table",
		zap.String("project", tbl.ProjectID),
		zap.String("dataset", tbl.DatasetID),
		zap.String("table", tbl.TableID),
	)
	return nil
}

// convertToTypesSchema converts metadata FieldSchema to types.FieldSchema.
func (r *Repository) convertToTypesSchema(schema *TableSchema) types.TableSchema {
	return types.TableSchema{
		Fields: convertFields(schema.Fields),
	}
}

// convertFields recursively converts metadata FieldSchema slice to types.FieldSchema slice.
func convertFields(fields []FieldSchema) []types.FieldSchema {
	result := make([]types.FieldSchema, len(fields))
	for i, f := range fields {
		result[i] = types.FieldSchema{
			Name:        f.Name,
			Type:        f.Type,
			Mode:        f.Mode,
			Description: f.Description,
			Fields:      convertFields(f.Fields),
		}
	}
	return result
}

// GetTable retrieves a table by project, dataset, and table ID.
func (r *Repository) GetTable(ctx context.Context, projectID, datasetID, tableID string) (*Table, error) {
	rows, err := r.connMgr.Query(ctx,
		`SELECT CAST(metadata AS TEXT) FROM _bq_tables WHERE project_id = $1 AND dataset_id = $2 AND table_id = $3`,
		projectID, datasetID, tableID,
	)
	if err != nil {
		return nil, fmt.Errorf("get table %q.%q.%q: %w", projectID, datasetID, tableID, err)
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, fmt.Errorf("table %q.%q.%q not found", projectID, datasetID, tableID)
	}

	var data string
	if err := rows.Scan(&data); err != nil {
		return nil, fmt.Errorf("scan table: %w", err)
	}

	var tbl Table
	if err := json.Unmarshal([]byte(data), &tbl); err != nil {
		return nil, fmt.Errorf("unmarshal table: %w", err)
	}
	return &tbl, nil
}

// ListTables returns all tables for a dataset.
func (r *Repository) ListTables(ctx context.Context, projectID, datasetID string) ([]Table, error) {
	rows, err := r.connMgr.Query(ctx,
		`SELECT CAST(metadata AS TEXT) FROM _bq_tables WHERE project_id = $1 AND dataset_id = $2 ORDER BY table_id`,
		projectID, datasetID,
	)
	if err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}
	defer rows.Close()

	var tables []Table
	for rows.Next() {
		var data string
		if err := rows.Scan(&data); err != nil {
			return nil, fmt.Errorf("scan table: %w", err)
		}
		var tbl Table
		if err := json.Unmarshal([]byte(data), &tbl); err != nil {
			return nil, fmt.Errorf("unmarshal table: %w", err)
		}
		tables = append(tables, tbl)
	}
	if tables == nil {
		tables = []Table{}
	}
	return tables, rows.Err()
}

// UpdateTable updates an existing table's metadata.
func (r *Repository) UpdateTable(ctx context.Context, tbl Table) error {
	data, err := json.Marshal(tbl)
	if err != nil {
		return fmt.Errorf("marshal table: %w", err)
	}

	result, err := r.connMgr.Exec(ctx,
		`UPDATE _bq_tables SET metadata = $1 WHERE project_id = $2 AND dataset_id = $3 AND table_id = $4`,
		string(data), tbl.ProjectID, tbl.DatasetID, tbl.TableID,
	)
	if err != nil {
		return fmt.Errorf("update table %q: %w", tbl.TableID, err)
	}

	affected, _ := result.RowsAffected()
	if affected == 0 {
		return fmt.Errorf("table %q.%q.%q not found", tbl.ProjectID, tbl.DatasetID, tbl.TableID)
	}
	return nil
}

// DeleteTable removes a table from metadata and drops the actual DuckDB table.
func (r *Repository) DeleteTable(ctx context.Context, projectID, datasetID, tableID string) error {
	_, err := r.connMgr.Exec(ctx,
		`DELETE FROM _bq_tables WHERE project_id = $1 AND dataset_id = $2 AND table_id = $3`,
		projectID, datasetID, tableID,
	)
	if err != nil {
		return fmt.Errorf("delete table metadata %q: %w", tableID, err)
	}

	// Drop actual DuckDB table
	_, err = r.connMgr.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS "%s"."%s"`, datasetID, tableID))
	if err != nil {
		return fmt.Errorf("drop table %q.%q: %w", datasetID, tableID, err)
	}

	r.logger.Debug("deleted table",
		zap.String("project", projectID),
		zap.String("dataset", datasetID),
		zap.String("table", tableID),
	)
	return nil
}

// --- Jobs ---

// CreateJob inserts a new job into the metadata store.
func (r *Repository) CreateJob(ctx context.Context, job Job) error {
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}

	_, err = r.connMgr.Exec(ctx,
		`INSERT INTO _bq_jobs (project_id, job_id, metadata) VALUES ($1, $2, $3)`,
		job.ProjectID, job.JobID, string(data),
	)
	if err != nil {
		return fmt.Errorf("create job %q: %w", job.JobID, err)
	}

	r.logger.Debug("created job", zap.String("project", job.ProjectID), zap.String("job", job.JobID))
	return nil
}

// GetJob retrieves a job by project and job ID.
func (r *Repository) GetJob(ctx context.Context, projectID, jobID string) (*Job, error) {
	rows, err := r.connMgr.Query(ctx,
		`SELECT CAST(metadata AS TEXT) FROM _bq_jobs WHERE project_id = $1 AND job_id = $2`,
		projectID, jobID,
	)
	if err != nil {
		return nil, fmt.Errorf("get job %q.%q: %w", projectID, jobID, err)
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, fmt.Errorf("job %q.%q not found", projectID, jobID)
	}

	var data string
	if err := rows.Scan(&data); err != nil {
		return nil, fmt.Errorf("scan job: %w", err)
	}

	var job Job
	if err := json.Unmarshal([]byte(data), &job); err != nil {
		return nil, fmt.Errorf("unmarshal job: %w", err)
	}
	return &job, nil
}

// ListJobs returns all jobs for a project.
func (r *Repository) ListJobs(ctx context.Context, projectID string) ([]Job, error) {
	rows, err := r.connMgr.Query(ctx,
		`SELECT CAST(metadata AS TEXT) FROM _bq_jobs WHERE project_id = $1 ORDER BY job_id`,
		projectID,
	)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()

	var jobs []Job
	for rows.Next() {
		var data string
		if err := rows.Scan(&data); err != nil {
			return nil, fmt.Errorf("scan job: %w", err)
		}
		var job Job
		if err := json.Unmarshal([]byte(data), &job); err != nil {
			return nil, fmt.Errorf("unmarshal job: %w", err)
		}
		jobs = append(jobs, job)
	}
	if jobs == nil {
		jobs = []Job{}
	}
	return jobs, rows.Err()
}

// UpdateJob updates an existing job's metadata.
func (r *Repository) UpdateJob(ctx context.Context, job Job) error {
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}

	result, err := r.connMgr.Exec(ctx,
		`UPDATE _bq_jobs SET metadata = $1 WHERE project_id = $2 AND job_id = $3`,
		string(data), job.ProjectID, job.JobID,
	)
	if err != nil {
		return fmt.Errorf("update job %q: %w", job.JobID, err)
	}

	affected, _ := result.RowsAffected()
	if affected == 0 {
		return fmt.Errorf("job %q.%q not found", job.ProjectID, job.JobID)
	}
	return nil
}
