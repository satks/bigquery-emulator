package metadata

import (
	"fmt"
	"time"
)

// Project represents a BigQuery project.
type Project struct {
	ID           string `json:"id"`
	FriendlyName string `json:"friendlyName,omitempty"`
	NumericID    int64  `json:"numericId,omitempty"`
}

// Dataset represents a BigQuery dataset.
type Dataset struct {
	ProjectID              string            `json:"projectId"`
	DatasetID              string            `json:"datasetId"`
	FriendlyName           string            `json:"friendlyName,omitempty"`
	Description            string            `json:"description,omitempty"`
	Location               string            `json:"location,omitempty"`
	DefaultTableExpiration int64             `json:"defaultTableExpirationMs,omitempty"` // milliseconds
	Labels                 map[string]string `json:"labels,omitempty"`
	CreationTime           time.Time         `json:"creationTime"`
	LastModifiedTime       time.Time         `json:"lastModifiedTime"`
	Access                 []AccessEntry     `json:"access,omitempty"`
}

// AccessEntry represents a dataset-level access control entry (legacy ACL).
type AccessEntry struct {
	Role         string            `json:"role"`                   // OWNER, WRITER, READER
	UserByEmail  string            `json:"userByEmail,omitempty"`
	GroupByEmail string            `json:"groupByEmail,omitempty"`
	Domain       string            `json:"domain,omitempty"`
	SpecialGroup string            `json:"specialGroup,omitempty"` // projectOwners, projectWriters, projectReaders, allAuthenticatedUsers
	View         *TableReference   `json:"view,omitempty"`         // Authorized view
	Dataset      *DatasetReference `json:"dataset,omitempty"`      // Authorized dataset
	Routine      *RoutineReference `json:"routine,omitempty"`      // Authorized routine
	IAMMember    string            `json:"iamMember,omitempty"`
}

// TableReference identifies a table.
type TableReference struct {
	ProjectID string `json:"projectId"`
	DatasetID string `json:"datasetId"`
	TableID   string `json:"tableId"`
}

// DatasetReference identifies a dataset.
type DatasetReference struct {
	ProjectID string `json:"projectId"`
	DatasetID string `json:"datasetId"`
}

// RoutineReference identifies a routine.
type RoutineReference struct {
	ProjectID string `json:"projectId"`
	DatasetID string `json:"datasetId"`
	RoutineID string `json:"routineId"`
}

// Table represents a BigQuery table.
type Table struct {
	ProjectID        string            `json:"projectId"`
	DatasetID        string            `json:"datasetId"`
	TableID          string            `json:"tableId"`
	Type             string            `json:"type"`                    // TABLE, VIEW, MATERIALIZED_VIEW, EXTERNAL, SNAPSHOT
	Description      string            `json:"description,omitempty"`
	Schema           *TableSchema      `json:"schema,omitempty"`
	Labels           map[string]string `json:"labels,omitempty"`
	CreationTime     time.Time         `json:"creationTime"`
	ExpirationTime   *time.Time        `json:"expirationTime,omitempty"`
	LastModifiedTime time.Time         `json:"lastModifiedTime"`
	NumBytes         int64             `json:"numBytes"`
	NumRows          uint64            `json:"numRows"`
	ViewQuery        string            `json:"viewQuery,omitempty"` // For VIEW type
}

// TableSchema represents the schema of a BigQuery table.
type TableSchema struct {
	Fields []FieldSchema `json:"fields"`
}

// FieldSchema represents a single field in a table schema.
type FieldSchema struct {
	Name        string        `json:"name"`
	Type        string        `json:"type"`
	Mode        string        `json:"mode,omitempty"`
	Description string        `json:"description,omitempty"`
	Fields      []FieldSchema `json:"fields,omitempty"`
}

// Job represents a BigQuery job.
type Job struct {
	ProjectID    string        `json:"projectId"`
	JobID        string        `json:"jobId"`
	Location     string        `json:"location,omitempty"`
	Config       JobConfig     `json:"configuration"`
	Status       JobStatus     `json:"status"`
	Statistics   JobStatistics `json:"statistics,omitempty"`
	CreationTime time.Time     `json:"creationTime"`
	StartTime    *time.Time    `json:"startTime,omitempty"`
	EndTime      *time.Time    `json:"endTime,omitempty"`
	UserEmail    string        `json:"user_email,omitempty"`
}

// Job status constants.
const (
	JobStatePending = "PENDING"
	JobStateRunning = "RUNNING"
	JobStateDone    = "DONE"
)

// JobConfig holds the configuration for a job.
type JobConfig struct {
	JobType string         `json:"jobType"` // QUERY, LOAD, EXTRACT, COPY
	Query   *QueryConfig   `json:"query,omitempty"`
	Load    *LoadConfig    `json:"load,omitempty"`
	Extract *ExtractConfig `json:"extract,omitempty"`
}

// QueryConfig holds query job configuration.
type QueryConfig struct {
	Query             string          `json:"query"`
	UseLegacySQL      bool            `json:"useLegacySql"`
	DestinationTable  *TableReference `json:"destinationTable,omitempty"`
	WriteDisposition  string          `json:"writeDisposition,omitempty"`  // WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
	CreateDisposition string          `json:"createDisposition,omitempty"` // CREATE_IF_NEEDED, CREATE_NEVER
}

// LoadConfig holds load job configuration.
type LoadConfig struct {
	SourceURIs       []string        `json:"sourceUris,omitempty"`
	SourceFormat     string          `json:"sourceFormat,omitempty"` // CSV, NEWLINE_DELIMITED_JSON, PARQUET
	DestinationTable *TableReference `json:"destinationTable"`
	Schema           *TableSchema    `json:"schema,omitempty"`
	WriteDisposition string          `json:"writeDisposition,omitempty"`
}

// ExtractConfig holds extract job configuration.
type ExtractConfig struct {
	SourceTable       *TableReference `json:"sourceTable"`
	DestinationURIs   []string        `json:"destinationUris,omitempty"`
	DestinationFormat string          `json:"destinationFormat,omitempty"` // CSV, NEWLINE_DELIMITED_JSON, AVRO
}

// JobStatus holds the current status of a job.
type JobStatus struct {
	State       string    `json:"state"` // PENDING, RUNNING, DONE
	ErrorResult *JobError `json:"errorResult,omitempty"`
}

// JobError represents an error that occurred during job execution.
type JobError struct {
	Reason   string `json:"reason"`
	Message  string `json:"message"`
	Location string `json:"location,omitempty"`
}

// JobStatistics holds statistics about a completed job.
type JobStatistics struct {
	TotalBytesProcessed int64            `json:"totalBytesProcessed,omitempty"`
	TotalSlotMs         int64            `json:"totalSlotMs,omitempty"`
	Query               *QueryStatistics `json:"query,omitempty"`
}

// QueryStatistics holds query-specific statistics.
type QueryStatistics struct {
	TotalBytesProcessed int64  `json:"totalBytesProcessed,omitempty"`
	TotalBytesBilled    int64  `json:"totalBytesBilled,omitempty"`
	CacheHit            bool   `json:"cacheHit"`
	StatementType       string `json:"statementType,omitempty"`
}

// Routine represents a BigQuery routine (UDF or stored procedure).
type Routine struct {
	ProjectID        string    `json:"projectId"`
	DatasetID        string    `json:"datasetId"`
	RoutineID        string    `json:"routineId"`
	RoutineType      string    `json:"routineType"` // SCALAR_FUNCTION, TABLE_VALUED_FUNCTION, PROCEDURE
	Language         string    `json:"language"`     // SQL, JAVASCRIPT
	Body             string    `json:"definitionBody"`
	CreationTime     time.Time `json:"creationTime"`
	LastModifiedTime time.Time `json:"lastModifiedTime"`
}

// Model represents a BigQuery ML model.
type Model struct {
	ProjectID        string    `json:"projectId"`
	DatasetID        string    `json:"datasetId"`
	ModelID          string    `json:"modelId"`
	ModelType        string    `json:"modelType"`
	Description      string    `json:"description,omitempty"`
	CreationTime     time.Time `json:"creationTime"`
	LastModifiedTime time.Time `json:"lastModifiedTime"`
}

// Validate checks that a Dataset has all required fields.
func (d *Dataset) Validate() error {
	if d.ProjectID == "" {
		return fmt.Errorf("dataset validation: ProjectID is required")
	}
	if d.DatasetID == "" {
		return fmt.Errorf("dataset validation: DatasetID is required")
	}
	return nil
}

// Validate checks that a Table has all required fields.
func (t *Table) Validate() error {
	if t.ProjectID == "" {
		return fmt.Errorf("table validation: ProjectID is required")
	}
	if t.DatasetID == "" {
		return fmt.Errorf("table validation: DatasetID is required")
	}
	if t.TableID == "" {
		return fmt.Errorf("table validation: TableID is required")
	}
	return nil
}

// Validate checks that a Job has all required fields.
func (j *Job) Validate() error {
	if j.ProjectID == "" {
		return fmt.Errorf("job validation: ProjectID is required")
	}
	if j.JobID == "" {
		return fmt.Errorf("job validation: JobID is required")
	}
	return nil
}

// Validate checks that a FieldSchema has all required fields.
// Validates nested fields recursively for RECORD/STRUCT types.
func (f *FieldSchema) Validate() error {
	if f.Name == "" {
		return fmt.Errorf("field schema validation: Name is required")
	}
	if f.Type == "" {
		return fmt.Errorf("field schema validation: Type is required for field %q", f.Name)
	}
	for i := range f.Fields {
		if err := f.Fields[i].Validate(); err != nil {
			return fmt.Errorf("field %q nested field: %w", f.Name, err)
		}
	}
	return nil
}

// Validate checks that an AccessEntry has all required fields.
// Role is required, and at least one entity field must be set.
func (a *AccessEntry) Validate() error {
	if a.Role == "" {
		return fmt.Errorf("access entry validation: Role is required")
	}
	hasEntity := a.UserByEmail != "" ||
		a.GroupByEmail != "" ||
		a.Domain != "" ||
		a.SpecialGroup != "" ||
		a.View != nil ||
		a.Dataset != nil ||
		a.Routine != nil ||
		a.IAMMember != ""
	if !hasEntity {
		return fmt.Errorf("access entry validation: at least one entity field must be set (userByEmail, groupByEmail, domain, specialGroup, view, dataset, routine, or iamMember)")
	}
	return nil
}
