package metadata

import (
	"encoding/json"
	"testing"
	"time"
)

func TestDataset_Validate_Valid(t *testing.T) {
	d := Dataset{
		ProjectID:    "my-project",
		DatasetID:    "my_dataset",
		CreationTime: time.Now(),
	}
	if err := d.Validate(); err != nil {
		t.Fatalf("expected valid dataset, got error: %v", err)
	}
}

func TestDataset_Validate_MissingProjectID(t *testing.T) {
	d := Dataset{
		DatasetID: "my_dataset",
	}
	err := d.Validate()
	if err == nil {
		t.Fatal("expected error for missing ProjectID, got nil")
	}
}

func TestDataset_Validate_MissingDatasetID(t *testing.T) {
	d := Dataset{
		ProjectID: "my-project",
	}
	err := d.Validate()
	if err == nil {
		t.Fatal("expected error for missing DatasetID, got nil")
	}
}

func TestTable_Validate_Valid(t *testing.T) {
	tbl := Table{
		ProjectID: "my-project",
		DatasetID: "my_dataset",
		TableID:   "my_table",
		Type:      "TABLE",
	}
	if err := tbl.Validate(); err != nil {
		t.Fatalf("expected valid table, got error: %v", err)
	}
}

func TestTable_Validate_MissingFields(t *testing.T) {
	tests := []struct {
		name  string
		table Table
	}{
		{
			name: "missing ProjectID",
			table: Table{
				DatasetID: "ds",
				TableID:   "tbl",
			},
		},
		{
			name: "missing DatasetID",
			table: Table{
				ProjectID: "proj",
				TableID:   "tbl",
			},
		},
		{
			name: "missing TableID",
			table: Table{
				ProjectID: "proj",
				DatasetID: "ds",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.table.Validate()
			if err == nil {
				t.Fatalf("expected error for %s, got nil", tt.name)
			}
		})
	}
}

func TestJob_Validate_Valid(t *testing.T) {
	j := Job{
		ProjectID: "my-project",
		JobID:     "job_123",
	}
	if err := j.Validate(); err != nil {
		t.Fatalf("expected valid job, got error: %v", err)
	}
}

func TestJob_Validate_MissingJobID(t *testing.T) {
	j := Job{
		ProjectID: "my-project",
	}
	err := j.Validate()
	if err == nil {
		t.Fatal("expected error for missing JobID, got nil")
	}

	j2 := Job{
		JobID: "job_123",
	}
	err = j2.Validate()
	if err == nil {
		t.Fatal("expected error for missing ProjectID, got nil")
	}
}

func TestFieldSchema_Validate_Valid(t *testing.T) {
	f := FieldSchema{
		Name: "user_id",
		Type: "INT64",
	}
	if err := f.Validate(); err != nil {
		t.Fatalf("expected valid field, got error: %v", err)
	}
}

func TestFieldSchema_Validate_MissingName(t *testing.T) {
	f := FieldSchema{
		Type: "STRING",
	}
	err := f.Validate()
	if err == nil {
		t.Fatal("expected error for missing Name, got nil")
	}
}

func TestFieldSchema_Validate_MissingType(t *testing.T) {
	f := FieldSchema{
		Name: "user_id",
	}
	err := f.Validate()
	if err == nil {
		t.Fatal("expected error for missing Type, got nil")
	}
}

func TestFieldSchema_Validate_NestedStruct(t *testing.T) {
	// Valid nested struct
	f := FieldSchema{
		Name: "address",
		Type: "RECORD",
		Fields: []FieldSchema{
			{Name: "street", Type: "STRING"},
			{Name: "city", Type: "STRING"},
		},
	}
	if err := f.Validate(); err != nil {
		t.Fatalf("expected valid nested field, got error: %v", err)
	}

	// Invalid: nested field missing name
	fBad := FieldSchema{
		Name: "address",
		Type: "RECORD",
		Fields: []FieldSchema{
			{Name: "street", Type: "STRING"},
			{Type: "STRING"}, // missing name
		},
	}
	err := fBad.Validate()
	if err == nil {
		t.Fatal("expected error for nested field missing name, got nil")
	}

	// Invalid: nested field missing type
	fBad2 := FieldSchema{
		Name: "address",
		Type: "RECORD",
		Fields: []FieldSchema{
			{Name: "street"}, // missing type
		},
	}
	err = fBad2.Validate()
	if err == nil {
		t.Fatal("expected error for nested field missing type, got nil")
	}
}

func TestAccessEntry_Validate_Valid(t *testing.T) {
	a := AccessEntry{
		Role:        "READER",
		UserByEmail: "user@example.com",
	}
	if err := a.Validate(); err != nil {
		t.Fatalf("expected valid access entry, got error: %v", err)
	}

	// Valid with group
	a2 := AccessEntry{
		Role:         "WRITER",
		GroupByEmail: "group@example.com",
	}
	if err := a2.Validate(); err != nil {
		t.Fatalf("expected valid access entry with group, got error: %v", err)
	}

	// Valid with special group
	a3 := AccessEntry{
		Role:         "OWNER",
		SpecialGroup: "projectOwners",
	}
	if err := a3.Validate(); err != nil {
		t.Fatalf("expected valid access entry with special group, got error: %v", err)
	}

	// Valid with domain
	a4 := AccessEntry{
		Role:   "READER",
		Domain: "example.com",
	}
	if err := a4.Validate(); err != nil {
		t.Fatalf("expected valid access entry with domain, got error: %v", err)
	}

	// Valid with view
	a5 := AccessEntry{
		Role: "READER",
		View: &TableReference{
			ProjectID: "proj",
			DatasetID: "ds",
			TableID:   "view1",
		},
	}
	if err := a5.Validate(); err != nil {
		t.Fatalf("expected valid access entry with view, got error: %v", err)
	}

	// Valid with IAM member
	a6 := AccessEntry{
		Role:      "READER",
		IAMMember: "user:someone@example.com",
	}
	if err := a6.Validate(); err != nil {
		t.Fatalf("expected valid access entry with IAM member, got error: %v", err)
	}
}

func TestAccessEntry_Validate_MissingRole(t *testing.T) {
	a := AccessEntry{
		UserByEmail: "user@example.com",
	}
	err := a.Validate()
	if err == nil {
		t.Fatal("expected error for missing Role, got nil")
	}
}

func TestAccessEntry_Validate_NoEntity(t *testing.T) {
	a := AccessEntry{
		Role: "READER",
	}
	err := a.Validate()
	if err == nil {
		t.Fatal("expected error for no entity field set, got nil")
	}
}

func TestDataset_JSONMarshal(t *testing.T) {
	now := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	d := Dataset{
		ProjectID:    "my-project",
		DatasetID:    "my_dataset",
		FriendlyName: "My Dataset",
		Description:  "A test dataset",
		Location:     "US",
		Labels:       map[string]string{"env": "test"},
		CreationTime: now,
		LastModifiedTime: now,
	}

	data, err := json.Marshal(d)
	if err != nil {
		t.Fatalf("failed to marshal dataset: %v", err)
	}

	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v", err)
	}

	if m["projectId"] != "my-project" {
		t.Fatalf("expected projectId 'my-project', got %v", m["projectId"])
	}
	if m["datasetId"] != "my_dataset" {
		t.Fatalf("expected datasetId 'my_dataset', got %v", m["datasetId"])
	}
	if m["friendlyName"] != "My Dataset" {
		t.Fatalf("expected friendlyName 'My Dataset', got %v", m["friendlyName"])
	}
	if m["location"] != "US" {
		t.Fatalf("expected location 'US', got %v", m["location"])
	}

	labels, ok := m["labels"].(map[string]interface{})
	if !ok {
		t.Fatal("expected labels to be an object")
	}
	if labels["env"] != "test" {
		t.Fatalf("expected label env=test, got %v", labels["env"])
	}

	// Verify omitempty: DefaultTableExpiration should not appear if zero
	if _, exists := m["defaultTableExpirationMs"]; exists {
		t.Fatal("defaultTableExpirationMs should be omitted when zero")
	}
	// Access should not appear when nil
	if _, exists := m["access"]; exists {
		t.Fatal("access should be omitted when nil")
	}
}

func TestTable_JSONMarshal(t *testing.T) {
	now := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	tbl := Table{
		ProjectID: "my-project",
		DatasetID: "my_dataset",
		TableID:   "my_table",
		Type:      "TABLE",
		Schema: &TableSchema{
			Fields: []FieldSchema{
				{Name: "id", Type: "INT64", Mode: "REQUIRED"},
				{Name: "name", Type: "STRING"},
			},
		},
		CreationTime:     now,
		LastModifiedTime: now,
		NumBytes:         1024,
		NumRows:          100,
	}

	data, err := json.Marshal(tbl)
	if err != nil {
		t.Fatalf("failed to marshal table: %v", err)
	}

	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v", err)
	}

	if m["projectId"] != "my-project" {
		t.Fatalf("expected projectId 'my-project', got %v", m["projectId"])
	}
	if m["tableId"] != "my_table" {
		t.Fatalf("expected tableId 'my_table', got %v", m["tableId"])
	}
	if m["type"] != "TABLE" {
		t.Fatalf("expected type 'TABLE', got %v", m["type"])
	}

	schema, ok := m["schema"].(map[string]interface{})
	if !ok {
		t.Fatal("expected schema to be an object")
	}
	fields, ok := schema["fields"].([]interface{})
	if !ok {
		t.Fatal("expected schema.fields to be an array")
	}
	if len(fields) != 2 {
		t.Fatalf("expected 2 fields, got %d", len(fields))
	}

	// Verify omitempty: description should not appear when empty
	if _, exists := m["description"]; exists {
		t.Fatal("description should be omitted when empty")
	}
	// expirationTime should not appear when nil
	if _, exists := m["expirationTime"]; exists {
		t.Fatal("expirationTime should be omitted when nil")
	}
	// viewQuery should not appear when empty
	if _, exists := m["viewQuery"]; exists {
		t.Fatal("viewQuery should be omitted when empty")
	}
}

func TestJob_StatusTransitions(t *testing.T) {
	j := Job{
		ProjectID: "my-project",
		JobID:     "job_123",
		Status: JobStatus{
			State: JobStatePending,
		},
	}

	if j.Status.State != "PENDING" {
		t.Fatalf("expected initial state PENDING, got %q", j.Status.State)
	}

	// Transition to RUNNING
	j.Status.State = JobStateRunning
	if j.Status.State != "RUNNING" {
		t.Fatalf("expected state RUNNING, got %q", j.Status.State)
	}

	// Transition to DONE
	j.Status.State = JobStateDone
	if j.Status.State != "DONE" {
		t.Fatalf("expected state DONE, got %q", j.Status.State)
	}

	// DONE with error
	j.Status.ErrorResult = &JobError{
		Reason:  "invalidQuery",
		Message: "Syntax error in SQL",
	}
	if j.Status.ErrorResult == nil {
		t.Fatal("expected error result to be set")
	}
	if j.Status.ErrorResult.Reason != "invalidQuery" {
		t.Fatalf("expected reason 'invalidQuery', got %q", j.Status.ErrorResult.Reason)
	}
}
