package types

import (
	"encoding/json"
	"testing"
)

func TestFieldSchema_HasRequiredFields(t *testing.T) {
	f := FieldSchema{
		Name: "user_id",
		Type: BQInt64,
	}
	if f.Name != "user_id" {
		t.Errorf("expected Name 'user_id', got %q", f.Name)
	}
	if f.Type != "INT64" {
		t.Errorf("expected Type 'INT64', got %q", f.Type)
	}
}

func TestFieldSchema_JSONMarshal(t *testing.T) {
	f := FieldSchema{
		Name:        "email",
		Type:        BQString,
		Mode:        ModeRequired,
		Description: "User email address",
	}

	data, err := json.Marshal(f)
	if err != nil {
		t.Fatalf("failed to marshal FieldSchema: %v", err)
	}

	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v", err)
	}

	if m["name"] != "email" {
		t.Errorf("expected name 'email', got %v", m["name"])
	}
	if m["type"] != "STRING" {
		t.Errorf("expected type 'STRING', got %v", m["type"])
	}
	if m["mode"] != "REQUIRED" {
		t.Errorf("expected mode 'REQUIRED', got %v", m["mode"])
	}
	if m["description"] != "User email address" {
		t.Errorf("expected description 'User email address', got %v", m["description"])
	}

	// Fields with omitempty should not appear when empty
	if _, exists := m["fields"]; exists {
		t.Error("empty fields should be omitted from JSON")
	}
	if _, exists := m["policyTags"]; exists {
		t.Error("nil policyTags should be omitted from JSON")
	}
}

func TestFieldSchema_JSONUnmarshal(t *testing.T) {
	// Simulates JSON from BigQuery API
	input := `{
		"name": "created_at",
		"type": "TIMESTAMP",
		"mode": "NULLABLE",
		"description": "Record creation time",
		"fields": [],
		"policyTags": {
			"names": ["projects/my-project/locations/us/taxonomies/123/policyTags/456"]
		}
	}`

	var f FieldSchema
	if err := json.Unmarshal([]byte(input), &f); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if f.Name != "created_at" {
		t.Errorf("expected Name 'created_at', got %q", f.Name)
	}
	if f.Type != BQTimestamp {
		t.Errorf("expected Type %q, got %q", BQTimestamp, f.Type)
	}
	if f.Mode != ModeNullable {
		t.Errorf("expected Mode %q, got %q", ModeNullable, f.Mode)
	}
	if f.Description != "Record creation time" {
		t.Errorf("expected Description 'Record creation time', got %q", f.Description)
	}
	if f.PolicyTags == nil {
		t.Fatal("expected PolicyTags to be non-nil")
	}
	if len(f.PolicyTags.Names) != 1 {
		t.Fatalf("expected 1 policy tag, got %d", len(f.PolicyTags.Names))
	}
	if f.PolicyTags.Names[0] != "projects/my-project/locations/us/taxonomies/123/policyTags/456" {
		t.Errorf("unexpected policy tag: %q", f.PolicyTags.Names[0])
	}
}

func TestFieldSchema_JSONUnmarshal_NestedStruct(t *testing.T) {
	input := `{
		"name": "address",
		"type": "RECORD",
		"mode": "NULLABLE",
		"fields": [
			{"name": "street", "type": "STRING", "mode": "NULLABLE"},
			{"name": "city", "type": "STRING", "mode": "REQUIRED"},
			{"name": "zip", "type": "STRING", "mode": "NULLABLE"}
		]
	}`

	var f FieldSchema
	if err := json.Unmarshal([]byte(input), &f); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if f.Type != BQRecord {
		t.Errorf("expected Type %q, got %q", BQRecord, f.Type)
	}
	if len(f.Fields) != 3 {
		t.Fatalf("expected 3 nested fields, got %d", len(f.Fields))
	}
	if f.Fields[1].Name != "city" {
		t.Errorf("expected second field name 'city', got %q", f.Fields[1].Name)
	}
	if f.Fields[1].Mode != ModeRequired {
		t.Errorf("expected second field mode REQUIRED, got %q", f.Fields[1].Mode)
	}
}

func TestTableSchema_Empty(t *testing.T) {
	s := TableSchema{}
	if s.Fields != nil {
		t.Error("empty TableSchema should have nil Fields")
	}

	s2 := TableSchema{Fields: []FieldSchema{}}
	if len(s2.Fields) != 0 {
		t.Error("TableSchema with empty slice should have 0 fields")
	}

	// Should marshal to valid JSON
	data, err := json.Marshal(s2)
	if err != nil {
		t.Fatalf("failed to marshal empty TableSchema: %v", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	fields, ok := result["fields"].([]interface{})
	if !ok {
		t.Fatal("expected 'fields' to be an array")
	}
	if len(fields) != 0 {
		t.Errorf("expected empty fields array, got %d items", len(fields))
	}
}

func TestTypeConstants(t *testing.T) {
	// Verify all type constants have expected values
	types := map[string]string{
		"BQInt64":      BQInt64,
		"BQFloat64":    BQFloat64,
		"BQNumeric":    BQNumeric,
		"BQBigNumeric": BQBigNumeric,
		"BQBool":       BQBool,
		"BQString":     BQString,
		"BQBytes":      BQBytes,
		"BQDate":       BQDate,
		"BQTime":       BQTime,
		"BQTimestamp":  BQTimestamp,
		"BQDatetime":   BQDatetime,
		"BQGeography":  BQGeography,
		"BQJson":       BQJson,
		"BQArray":      BQArray,
		"BQStruct":     BQStruct,
		"BQRecord":     BQRecord,
		"BQInterval":   BQInterval,
	}

	for name, val := range types {
		if val == "" {
			t.Errorf("type constant %s should not be empty", name)
		}
	}

	// RECORD is an alias for STRUCT in BQ, but has different constant value
	if BQRecord != "RECORD" {
		t.Errorf("BQRecord should be 'RECORD', got %q", BQRecord)
	}
	if BQStruct != "STRUCT" {
		t.Errorf("BQStruct should be 'STRUCT', got %q", BQStruct)
	}
}

func TestModeConstants(t *testing.T) {
	if ModeNullable != "NULLABLE" {
		t.Errorf("ModeNullable should be 'NULLABLE', got %q", ModeNullable)
	}
	if ModeRequired != "REQUIRED" {
		t.Errorf("ModeRequired should be 'REQUIRED', got %q", ModeRequired)
	}
	if ModeRepeated != "REPEATED" {
		t.Errorf("ModeRepeated should be 'REPEATED', got %q", ModeRepeated)
	}
}
