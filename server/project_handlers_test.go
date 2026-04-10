package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestListProjects(t *testing.T) {
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

	resp, err := http.Get(ts.URL + "/bigquery/v2/projects")
	if err != nil {
		t.Fatalf("GET /projects error = %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /projects status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	var body map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response error = %v", err)
	}

	if body["kind"] != "bigquery#projectList" {
		t.Errorf("kind = %v, want bigquery#projectList", body["kind"])
	}

	totalItems, ok := body["totalItems"].(float64)
	if !ok || totalItems < 1 {
		t.Errorf("totalItems = %v, want >= 1", body["totalItems"])
	}

	projects, ok := body["projects"].([]interface{})
	if !ok || len(projects) < 1 {
		t.Fatalf("projects missing or empty")
	}

	// Verify first project has correct structure
	p := projects[0].(map[string]interface{})
	if p["id"] != "test-project" {
		t.Errorf("project id = %v, want test-project", p["id"])
	}
	if p["kind"] != "bigquery#project" {
		t.Errorf("project kind = %v, want bigquery#project", p["kind"])
	}

	ref, ok := p["projectReference"].(map[string]interface{})
	if !ok {
		t.Fatal("projectReference missing")
	}
	if ref["projectId"] != "test-project" {
		t.Errorf("projectReference.projectId = %v, want test-project", ref["projectId"])
	}

	if _, ok := p["numericId"]; !ok {
		t.Error("numericId missing")
	}
	if _, ok := p["friendlyName"]; !ok {
		t.Error("friendlyName missing")
	}
}

func TestGetProject(t *testing.T) {
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

	resp, err := http.Get(ts.URL + "/bigquery/v2/projects/test-project")
	if err != nil {
		t.Fatalf("GET /projects/{id} error = %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /projects/{id} status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	var body map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response error = %v", err)
	}

	if body["kind"] != "bigquery#project" {
		t.Errorf("kind = %v, want bigquery#project", body["kind"])
	}
	if body["id"] != "test-project" {
		t.Errorf("id = %v, want test-project", body["id"])
	}

	ref, ok := body["projectReference"].(map[string]interface{})
	if !ok {
		t.Fatal("projectReference missing")
	}
	if ref["projectId"] != "test-project" {
		t.Errorf("projectReference.projectId = %v, want test-project", ref["projectId"])
	}

	if _, ok := body["numericId"]; !ok {
		t.Error("numericId missing")
	}
	if _, ok := body["friendlyName"]; !ok {
		t.Error("friendlyName missing")
	}
}

func TestGetProject_NotFound(t *testing.T) {
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

	resp, err := http.Get(ts.URL + "/bigquery/v2/projects/nonexistent-project")
	if err != nil {
		t.Fatalf("GET /projects/{id} error = %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET /projects/{id} status = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}

	var body map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response error = %v", err)
	}

	errObj, ok := body["error"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected error envelope, got %v", body)
	}
	if errObj["status"] != "NOT_FOUND" {
		t.Errorf("error status = %v, want NOT_FOUND", errObj["status"])
	}
}
