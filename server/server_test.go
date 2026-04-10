package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNew(t *testing.T) {
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

	if srv.Router() == nil {
		t.Fatal("Router() returned nil")
	}
	if srv.Handler() == nil {
		t.Fatal("Handler() returned nil")
	}
}

func TestServer_Health(t *testing.T) {
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

	resp, err := http.Get(ts.URL + "/health")
	if err != nil {
		t.Fatalf("GET /health error = %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET /health status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response error = %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("health status = %q, want %q", body["status"], "ok")
	}
}

func TestServer_TokenEndpoint(t *testing.T) {
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

	// Test both /token and /oauth2/v4/token paths
	for _, path := range []string{"/token", "/oauth2/v4/token"} {
		resp, err := http.Post(ts.URL+path, "application/x-www-form-urlencoded", nil)
		if err != nil {
			t.Fatalf("POST %s error = %v", path, err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("POST %s status = %d, want %d", path, resp.StatusCode, http.StatusOK)
		}

		var body map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatalf("POST %s decode error = %v", path, err)
		}

		if body["access_token"] != "emulator-token" {
			t.Errorf("POST %s access_token = %v, want %q", path, body["access_token"], "emulator-token")
		}
		if body["token_type"] != "Bearer" {
			t.Errorf("POST %s token_type = %v, want %q", path, body["token_type"], "Bearer")
		}
		if body["expires_in"] != float64(3600) {
			t.Errorf("POST %s expires_in = %v, want %v", path, body["expires_in"], 3600)
		}
	}
}

func TestServer_RoutesExist(t *testing.T) {
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

	routes := []struct {
		method string
		path   string
	}{
		{"GET", "/bigquery/v2/projects"},
		{"GET", "/bigquery/v2/projects/test-project"},
		{"GET", "/bigquery/v2/projects/test-project/datasets"},
		{"POST", "/bigquery/v2/projects/test-project/datasets"},
		{"GET", "/bigquery/v2/projects/test-project/datasets/ds1"},
		{"PATCH", "/bigquery/v2/projects/test-project/datasets/ds1"},
		{"DELETE", "/bigquery/v2/projects/test-project/datasets/ds1"},
		{"GET", "/bigquery/v2/projects/test-project/datasets/ds1/tables"},
		{"POST", "/bigquery/v2/projects/test-project/datasets/ds1/tables"},
		{"GET", "/bigquery/v2/projects/test-project/datasets/ds1/tables/tbl1"},
		{"PATCH", "/bigquery/v2/projects/test-project/datasets/ds1/tables/tbl1"},
		{"DELETE", "/bigquery/v2/projects/test-project/datasets/ds1/tables/tbl1"},
		{"GET", "/bigquery/v2/projects/test-project/datasets/ds1/tables/tbl1/data"},
		{"POST", "/bigquery/v2/projects/test-project/datasets/ds1/tables/tbl1/insertAll"},
		{"GET", "/bigquery/v2/projects/test-project/jobs"},
		{"POST", "/bigquery/v2/projects/test-project/jobs"},
		{"GET", "/bigquery/v2/projects/test-project/jobs/job1"},
		{"POST", "/bigquery/v2/projects/test-project/jobs/job1/cancel"},
		{"GET", "/bigquery/v2/projects/test-project/queries/job1"},
	}

	for _, rt := range routes {
		req, err := http.NewRequest(rt.method, ts.URL+rt.path, nil)
		if err != nil {
			t.Fatalf("NewRequest(%s %s) error = %v", rt.method, rt.path, err)
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Do(%s %s) error = %v", rt.method, rt.path, err)
		}

		// A chi framework 404 returns "404 page not found" in text/plain.
		// Our registered handlers return JSON even for 404 (not-found resources).
		// Check that Content-Type is application/json, which proves the route IS registered.
		ct := resp.Header.Get("Content-Type")
		resp.Body.Close()

		if ct != "application/json" {
			t.Errorf("%s %s returned Content-Type %q (route not registered?)", rt.method, rt.path, ct)
		}
	}
}
