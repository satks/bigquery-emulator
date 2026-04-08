package server

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMiddleware_AuthBypass_NoHeader(t *testing.T) {
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

	// Request without any auth header should pass through
	resp, err := http.Get(ts.URL + "/health")
	if err != nil {
		t.Fatalf("GET /health error = %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want %d (auth bypass should allow request without auth)", resp.StatusCode, http.StatusOK)
	}
}

func TestMiddleware_AuthBypass_BearerToken(t *testing.T) {
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

	// Request with bearer token should also pass through
	req, err := http.NewRequest("GET", ts.URL+"/health", nil)
	if err != nil {
		t.Fatalf("NewRequest error = %v", err)
	}
	req.Header.Set("Authorization", "Bearer ya29.some-fake-token")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do() error = %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want %d (auth bypass should allow request with bearer token)", resp.StatusCode, http.StatusOK)
	}
}

func TestMiddleware_ContentType(t *testing.T) {
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

	ct := resp.Header.Get("Content-Type")
	if ct != "application/json" {
		t.Errorf("Content-Type = %q, want %q", ct, "application/json")
	}
}

func TestMiddleware_RequestLogger(t *testing.T) {
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

	// Just verify the middleware doesn't crash - hard to test actual logging
	resp, err := http.Get(ts.URL + "/health")
	if err != nil {
		t.Fatalf("GET /health error = %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want %d (request logger middleware should not interfere)", resp.StatusCode, http.StatusOK)
	}
}
