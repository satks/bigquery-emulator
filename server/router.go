package server

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
)

// setupRoutes registers all HTTP routes on the server's router.
func (s *Server) setupRoutes() {
	r := s.router

	// Middleware
	r.Use(s.authBypassMiddleware)
	r.Use(s.requestLoggerMiddleware)
	r.Use(s.contentTypeMiddleware)

	// Health check
	r.Get("/health", s.healthHandler)

	// Mock OAuth2 token endpoint (accepts any assertion, returns dummy token)
	r.Post("/token", s.tokenHandler)
	r.Post("/oauth2/v4/token", s.tokenHandler)

	// BigQuery REST API v2 routes

	// Project listing (no {projectId} param)
	r.Get("/bigquery/v2/projects", s.listProjects)

	r.Route("/bigquery/v2/projects/{projectId}", func(r chi.Router) {
		// Project detail (GET on the projectId itself)
		r.Get("/", s.getProject)

		// Datasets
		r.Get("/datasets", s.listDatasets)
		r.Post("/datasets", s.createDataset)
		r.Get("/datasets/{datasetId}", s.getDataset)
		r.Patch("/datasets/{datasetId}", s.patchDataset)
		r.Delete("/datasets/{datasetId}", s.deleteDataset)

		// Tables
		r.Get("/datasets/{datasetId}/tables", s.listTables)
		r.Post("/datasets/{datasetId}/tables", s.createTable)
		r.Get("/datasets/{datasetId}/tables/{tableId}", s.getTable)
		r.Patch("/datasets/{datasetId}/tables/{tableId}", s.patchTable)
		r.Delete("/datasets/{datasetId}/tables/{tableId}", s.deleteTable)

		// Table data
		r.Get("/datasets/{datasetId}/tables/{tableId}/data", s.listTableData)
		r.Post("/datasets/{datasetId}/tables/{tableId}/insertAll", s.insertAll)

		// Jobs
		r.Get("/jobs", s.listJobs)
		r.Post("/jobs", s.insertJob)
		r.Get("/jobs/{jobId}", s.getJob)
		r.Post("/jobs/{jobId}/cancel", s.cancelJob)
		r.Get("/queries/{jobId}", s.getQueryResults)
		r.Post("/queries", s.queriesInsert)
	})
}

// healthHandler returns a simple health check response.
func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// tokenHandler serves a mock OAuth2 token endpoint.
// Google Cloud SDKs exchange a JWT assertion for an access token here.
// The emulator accepts any assertion and returns a dummy bearer token.
func (s *Server) tokenHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"access_token": "emulator-token",
		"token_type":   "Bearer",
		"expires_in":   3600,
	})
}
