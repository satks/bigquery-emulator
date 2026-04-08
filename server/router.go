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

	// BigQuery REST API v2 routes
	r.Route("/bigquery/v2/projects/{projectId}", func(r chi.Router) {
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
	})
}

// healthHandler returns a simple health check response.
func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}
