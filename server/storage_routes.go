package server

import "github.com/sathish/bigquery-emulator/server/storage"

// setupStorageRoutes registers the BigQuery Storage API routes.
// If the storage service was not initialized (e.g., missing dependencies),
// this is a no-op.
func (s *Server) setupStorageRoutes() {
	if s.storageSvc == nil {
		return
	}
	s.storageSvc.RegisterRoutes(s.router)
}

// StorageService returns the storage service for testing.
func (s *Server) StorageService() *storage.Service {
	return s.storageSvc
}
