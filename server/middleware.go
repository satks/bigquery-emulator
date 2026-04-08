package server

import (
	"net/http"
	"time"

	"go.uber.org/zap"
)

// authBypassMiddleware accepts any request without authentication checks.
// If an Authorization header is present, it logs the fact but does not validate it.
func (s *Server) authBypassMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if auth := r.Header.Get("Authorization"); auth != "" {
			s.logger.Debug("auth header present (bypassed)", zap.String("path", r.URL.Path))
		}
		next.ServeHTTP(w, r)
	})
}

// requestLoggerMiddleware logs the method, path, status code, and duration of each request.
func (s *Server) requestLoggerMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		ww := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(ww, r)

		s.logger.Info("request",
			zap.String("method", r.Method),
			zap.String("path", r.URL.Path),
			zap.Int("status", ww.status),
			zap.Duration("duration", time.Since(start)),
		)
	})
}

// contentTypeMiddleware sets the Content-Type header to application/json on all responses.
func (s *Server) contentTypeMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		next.ServeHTTP(w, r)
	})
}

// statusWriter wraps http.ResponseWriter to capture the status code.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (sw *statusWriter) WriteHeader(code int) {
	sw.status = code
	sw.ResponseWriter.WriteHeader(code)
}
