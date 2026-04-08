package apierror

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// BigQueryError represents a BigQuery API error response.
// JSON format: {"error": {"code": 404, "message": "...", "status": "NOT_FOUND", "errors": [...]}}
type BigQueryError struct {
	Code    int           `json:"code"`
	Message string        `json:"message"`
	Status  string        `json:"status"`
	Errors  []ErrorDetail `json:"errors,omitempty"`
}

// ErrorDetail contains additional error information.
type ErrorDetail struct {
	Domain  string `json:"domain"`
	Reason  string `json:"reason"`
	Message string `json:"message"`
}

// Error implements the error interface.
func (e *BigQueryError) Error() string {
	return fmt.Sprintf("bigquery error %d (%s): %s", e.Code, e.Status, e.Message)
}

// StatusCode returns the HTTP status code for this error.
func (e *BigQueryError) StatusCode() int {
	return e.Code
}

// errorEnvelope wraps a BigQueryError in the BigQuery API envelope format.
type errorEnvelope struct {
	Error *BigQueryError `json:"error"`
}

// WriteResponse writes the BigQuery error envelope as JSON to the ResponseWriter.
func (e *BigQueryError) WriteResponse(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(e.Code)

	envelope := errorEnvelope{Error: e}
	_ = json.NewEncoder(w).Encode(envelope)
}

// NewNotFoundError creates a 404 NOT_FOUND error for a missing resource.
func NewNotFoundError(resourceType, resourceName string) *BigQueryError {
	msg := fmt.Sprintf("Not found: %s %s", resourceType, resourceName)
	return &BigQueryError{
		Code:    404,
		Message: msg,
		Status:  "NOT_FOUND",
		Errors: []ErrorDetail{
			{
				Domain:  "global",
				Reason:  "notFound",
				Message: msg,
			},
		},
	}
}

// NewAlreadyExistsError creates a 409 ALREADY_EXISTS error for a duplicate resource.
func NewAlreadyExistsError(resourceType, resourceName string) *BigQueryError {
	msg := fmt.Sprintf("Already exists: %s %s", resourceType, resourceName)
	return &BigQueryError{
		Code:    409,
		Message: msg,
		Status:  "ALREADY_EXISTS",
		Errors: []ErrorDetail{
			{
				Domain:  "global",
				Reason:  "duplicate",
				Message: msg,
			},
		},
	}
}

// NewBadRequestError creates a 400 INVALID_ARGUMENT error.
func NewBadRequestError(message string) *BigQueryError {
	return &BigQueryError{
		Code:    400,
		Message: message,
		Status:  "INVALID_ARGUMENT",
		Errors: []ErrorDetail{
			{
				Domain:  "global",
				Reason:  "invalidQuery",
				Message: message,
			},
		},
	}
}

// NewForbiddenError creates a 403 FORBIDDEN error.
func NewForbiddenError(message string) *BigQueryError {
	return &BigQueryError{
		Code:    403,
		Message: message,
		Status:  "FORBIDDEN",
		Errors: []ErrorDetail{
			{
				Domain:  "global",
				Reason:  "forbidden",
				Message: message,
			},
		},
	}
}

// NewUnauthorizedError creates a 401 UNAUTHENTICATED error.
func NewUnauthorizedError(message string) *BigQueryError {
	return &BigQueryError{
		Code:    401,
		Message: message,
		Status:  "UNAUTHENTICATED",
		Errors: []ErrorDetail{
			{
				Domain:  "global",
				Reason:  "unauthorized",
				Message: message,
			},
		},
	}
}

// NewInternalError creates a 500 INTERNAL error.
func NewInternalError(message string) *BigQueryError {
	return &BigQueryError{
		Code:    500,
		Message: message,
		Status:  "INTERNAL",
		Errors: []ErrorDetail{
			{
				Domain:  "global",
				Reason:  "backendError",
				Message: message,
			},
		},
	}
}

// NewNotImplementedError creates a 501 UNIMPLEMENTED error for unsupported features.
func NewNotImplementedError(feature string) *BigQueryError {
	msg := fmt.Sprintf("Not implemented: %s", feature)
	return &BigQueryError{
		Code:    501,
		Message: msg,
		Status:  "UNIMPLEMENTED",
		Errors: []ErrorDetail{
			{
				Domain:  "global",
				Reason:  "notImplemented",
				Message: msg,
			},
		},
	}
}
