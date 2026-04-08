package apierror

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestBigQueryError_Error(t *testing.T) {
	err := &BigQueryError{
		Code:    404,
		Message: "Not found: Dataset my_project:my_dataset",
		Status:  "NOT_FOUND",
	}

	got := err.Error()
	if !strings.Contains(got, "404") {
		t.Fatalf("expected error string to contain '404', got %q", got)
	}
	if !strings.Contains(got, "Not found: Dataset my_project:my_dataset") {
		t.Fatalf("expected error string to contain message, got %q", got)
	}
}

func TestBigQueryError_StatusCode(t *testing.T) {
	tests := []struct {
		name string
		code int
	}{
		{"bad request", 400},
		{"unauthorized", 401},
		{"forbidden", 403},
		{"not found", 404},
		{"conflict", 409},
		{"internal", 500},
		{"not implemented", 501},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := &BigQueryError{Code: tt.code}
			if got := err.StatusCode(); got != tt.code {
				t.Fatalf("expected StatusCode() = %d, got %d", tt.code, got)
			}
		})
	}
}

func TestBigQueryError_WriteResponse(t *testing.T) {
	err := NewNotFoundError("Dataset", "my_project:my_dataset")

	w := httptest.NewRecorder()
	err.WriteResponse(w)

	resp := w.Result()
	if resp.StatusCode != 404 {
		t.Fatalf("expected HTTP status 404, got %d", resp.StatusCode)
	}

	contentType := resp.Header.Get("Content-Type")
	if contentType != "application/json" {
		t.Fatalf("expected Content-Type 'application/json', got %q", contentType)
	}

	var envelope map[string]json.RawMessage
	if jsonErr := json.NewDecoder(resp.Body).Decode(&envelope); jsonErr != nil {
		t.Fatalf("failed to decode response body: %v", jsonErr)
	}

	if _, ok := envelope["error"]; !ok {
		t.Fatal("expected response to have 'error' key")
	}

	var errorBody struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Status  string `json:"status"`
	}
	if jsonErr := json.Unmarshal(envelope["error"], &errorBody); jsonErr != nil {
		t.Fatalf("failed to unmarshal error body: %v", jsonErr)
	}
	if errorBody.Code != 404 {
		t.Fatalf("expected error code 404, got %d", errorBody.Code)
	}
	if errorBody.Status != "NOT_FOUND" {
		t.Fatalf("expected error status 'NOT_FOUND', got %q", errorBody.Status)
	}
}

func TestNewNotFoundError(t *testing.T) {
	err := NewNotFoundError("Dataset", "my_project:my_dataset")

	if err.Code != 404 {
		t.Fatalf("expected code 404, got %d", err.Code)
	}
	if err.Status != "NOT_FOUND" {
		t.Fatalf("expected status 'NOT_FOUND', got %q", err.Status)
	}
	if !strings.Contains(err.Message, "Dataset") {
		t.Fatalf("expected message to contain 'Dataset', got %q", err.Message)
	}
	if !strings.Contains(err.Message, "my_project:my_dataset") {
		t.Fatalf("expected message to contain resource name, got %q", err.Message)
	}
	if len(err.Errors) != 1 {
		t.Fatalf("expected 1 error detail, got %d", len(err.Errors))
	}
	if err.Errors[0].Reason != "notFound" {
		t.Fatalf("expected reason 'notFound', got %q", err.Errors[0].Reason)
	}
	if err.Errors[0].Domain != "global" {
		t.Fatalf("expected domain 'global', got %q", err.Errors[0].Domain)
	}
}

func TestNewAlreadyExistsError(t *testing.T) {
	err := NewAlreadyExistsError("Dataset", "my_project:my_dataset")

	if err.Code != 409 {
		t.Fatalf("expected code 409, got %d", err.Code)
	}
	if err.Status != "ALREADY_EXISTS" {
		t.Fatalf("expected status 'ALREADY_EXISTS', got %q", err.Status)
	}
	if !strings.Contains(err.Message, "Dataset") {
		t.Fatalf("expected message to contain 'Dataset', got %q", err.Message)
	}
	if !strings.Contains(err.Message, "my_project:my_dataset") {
		t.Fatalf("expected message to contain resource name, got %q", err.Message)
	}
	if len(err.Errors) != 1 {
		t.Fatalf("expected 1 error detail, got %d", len(err.Errors))
	}
	if err.Errors[0].Reason != "duplicate" {
		t.Fatalf("expected reason 'duplicate', got %q", err.Errors[0].Reason)
	}
}

func TestNewBadRequestError(t *testing.T) {
	err := NewBadRequestError("Invalid query syntax")

	if err.Code != 400 {
		t.Fatalf("expected code 400, got %d", err.Code)
	}
	if err.Status != "INVALID_ARGUMENT" {
		t.Fatalf("expected status 'INVALID_ARGUMENT', got %q", err.Status)
	}
	if err.Message != "Invalid query syntax" {
		t.Fatalf("expected message 'Invalid query syntax', got %q", err.Message)
	}
	if len(err.Errors) != 1 {
		t.Fatalf("expected 1 error detail, got %d", len(err.Errors))
	}
	if err.Errors[0].Reason != "invalidQuery" {
		t.Fatalf("expected reason 'invalidQuery', got %q", err.Errors[0].Reason)
	}
}

func TestNewForbiddenError(t *testing.T) {
	err := NewForbiddenError("Access denied to dataset")

	if err.Code != 403 {
		t.Fatalf("expected code 403, got %d", err.Code)
	}
	if err.Status != "FORBIDDEN" {
		t.Fatalf("expected status 'FORBIDDEN', got %q", err.Status)
	}
	if err.Message != "Access denied to dataset" {
		t.Fatalf("expected message 'Access denied to dataset', got %q", err.Message)
	}
	if len(err.Errors) != 1 {
		t.Fatalf("expected 1 error detail, got %d", len(err.Errors))
	}
	if err.Errors[0].Reason != "forbidden" {
		t.Fatalf("expected reason 'forbidden', got %q", err.Errors[0].Reason)
	}
}

func TestNewUnauthorizedError(t *testing.T) {
	err := NewUnauthorizedError("Invalid credentials")

	if err.Code != 401 {
		t.Fatalf("expected code 401, got %d", err.Code)
	}
	if err.Status != "UNAUTHENTICATED" {
		t.Fatalf("expected status 'UNAUTHENTICATED', got %q", err.Status)
	}
	if err.Message != "Invalid credentials" {
		t.Fatalf("expected message 'Invalid credentials', got %q", err.Message)
	}
	if len(err.Errors) != 1 {
		t.Fatalf("expected 1 error detail, got %d", len(err.Errors))
	}
	if err.Errors[0].Reason != "unauthorized" {
		t.Fatalf("expected reason 'unauthorized', got %q", err.Errors[0].Reason)
	}
}

func TestNewInternalError(t *testing.T) {
	err := NewInternalError("Something went wrong")

	if err.Code != 500 {
		t.Fatalf("expected code 500, got %d", err.Code)
	}
	if err.Status != "INTERNAL" {
		t.Fatalf("expected status 'INTERNAL', got %q", err.Status)
	}
	if err.Message != "Something went wrong" {
		t.Fatalf("expected message 'Something went wrong', got %q", err.Message)
	}
	if len(err.Errors) != 1 {
		t.Fatalf("expected 1 error detail, got %d", len(err.Errors))
	}
	if err.Errors[0].Reason != "backendError" {
		t.Fatalf("expected reason 'backendError', got %q", err.Errors[0].Reason)
	}
}

func TestNewNotImplementedError(t *testing.T) {
	err := NewNotImplementedError("MERGE statements")

	if err.Code != 501 {
		t.Fatalf("expected code 501, got %d", err.Code)
	}
	if err.Status != "UNIMPLEMENTED" {
		t.Fatalf("expected status 'UNIMPLEMENTED', got %q", err.Status)
	}
	if !strings.Contains(err.Message, "MERGE statements") {
		t.Fatalf("expected message to contain feature name, got %q", err.Message)
	}
	if len(err.Errors) != 1 {
		t.Fatalf("expected 1 error detail, got %d", len(err.Errors))
	}
	if err.Errors[0].Reason != "notImplemented" {
		t.Fatalf("expected reason 'notImplemented', got %q", err.Errors[0].Reason)
	}
}

func TestBigQueryError_JSONFormat(t *testing.T) {
	err := NewNotFoundError("Dataset", "my_project:my_dataset")

	w := httptest.NewRecorder()
	err.WriteResponse(w)

	var raw map[string]interface{}
	if jsonErr := json.NewDecoder(w.Result().Body).Decode(&raw); jsonErr != nil {
		t.Fatalf("failed to decode JSON: %v", jsonErr)
	}

	// Must have exactly one top-level key: "error"
	if len(raw) != 1 {
		t.Fatalf("expected exactly 1 top-level key, got %d", len(raw))
	}

	errorObj, ok := raw["error"].(map[string]interface{})
	if !ok {
		t.Fatal("expected 'error' to be an object")
	}

	// Check code
	code, ok := errorObj["code"].(float64) // JSON numbers are float64
	if !ok || int(code) != 404 {
		t.Fatalf("expected code 404, got %v", errorObj["code"])
	}

	// Check message
	message, ok := errorObj["message"].(string)
	if !ok || message != "Not found: Dataset my_project:my_dataset" {
		t.Fatalf("expected specific message, got %v", errorObj["message"])
	}

	// Check status
	status, ok := errorObj["status"].(string)
	if !ok || status != "NOT_FOUND" {
		t.Fatalf("expected status 'NOT_FOUND', got %v", errorObj["status"])
	}

	// Check errors array
	errors, ok := errorObj["errors"].([]interface{})
	if !ok || len(errors) != 1 {
		t.Fatalf("expected errors array with 1 element, got %v", errorObj["errors"])
	}

	detail, ok := errors[0].(map[string]interface{})
	if !ok {
		t.Fatal("expected error detail to be an object")
	}
	if detail["domain"] != "global" {
		t.Fatalf("expected domain 'global', got %v", detail["domain"])
	}
	if detail["reason"] != "notFound" {
		t.Fatalf("expected reason 'notFound', got %v", detail["reason"])
	}
	if detail["message"] != "Not found: Dataset my_project:my_dataset" {
		t.Fatalf("expected detail message to match, got %v", detail["message"])
	}

	// Verify the response implements http.ResponseWriter correctly
	resp := w.Result()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected HTTP 404, got %d", resp.StatusCode)
	}
}
