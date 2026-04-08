package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// isNotFoundError checks if an error indicates a resource was not found.
// The repository layer returns errors containing "not found" for missing resources.
func isNotFoundError(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "not found")
}

// isAlreadyExistsError checks if an error indicates a resource already exists.
// The repository layer returns errors containing "already exists" or duplicate key errors.
func isAlreadyExistsError(err error) bool {
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "already exists") ||
		strings.Contains(msg, "duplicate") ||
		strings.Contains(msg, "constraint")
}

// writeJSON writes a JSON response with the given status code.
func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// rowsToBQFormat converts [][]interface{} to BigQuery row format:
// [{"f": [{"v": "value1"}, {"v": "value2"}]}, ...]
// NULL values are represented as {"v": null}.
func rowsToBQFormat(rows [][]interface{}) []map[string]interface{} {
	bqRows := make([]map[string]interface{}, len(rows))
	for i, row := range rows {
		fields := make([]map[string]interface{}, len(row))
		for j, val := range row {
			if val == nil {
				fields[j] = map[string]interface{}{"v": nil}
			} else {
				fields[j] = map[string]interface{}{"v": formatValue(val)}
			}
		}
		bqRows[i] = map[string]interface{}{"f": fields}
	}
	return bqRows
}

// formatValue converts a Go value to a string representation for BQ JSON output.
func formatValue(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	switch val := v.(type) {
	case []byte:
		return string(val)
	case time.Time:
		return val.Format(time.RFC3339Nano)
	default:
		return fmt.Sprintf("%v", val)
	}
}
