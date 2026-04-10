package server

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
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

// generateEtag generates a deterministic etag from a resource identifier.
func generateEtag(id string) string {
	h := sha256.Sum256([]byte(id))
	return base64.StdEncoding.EncodeToString(h[:])[:16]
}

// encodePageToken encodes an integer offset as an opaque base64 page token.
// The BigQuery API returns opaque tokens; SDKs pass them back verbatim.
func encodePageToken(offset int) string {
	return base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%d", offset)))
}

// decodePageToken decodes an opaque base64 page token back to an integer offset.
// Returns 0 if the token is empty or invalid.
func decodePageToken(token string) int {
	if token == "" {
		return 0
	}
	decoded, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return 0
	}
	val, err := strconv.Atoi(string(decoded))
	if err != nil {
		return 0
	}
	return val
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
