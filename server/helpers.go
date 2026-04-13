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
// columnTypes is an optional list of BQ type strings (e.g., "TIMESTAMP", "DATE")
// for type-aware value formatting. Pass nil for default formatting.
func rowsToBQFormat(rows [][]interface{}, columnTypes ...interface{}) []map[string]interface{} {
	// Extract type strings from schema (accepts []query.ColumnMeta or similar)
	var types []string
	if len(columnTypes) > 0 {
		types = extractTypeStrings(columnTypes[0])
	}

	bqRows := make([]map[string]interface{}, len(rows))
	for i, row := range rows {
		fields := make([]map[string]interface{}, len(row))
		for j, val := range row {
			if val == nil {
				fields[j] = map[string]interface{}{"v": nil}
			} else {
				bqType := ""
				if j < len(types) {
					bqType = types[j]
				}
				fields[j] = map[string]interface{}{"v": formatValue(val, bqType)}
			}
		}
		bqRows[i] = map[string]interface{}{"f": fields}
	}
	return bqRows
}

// extractTypeStrings pulls BQ type strings from a schema slice.
// Accepts any slice of structs with a Type string field (e.g., query.ColumnMeta).
func extractTypeStrings(schema interface{}) []string {
	// Use reflection-free approach: try JSON roundtrip
	type hasType struct{ Type string }
	data, err := json.Marshal(schema)
	if err != nil {
		return nil
	}
	var cols []hasType
	if err := json.Unmarshal(data, &cols); err != nil {
		return nil
	}
	types := make([]string, len(cols))
	for i, c := range cols {
		types[i] = c.Type
	}
	return types
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

// formatValue converts a Go value to BQ JSON wire format.
// bqType is the BigQuery schema type (e.g., "TIMESTAMP", "DATE") which
// determines the exact string format both Go and Node.js SDKs expect.
func formatValue(v interface{}, bqType string) interface{} {
	if v == nil {
		return nil
	}

	upperType := strings.ToUpper(bqType)

	// Handle time.Time based on the BQ schema type
	if t, ok := v.(time.Time); ok {
		switch upperType {
		case "TIMESTAMP":
			// Go SDK: strconv.ParseInt(v, 10, 64) -> time.UnixMicro
			// Node.js SDK: BigInt(v) * BigInt(1000)
			return fmt.Sprintf("%d", t.UnixMicro())
		case "DATE":
			// Go SDK: civil.ParseDate expects "YYYY-MM-DD"
			return t.Format("2006-01-02")
		case "TIME":
			// Go SDK: civil.ParseTime expects "HH:MM:SS.ffffff"
			return t.Format("15:04:05.000000")
		case "DATETIME":
			// Go SDK: civil.ParseDateTime expects "YYYY-MM-DD HH:MM:SS.ffffff"
			return t.Format("2006-01-02 15:04:05.000000")
		default:
			// Unknown type with time.Time — default to TIMESTAMP (microseconds)
			return fmt.Sprintf("%d", t.UnixMicro())
		}
	}

	// Handle []byte — BYTES columns must be base64 encoded
	if b, ok := v.([]byte); ok {
		return base64.StdEncoding.EncodeToString(b)
	}

	// Handle bool — must be lowercase "true"/"false"
	if b, ok := v.(bool); ok {
		if b {
			return "true"
		}
		return "false"
	}

	// Handle float — use strconv to avoid scientific notation
	switch f := v.(type) {
	case float64:
		return strconv.FormatFloat(f, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(f), 'f', -1, 32)
	}

	// Handle integers — plain decimal string
	switch i := v.(type) {
	case int64:
		return strconv.FormatInt(i, 10)
	case int32:
		return strconv.FormatInt(int64(i), 10)
	case int:
		return strconv.Itoa(i)
	}

	// Default: fmt.Sprintf
	return fmt.Sprintf("%v", v)
}
