# Design Decisions

Key architectural and technical decisions made during development, with rationale.

## DuckDB as Backend (vs SQLite)

**Decision:** Use DuckDB instead of SQLite (which goccy/bigquery-emulator uses).

**Rationale:**
- DuckDB is columnar OLAP — blazing fast aggregations, window functions, joins
- Native LIST/STRUCT types map directly to BQ ARRAY/STRUCT (no JSON workarounds)
- Native Arrow integration for Storage API (zero-copy potential)
- SQL dialect closer to BigQuery's Google Standard SQL than SQLite

## DuckDB Driver: marcboeker/go-duckdb

**Decision:** Use `github.com/marcboeker/go-duckdb` v1.8.5.

**Rationale:** The spec referenced `duckdb-go/v2` which doesn't exist. marcboeker/go-duckdb is the working CGO driver, used by the snowflake-emulator. Registers as `"duckdb"` for `database/sql`.

## HTTP Router: chi/v5 (vs gorilla/mux)

**Decision:** Use `github.com/go-chi/chi/v5`.

**Rationale:** gorilla/mux is in maintenance mode. chi is lighter, actively maintained, and compatible with `net/http` middleware.

## Namespace: Strip-Prefix (vs Catalog Mode)

**Decision:** Strip the project prefix from SQL identifiers. Map `PROJECT.DATASET.TABLE` to `DATASET.TABLE` in DuckDB (2-level).

**Alternative considered:** ATTACH-based catalog mode (used by the snowflake-emulator for its DATABASE.SCHEMA.TABLE hierarchy).

**Rationale:**
- BigQuery's REST API already scopes by project in the URL path — no need to duplicate in SQL
- SDKs default to 2-part names; 3-part names are only for explicit cross-project queries
- Cross-project queries are rare in emulator usage (single-project via `--project` flag)
- Strip-prefix is ~20 lines vs ~200+ lines for catalog mode
- If multi-project is ever needed, catalog mode can be added as an opt-in flag

## MERGE Translation: UPDATE + INSERT WHERE NOT EXISTS (vs INSERT ON CONFLICT)

**Decision:** Translate BigQuery MERGE to `UPDATE ... FROM source WHERE condition` + `INSERT ... WHERE NOT EXISTS`.

**Alternative rejected:** `INSERT ... ON CONFLICT DO UPDATE` — requires a UNIQUE constraint on the conflict column. BigQuery tables don't have UNIQUE constraints; MERGE matches rows via arbitrary ON clauses.

**Rationale:** The UPDATE + INSERT approach works on any table without requiring UNIQUE indexes, matching BigQuery's semantics.

## Type Names: REST API Names in Responses

**Decision:** Return REST API type names (INTEGER, FLOAT, BOOLEAN, RECORD) in schema responses, not Standard SQL names (INT64, FLOAT64, BOOL, STRUCT).

**Rationale:** The Go SDK's read path does a direct string cast with no alias resolution. `INT64` causes "unrecognized type" errors. Both SDKs accept REST API names on input (they resolve aliases on write), but only REST API names on output.

## Value Serialization: Type-Aware formatValue()

**Decision:** `formatValue(v, bqType)` formats values based on the BQ schema type, not just the Go type.

**Rationale:** `time.Time` needs different formats for TIMESTAMP (integer microseconds), DATE ("YYYY-MM-DD"), TIME ("HH:MM:SS.ffffff"), and DATETIME ("YYYY-MM-DD HH:MM:SS.ffffff"). Without the schema type, all temporal values looked the same.

## DDL Metadata Sync

**Decision:** After executing DDL via SQL (CREATE SCHEMA, CREATE TABLE), auto-register the schema/table in the metadata repository so it appears in REST API listings.

**Rationale:** The REST API lists datasets/tables from `_bq_metadata` tables. SQL DDL bypasses the repository and creates DuckDB objects directly. Without sync, SQL-created objects are invisible to the REST API. The sync also introspects DuckDB's `information_schema.columns` to capture column schemas.

## Error Classification: 404 for "Does Not Exist"

**Decision:** Map DuckDB "does not exist" errors to HTTP 404 (not 500).

**Rationale:** The Go SDK treats 500 as retryable and loops for 30 seconds. 404 is non-retryable — the SDK fails immediately. Real BigQuery returns 404 for non-existent tables.

## DML Job Results: totalRows=0

**Decision:** For DML jobs (INSERT/UPDATE/DELETE), getQueryResults returns `totalRows: "0"` with `numDmlAffectedRows` carrying the affected count.

**Rationale:** The Go SDK uses `totalRows` for pagination. If totalRows > 0 but no rows are returned, it generates a pageToken and polls forever. Real BigQuery returns totalRows=0 for DML.

## getQueryResults with maxResults=0

**Decision:** Return schema + totalRows but zero rows when `maxResults=0`.

**Rationale:** The Go SDK's first poll uses `maxResults=0` as a completion check. It expects metadata only, no data rows. Returning rows with maxResults=0 confuses the SDK's pagination state.

## Auth Bypass + Mock Token Endpoint

**Decision:** Accept all requests without auth validation. Serve `/token` and `/oauth2/v4/token` with a dummy bearer token.

**Rationale:** The emulator is for local development. The Node.js SDK's `google-auth-library` needs a token endpoint; the Go SDK can use `option.WithoutAuthentication()`. The gtoken package in Node.js hardcodes Google's OAuth URL — this requires a patch-package workaround on the client side.

## Dual-Prefix Routing

**Decision:** Mount all BigQuery API routes at both `/bigquery/v2/...` and `/...`.

**Rationale:** The Go SDK uses `/bigquery/v2/` prefix. The Node.js SDK with `BIGQUERY_EMULATOR_HOST` omits it and hits `/projects/...` directly. Serving both avoids SDK-specific workarounds.

## Metadata Storage: DuckDB Tables (vs Separate SQLite)

**Decision:** Store metadata in `_bq_*` DuckDB tables within the same database instance.

**Rationale:** Single engine simplicity. No need to manage a separate SQLite database alongside DuckDB. Metadata operations share the same connection manager and transaction semantics.

## Permission Bypass as Default

**Decision:** Permission enforcement is OFF by default (bypass mode). All checks pass with zero overhead via the noop pattern.

**Rationale:** Local development shouldn't be blocked by permission errors. The bypass uses noop interface implementations (not if-checks), so there's zero branching overhead in the hot path.

## DDL Type Translation in SQL Body

**Decision:** Replace BQ-only type names (INT64, FLOAT64, STRING, BOOL, BYTES, BIGNUMERIC) with DuckDB equivalents using word-boundary regex in the translator.

**Rationale:** DuckDB doesn't recognize `INT64` or `STRING` as type names in DDL. The CAST path already handled this, but DDL column definitions bypassed it. The regex runs on every SQL statement, but the cost is negligible (~microseconds).
