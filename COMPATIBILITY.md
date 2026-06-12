# BigQuery Compatibility Matrix

What this emulator supports, what it translates, and what it deliberately does not do. If you're evaluating whether your queries will run: the SQL below is verified by automated tests against real client SDKs.

A machine-readable description of the REST surface lives in [openapi.yaml](openapi.yaml) — keep the two in sync when support status changes.

## REST API v2 endpoints

| Area | Endpoints | Status |
|---|---|---|
| Projects | list, get | ✅ |
| Datasets | insert, get, list, patch, delete | ✅ (etag, pagination) |
| Tables | insert, get, list, patch, delete | ✅ (nested/repeated schemas, pagination) |
| Table data | list, insertAll (streaming inserts) | ✅ (pagination, BQ row format) |
| Jobs | insert, get, list, cancel, getQueryResults | ✅ (query/load/extract; load/extract are no-op stubs marked DONE) |
| Queries | synchronous `POST /queries` | ✅ |
| Storage API | createReadSession, readRows, createWriteStream, appendRows | ✅ (Arrow IPC) |
| Auth | mock `POST /token`, `POST /oauth2/v4/token` | ✅ (no real auth; bypass mode) |
| IAM / policies | role bindings, dataset ACLs, row-level security, column masking | ✅ in-process (bypass mode by default) |
| BI Engine, ML, routines, external tables, reservations | — | ❌ not implemented |

Routes are served under both `/bigquery/v2/projects/{id}/...` and `/projects/{id}/...` (Node SDK uses the latter).

## SQL: identifiers and types

| BigQuery | Behavior |
|---|---|
| `` `project.dataset.table` `` (backticked) | ✅ project stripped → `"dataset"."table"` |
| `project-id.dataset.table` (unquoted, hyphenated) | ✅ recognized in FROM/JOIN/INSERT/UPDATE/DELETE/CREATE/DROP position; arithmetic `a - b` untouched |
| `INT64`, `FLOAT64`, `STRING`, `BOOL`, `BYTES` | ✅ → BIGINT, DOUBLE, VARCHAR, BOOLEAN, BLOB |
| `NUMERIC`, `BIGNUMERIC` | ✅ → DECIMAL(38,9), DECIMAL(76,38) |
| `TIMESTAMP`, `DATETIME`, `DATE`, `TIME`, `JSON`, `GEOGRAPHY` | ✅ → TIMESTAMPTZ, TIMESTAMP, DATE, TIME, JSON, VARCHAR |
| `ARRAY<T>` | ✅ → `T[]` |
| `STRUCT<...>` typed constructor | ❌ untyped `STRUCT(x AS name)` only |
| `OPTIONS(...)` in DDL | ✅ stripped, stored as metadata |

## SQL: statements

| Statement | Status |
|---|---|
| SELECT / WITH (CTEs) / set operations / window functions / QUALIFY | ✅ (DuckDB native) |
| INSERT / UPDATE / DELETE / TRUNCATE | ✅ |
| CREATE / DROP SCHEMA & TABLE (incl. `IF [NOT] EXISTS`, CTAS, `OR REPLACE`) | ✅ — SQL-created objects appear in the REST API (metadata sync) |
| MERGE INTO ... WHEN MATCHED / NOT MATCHED [AND cond] | ✅ decomposed into UPDATE + INSERT (+ DELETE); aliases without `AS`, subquery USING, QUALIFY all handled |
| `SELECT * EXCEPT(cols)` | ✅ → `* EXCLUDE (cols)` |
| `SELECT * REPLACE(...)` | ❌ |
| `FROM UNNEST(arr) AS elem` | ✅ alias binds the element (BigQuery semantics) |
| `UNNEST ... WITH OFFSET` | ❌ |
| `CREATE FUNCTION ... LANGUAGE js` | ❌ fails fast with a clear error — DuckDB has no JavaScript engine |
| SQL UDFs (`CREATE FUNCTION` with SQL body) | ⚠️ passed through to DuckDB; BigQuery-specific syntax may not parse |

## SQL: functions

| BigQuery | Translation |
|---|---|
| `IFNULL`, `SAFE_CAST`, `COUNTIF` | `COALESCE`, `TRY_CAST`, `count_if` |
| `SAFE_DIVIDE(a, b)` | NULL-on-zero CASE expression |
| `ARRAY_AGG(x [IGNORE NULLS] [ORDER BY ...])` | `list(...)` — IGNORE/RESPECT NULLS stripped (aggregate-arg form is window-only in DuckDB), ORDER BY kept |
| `ARRAY_LENGTH`, `GENERATE_ARRAY` | `len`, `generate_series` |
| `STRUCT(e AS name, ...)` | `{'name': e, ...}` struct literal; unnamed args take the column name or `fN_` |
| `GENERATE_UUID()` | `CAST(uuid() AS VARCHAR)` — STRING like BigQuery, not DuckDB's INT128-backed UUID |
| `FARM_FINGERPRINT(s)` | deterministic signed INT64 macro; ⚠️ values ≠ real FarmHash64 (stable + well-distributed, fine for bucketing) |
| `MD5`, `SHA1`, `SHA256` | `unhex(...)` → real BYTES (base64 in JSON, `TO_BASE64` works). `SHA512` ❌ |
| `TO_JSON_STRING`, `PARSE_JSON`, `JSON_VALUE` | `to_json(...)::VARCHAR`, `json(...)`, native |
| `CURRENT_TIMESTAMP/DATE/TIME()` | parenless DuckDB equivalents |
| `DATE/TIMESTAMP/DATETIME/TIME` + `_ADD` / `_SUB` | `(expr) ± INTERVAL n unit` |
| `DATE_DIFF`, `DATE_TRUNC`, `TIMESTAMP_TRUNC` | `date_diff` / `date_trunc` (arg order swapped) |
| `FORMAT_DATE/TIMESTAMP`, `PARSE_DATE/TIMESTAMP` | `strftime` / `strptime` (arg order swapped) |
| `TIMESTAMP('...')`, `DATE('...')`, `TIME('...')` literals | typed literals / CASTs |
| `REGEXP_CONTAINS/EXTRACT/REPLACE` | `regexp_matches/extract/replace` |
| `STARTS_WITH`, `ENDS_WITH`, `BYTE_LENGTH`, `CHAR_LENGTH` | `starts_with`, `suffix`, `octet_length`, `length` |
| `ST_GEOGPOINT` | `ST_Point` (no full GEOGRAPHY support) |
| Anything not listed | passed through — DuckDB shares many function names with BigQuery, so much works untranslated; genuinely missing functions return a DuckDB "Catalog Error" |

## Wire format

| Type | JSON encoding (`rows[].f[].v`) |
|---|---|
| TIMESTAMP | epoch **microseconds** as string (SDK-compatible) |
| DATE / TIME / DATETIME | civil strings (`2006-01-02`, `15:04:05.000000`, ...) |
| BYTES | base64 |
| BOOL | `"true"` / `"false"` |
| JSON columns | JSON text (objects, arrays, scalars) |
| ARRAY | `[{"v": ...}, ...]` repeated form |
| NUMERIC overflow / page tokens / etags | BigQuery-style |

## Known divergences

- `FARM_FINGERPRINT` values differ from production BigQuery (documented above).
- STRUCT values returned through the REST API are JSON text, not BigQuery's nested `{"v":{"f":[...]}}` record encoding.
- Load/extract jobs succeed immediately without moving data (stubs for SDK flows).
- `UNNEST(...) AS x` as a *second* comma-joined table works; a hyphenated project path as a second comma-joined table does not.
- Permission system defaults to bypass (allow-all); enable enforcement programmatically if needed.

Found a gap? Open an issue with the failing SQL — see [SIGNALSMITH_GAPS.md](SIGNALSMITH_GAPS.md) for the repro format that gets fixes landed fast.
