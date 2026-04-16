# Architecture

## Overview

The BigQuery Emulator is a local HTTP server that emulates the Google Cloud BigQuery REST API v2, backed by DuckDB as the SQL execution engine. It translates BigQuery SQL to DuckDB SQL, manages metadata in DuckDB tables, and returns responses in the exact format both Go and Node.js BigQuery SDKs expect.

## System Layers

```
Client (Go SDK / Node.js SDK / curl)
    |
    v
[HTTP Server] (chi router, port 9050)
    |
    +-- Auth Bypass Middleware (accepts any token)
    +-- Request Logger Middleware
    +-- Content-Type Middleware (application/json)
    |
    v
[Route Handlers]
    |
    +-- Dataset CRUD (/datasets)
    +-- Table CRUD (/datasets/{ds}/tables)
    +-- Job Management (/jobs, /queries)
    +-- TableData (/tables/{t}/data, /insertAll)
    +-- Storage API (/v1/readStreams, /v1/writeStreams)
    +-- Project listing (/projects)
    +-- OAuth token (/token)
    |
    v
[SQL Translation Layer]
    |
    +-- Translator: BQ SQL -> DuckDB SQL
    |   +-- Backtick quoting -> double quotes
    |   +-- Project prefix stripping (3-part -> 2-part names)
    |   +-- ARRAY<TYPE> -> TYPE[]
    |   +-- BQ type names in DDL (INT64->BIGINT, STRING->VARCHAR, etc.)
    |   +-- Function registry (IFNULL->COALESCE, DATE_ADD, SAFE_CAST, etc.)
    |   +-- MERGE -> UPDATE + INSERT WHERE NOT EXISTS
    |   +-- OPTIONS(...) stripping
    |
    +-- Classifier: detects SELECT vs DDL vs DML vs MERGE
    |
    v
[Execution Engine]
    |
    +-- Executor.Query()   -- SELECT (returns rows)
    +-- Executor.Execute() -- DDL/DML (returns affected count)
    |
    v
[DuckDB] (via marcboeker/go-duckdb)
    |
    +-- In-memory (:memory:) or file-backed (.duckdb)
    +-- Extensions: json, parquet
    +-- Schemas = BQ Datasets
    +-- Tables = BQ Tables
    +-- _bq_* metadata tables
```

## Key Components

### Connection Manager (`pkg/connection/manager.go`)
- Wraps `*sql.DB` with `sync.RWMutex`
- Concurrent reads (RLock), serialized writes (Lock)
- Auto-loads DuckDB json + parquet extensions

### SQL Translator (`pkg/query/translator.go`)
- Regex-based BQ-to-DuckDB SQL translation
- Function registry with 20+ BQ function mappings
- MERGE decomposition into UPDATE + INSERT WHERE NOT EXISTS
- Project prefix stripping for 3-part identifiers

### Metadata Repository (`pkg/metadata/repository.go`)
- Stores project/dataset/table/job metadata in `_bq_*` DuckDB tables
- Dataset creation -> `CREATE SCHEMA`
- Table creation -> `CREATE TABLE` with type-mapped columns
- DDL sync: SQL-created schemas/tables auto-register in metadata

### Job Manager (`pkg/job/manager.go`)
- Async job execution in goroutines
- Job lifecycle: PENDING -> RUNNING -> DONE
- Query result storage for pagination
- DML results: totalRows=0 (data rows), numDmlAffectedRows=N

### Type System (`pkg/types/`)
- Bidirectional BQ <-> DuckDB type mapping
- REST API type names in schema responses (INTEGER, FLOAT, BOOLEAN, RECORD)
- Standard SQL aliases accepted on input (INT64, FLOAT64, BOOL, STRUCT)
- Arrow type conversion for Storage API

### Response Formatting (`server/helpers.go`)
- Type-aware value serialization per BQ schema type
- TIMESTAMP -> integer microseconds
- DATE -> "YYYY-MM-DD", TIME -> "HH:MM:SS.ffffff"
- BYTES -> base64, BOOL -> lowercase "true"/"false"
- FLOAT -> plain decimal (no scientific notation)
- REPEATED -> `[{"v":"elem1"},{"v":"elem2"}]`

### Permission System (`pkg/permission/`)
- 7 IAM roles, 30 permissions
- Dataset ACLs (OWNER/WRITER/READER)
- Row-level security with SQL predicate injection
- Column-level security with 7 masking types
- Bypass mode (default) for local development

## Data Flow: Query Execution

```
POST /queries {"query": "SELECT * FROM `project`.dataset.table WHERE id > 1"}
    |
    v
1. Translate: strip project prefix, convert backticks
   -> SELECT * FROM dataset.table WHERE id > 1
    |
    v
2. Classify: StatementQuery (IsQuery=true)
    |
    v
3. Execute: executor.Query() -> DuckDB
    |
    v
4. Format response:
   - Schema: column names + REST API type names
   - Rows: [{"f":[{"v":"value"}]}] with type-aware formatting
   - Metadata: totalRows, jobComplete, jobReference
    |
    v
5. Return JSON response
```

## Data Flow: MERGE (Upsert)

```
MERGE INTO target AS t USING (subquery) AS s ON t.pk = s.pk
WHEN MATCHED THEN UPDATE SET name = s.name
WHEN NOT MATCHED THEN INSERT(pk, name) VALUES(s.pk, s.name)
    |
    v
Decomposed to two statements:
1. UPDATE target SET name = s.name FROM (subquery) AS s WHERE target.pk = s.pk
2. INSERT INTO target (pk, name) SELECT s.pk, s.name FROM (subquery) AS s
   WHERE NOT EXISTS (SELECT 1 FROM target WHERE target.pk = s.pk)
```

## Dual-Prefix Routing

Routes are mounted at both `/bigquery/v2/...` and `/...`:
- Go SDK with `option.WithEndpoint()` uses `/bigquery/v2/` prefix
- Node.js SDK with `BIGQUERY_EMULATOR_HOST` omits the prefix

## Error Classification

DuckDB errors are mapped to appropriate HTTP status codes:
- "does not exist" / "not found" -> 404 (non-retryable in Go SDK)
- "parser error" / "syntax error" -> 400
- Everything else -> 500
