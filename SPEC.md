# Technical Specification: BigQuery Emulator with DuckDB Backend

## 1. Overview & Objectives

**Goal:** Build a high-performance, local BigQuery emulator that seamlessly mimics the Google Cloud BigQuery REST and Storage APIs, utilizing DuckDB as the underlying execution engine.

### Why DuckDB instead of SQLite?

The current standard (goccy/bigquery-emulator) uses SQLite. While excellent for portability, SQLite is a row-based OLTP database. DuckDB is an in-process columnar OLAP database. Moving to DuckDB provides:

- **Analytical Performance:** Blazing fast aggregations, window functions, and multi-table joins.
- **Native Types:** Built-in support for LIST (BQ ARRAY) and STRUCT without needing JSON-string workarounds.
- **Native Arrow/Parquet:** Direct, zero-copy integration with Apache Arrow, which maps perfectly to the BigQuery Storage API.
- **SQL Compatibility:** DuckDB's Postgres-like dialect contains modern analytical functions that are much closer to Google Standard SQL (ZetaSQL) than SQLite.

## 2. System Architecture

The emulator will consist of three primary layers, written in Go to maximize compatibility with the existing goccy/bigquery-emulator API scaffolding.

### 2.1. API Layer (Frontend)

- **REST API:** Emulates the Google Cloud BigQuery API (`/bigquery/v2/projects/...`). Handles Jobs, Datasets, Tables, and Models metadata.
- **gRPC Storage API:** Emulates `google.cloud.bigquery.storage.v1`. Exposes ReadRows and AppendRows using Apache Arrow streams.

### 2.2. Translation Layer (ZetaSQL -> DuckDB SQL)

BigQuery clients send queries in **Google Standard SQL (ZetaSQL)**. DuckDB expects **DuckDB SQL**.

- **AST Parsing:** Use Google's go-zetasql to parse the incoming query and generate an Abstract Syntax Tree (AST) and resolved catalog.
- **SQL Transpiler:** A custom Go module (`zetaduckdb`) that traverses the ZetaSQL AST and emits DuckDB-compatible SQL.

### 2.3. Execution Engine (Backend)

- **Database Engine:** duckdb (via the `go-duckdb` CGO wrapper).
- **State Management:** DuckDB runs in either `:memory:` mode or file-backed mode (`emulator.duckdb`) for persistent state across restarts.

## 3. Data Model Mapping

### 3.1. Hierarchy Mapping

BigQuery utilizes a Project -> Dataset -> Table hierarchy. DuckDB uses a Catalog -> Schema -> Table hierarchy.

- **BQ Project** -> **DuckDB Catalog** (or isolated database files attached via `ATTACH`).
- **BQ Dataset** -> **DuckDB Schema** (`CREATE SCHEMA dataset_name;`).
- **BQ Table** -> **DuckDB Table**.

### 3.2. Type System Mapping

DuckDB has excellent coverage of BigQuery's complex types.

| BigQuery Type | DuckDB Equivalent | Notes |
|---|---|---|
| INT64 | BIGINT | 64-bit signed integer. |
| FLOAT64 | DOUBLE | 64-bit IEEE float. |
| NUMERIC / BIGNUMERIC | DECIMAL / HUGEINT | DuckDB supports up to 38 digits of precision natively. |
| BOOL | BOOLEAN | |
| STRING | VARCHAR | |
| BYTES | BLOB | |
| DATE / TIME / TIMESTAMP | DATE / TIME / TIMESTAMP | Timezones will require careful session-level configuration in DuckDB. |
| ARRAY\<T\> | LIST / T[] | Native support in DuckDB. |
| STRUCT\<...\> | STRUCT(...) | Native support in DuckDB. |
| JSON | JSON | DuckDB has a native JSON type/extension. |
| GEOGRAPHY | GEOMETRY | Provided via DuckDB's spatial extension. |

## 4. Component Implementation Plan

### Phase 1: Core API & Setup

- **Fork / Scaffold:** Start with the goccy/bigquery-emulator repository structure. Retain the HTTP/REST router (`server/server.go`) and Mock JSON responses.
- **Engine Interface:** Abstract the existing SQLite backend into a `Backend` interface (`Query(ctx, sql) -> Iterator`, `CreateTable(...)`, etc.).
- **DuckDB Integration:** Implement the `Backend` interface using `github.com/marcboeker/go-duckdb`.
  - *Initialization:* Automatically load DuckDB extensions (`json`, `parquet`, `spatial`).

### Phase 2: The SQL Translation Layer (ZetaSQL to DuckDB)

This is the most critical component. Since go-zetasql provides a resolved AST, we build a visitor pattern that translates nodes:

- **Standard Selects:** Pass-through (DuckDB handles standard SELECT, WHERE, GROUP BY perfectly).
- **UNNEST:** BigQuery: `SELECT * FROM UNNEST(arr)`. DuckDB: `SELECT * FROM unnest(arr)`. (Mostly 1:1, minor syntax tweaks).
- **Date/Time Functions:**
  - BQ: `CURRENT_TIMESTAMP()` -> DuckDB: `current_timestamp`
  - BQ: `DATE_ADD(date, INTERVAL 1 DAY)` -> DuckDB: `date + INTERVAL 1 DAY`
- **UDFs:** Implement missing BigQuery string/math functions (e.g., `FARM_FINGERPRINT`) by registering custom Go functions as DuckDB scalar UDFs via the C-API.

*Alternative approach for Phase 2:* Instead of building a transpiler from scratch, integrate **SQLGlot** (a Python/Rust transpiler) via an RPC or FFI call to translate bigquery dialect to duckdb dialect instantly.

### Phase 3: Metadata & Data Loading

- **System Tables:** Emulate BigQuery `INFORMATION_SCHEMA` by creating views in DuckDB that select from DuckDB's native `information_schema`.
- **YAML/Seed Loading:** Replace the row-by-row Go inserts with DuckDB's natively parallelized import functions.
  - *Implementation:* Generate a dynamic SQL statement: `CREATE TABLE table_a AS SELECT * FROM read_csv_auto('seed.csv');`
- **GCS Integration:** DuckDB has native HTTP/S3 support. To load data from a Google Cloud Storage URI (`gs://...`), utilize the DuckDB httpfs extension mapped to the local GCS emulator URL.

### Phase 4: Storage API (Arrow Integration)

The BigQuery Storage Read/Write APIs transfer data in Apache Arrow format.

- **ReadRows:** Instead of iterating through database rows in Go and converting to Arrow (as done with SQLite), utilize DuckDB's ability to output raw Arrow record batches directly from the C-API. This allows the emulator to stream gigabytes of data instantly to the gRPC client.
- **WriteRows (Append):** Take the incoming Arrow streams from the gRPC AppendRows endpoint, register the Arrow array via DuckDB's Arrow import API, and execute an `INSERT INTO target_table SELECT * FROM arrow_stream`.

## 5. Handling Key Challenges

### 5.1. Concurrency and Locking

- **Challenge:** DuckDB is highly optimized for multi-threaded *reads* but only allows a single *writer* at a time (at the database file level).
- **Solution:** For an emulator, this is generally acceptable. Implement a Go-level `sync.RWMutex` around DML statements (Inserts/Updates/DDL) to queue writes, while allowing concurrent read queries to pass directly to DuckDB.

### 5.2. Strict ZetaSQL Validation

- **Challenge:** BigQuery validation is extremely strict regarding type coercion.
- **Solution:** Use the go-zetasql analyzer *first* to validate the query semantics, catch type errors, and return identical error messages to real BigQuery. Only pass the query to DuckDB if ZetaSQL validation succeeds.

### 5.3. BigQuery specific syntax (e.g., CREATE OR REPLACE TABLE)

DuckDB supports `CREATE OR REPLACE TABLE`, but options like `OPTIONS(description="...", expiration_timestamp=...)` are not supported by DuckDB natively.

- **Solution:** The translation layer must strip BigQuery-specific `OPTIONS(...)` blocks before passing the DDL to DuckDB, and instead store these metadata properties in an internal Go map or a hidden SQLite/DuckDB metadata table to serve REST API GET requests for table descriptions.

## 6. SDK Compatibility Requirements

To ensure the emulator is a drop-in replacement for local development, it must be strictly compatible with the official Google Cloud SDKs.

### 6.1. Google Cloud Go SDK (cloud.google.com/go/bigquery)

- **Environment Variable Support:** The emulator must reliably intercept requests when the `BIGQUERY_EMULATOR_HOST` environment variable is set.
- **Payload Serialization:** JSON payloads returned by the REST API must precisely match the structs defined in the Go SDK (e.g., `JobConfiguration`, `TableReference`, `QueryResponse`). Missing or improperly typed fields can cause strict JSON unmarshalling panics in the client.
- **HTTP/gRPC Interoperability:** The Go SDK utilizes HTTP for control plane operations (Jobs, Datasets) and gRPC for the Storage API (Read/Append Rows). Both listeners must be active, operate over HTTP/1.1 and HTTP/2 seamlessly, and accept connections without enforcing TLS certificates.

### 6.2. Google Cloud Node.js SDK (@google-cloud/bigquery)

- **API Endpoint Overrides:** The emulator must fully support the `apiEndpoint` and `projectId` configurations passed directly into the `new BigQuery(...)` client constructor.
- **Authentication Bypass:** The Node.js SDK often expects an active Google Cloud authentication pipeline. The emulator must either accept dummy/anonymous OAuth2 credentials seamlessly or completely bypass 401 Unauthorized checks for incoming REST requests.
- **Streaming Inserts (table.insert()):** The emulator must handle the `insertAll` JSON HTTP payload natively used by the Node.js SDK, rapidly converting the JSON arrays into DuckDB INSERT statements or Arrow stream appends.
- **Pagination & Iterators:** The Node.js SDK heavily relies on async iterators to fetch large data volumes. The emulator must strictly implement the `pageToken` and `maxResults` logic for REST responses (Jobs, Query Rows, Datasets) to ensure iterators resolve correctly.

## 7. Development Roadmap Summary

- **Milestone 1: Proof of Concept:** REST API scaffold, basic hardcoded translation of SELECT queries, DuckDB in-memory execution, JSON response formatting.
- **Milestone 2: DDL & Metadata:** Support CREATE TABLE, DROP, Catalog/Schema mapping, and INFORMATION_SCHEMA views.
- **Milestone 3: Transpiler Hardening:** Build out the ZetaSQL AST -> DuckDB SQL visitor. Register custom UDFs in DuckDB for missing BQ functions.
- **Milestone 4: Storage API & SDKs:** Implement gRPC endpoints leveraging DuckDB's native zero-copy Apache Arrow integration. Ensure Node.js and Go SDK test suites pass.
- **Milestone 5: CI/CD & Testing:** Map the existing goccy/bigquery-emulator test suite to the new engine and ensure parity.
