# BigQuery Emulator (DuckDB Backend)

[![CI](https://github.com/satks/bigquery-emulator/actions/workflows/ci.yml/badge.svg)](https://github.com/satks/bigquery-emulator/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Go 1.24](https://img.shields.io/badge/Go-1.24-00ADD8?logo=go)](go.mod)

A high-performance **local Google Cloud BigQuery emulator** powered by [DuckDB](https://duckdb.org). Run BigQuery integration tests offline — in CI, in Docker, or on your laptop — with **no GCP project, no credentials, and no network access**. The official Go and Node.js BigQuery client libraries work against it unchanged.

Speaks the **BigQuery REST API v2** and translates **BigQuery Standard SQL → DuckDB SQL** at runtime: project-qualified table paths, `STRUCT`/`ARRAY`, `MERGE`, `UNNEST`, `* EXCEPT`, the timestamp arithmetic families, and 30+ function mappings. See the full [compatibility matrix](COMPATIBILITY.md).

## Features

- **Full BigQuery REST API v2** — datasets, tables, jobs, tabledata, projects
- **DuckDB-powered SQL execution** — columnar OLAP engine with native ARRAY/STRUCT support
- **SQL translation** — BigQuery SQL to DuckDB SQL (20+ function mappings, type translation, MERGE support)
- **Go + Node.js SDK compatible** — tested against both `cloud.google.com/go/bigquery` and `@google-cloud/bigquery`
- **MERGE INTO support** — translated to UPDATE + INSERT WHERE NOT EXISTS (no UNIQUE constraint required)
- **Apache Arrow Storage API** — read/write streams with Arrow IPC format
- **Permission system** — IAM roles, dataset ACLs, row-level security, column masking (bypass mode default)
- **Proper error codes** — 404 for non-existent resources (non-retryable), 400 for syntax errors
- **Type-aware serialization** — TIMESTAMP as epoch microseconds, DATE/TIME/DATETIME as civil strings, BYTES as base64
- **Multi-platform Docker** — ARM64 + AMD64 support

## Quick Start

```bash
# Build and run
go build -o bigquery-emulator ./cmd/bigquery-emulator/
./bigquery-emulator --project=test-project --port=9050
```

### Go SDK

```go
client, _ := bigquery.NewClient(ctx, "test-project",
    option.WithEndpoint("http://localhost:9050"),
    option.WithoutAuthentication(),
)
```

### Node.js SDK

```javascript
const bigquery = new BigQuery({projectId: 'test-project'});
// Set env: BIGQUERY_EMULATOR_HOST=http://localhost:9050
// Patch gtoken for auth: GOOGLE_TOKEN_URL=http://localhost:9050/token
```

### Docker

```bash
# Pre-built multi-arch image (published from version tags)
docker run -p 9050:9050 ghcr.io/satks/bigquery-emulator --project=test-project

# Or build locally
docker build -t bigquery-emulator .
docker run -p 9050:9050 bigquery-emulator --project=test-project
```

### CI (GitHub Actions service container)

```yaml
services:
  bigquery:
    image: ghcr.io/satks/bigquery-emulator:latest
    ports: ["9050:9050"]
# then: BIGQUERY_EMULATOR_HOST=localhost:9050
```

## SQL Translation

The emulator translates BigQuery SQL to DuckDB SQL automatically:

| BigQuery | DuckDB |
|---|---|
| `` `project`.dataset.table `` | `dataset.table` (project stripped) |
| `ARRAY<STRING>` | `VARCHAR[]` |
| `INT64`, `FLOAT64`, `STRING`, `BOOL` | `BIGINT`, `DOUBLE`, `VARCHAR`, `BOOLEAN` |
| `IFNULL(a, b)` | `COALESCE(a, b)` |
| `SAFE_CAST(x AS INT64)` | `TRY_CAST(x AS BIGINT)` |
| `DATE_ADD(d, INTERVAL 1 DAY)` | `(d) + INTERVAL 1 DAY` |
| `TIMESTAMP('2024-01-01')` | `TIMESTAMPTZ '2024-01-01'` |
| `MERGE INTO ... USING ... ON ...` | `UPDATE ... FROM ... WHERE ...` + `INSERT ... WHERE NOT EXISTS` |
| `CREATE TABLE t (x INT64)` | `CREATE TABLE t (x BIGINT)` |
| `OPTIONS(description='...')` | stripped (stored as metadata) |

## API Endpoints

Routes are served at both `/bigquery/v2/projects/{projectId}/...` and `/projects/{projectId}/...` (Node.js SDK compatibility).

| Category | Endpoints |
|---|---|
| **Projects** | `GET /projects`, `GET /projects/{id}` |
| **Datasets** | `GET/POST /datasets`, `GET/PATCH/DELETE /datasets/{id}` |
| **Tables** | `GET/POST /datasets/{ds}/tables`, `GET/PATCH/DELETE /datasets/{ds}/tables/{id}` |
| **Table Data** | `GET /tables/{id}/data`, `POST /tables/{id}/insertAll` |
| **Jobs** | `GET/POST /jobs`, `GET /jobs/{id}`, `POST /jobs/{id}/cancel` |
| **Queries** | `POST /queries` (sync), `GET /queries/{jobId}` (poll results) |
| **Storage** | `POST .../readSessions`, `GET /readStreams/{id}:readRows`, `POST .../writeStreams`, `POST /writeStreams/{id}:appendRows` |
| **Auth** | `POST /token`, `POST /oauth2/v4/token` (mock OAuth) |
| **Health** | `GET /health` |

## Configuration

| Flag | Default | Description |
|---|---|---|
| `--project` | (required) | Google Cloud project ID |
| `--port` | `9050` | HTTP server port |
| `--grpc-port` | `9060` | gRPC server port (reserved) |
| `--database` | `:memory:` | DuckDB path (`:memory:` or file) |
| `--log-level` | `info` | Log level |

| Environment Variable | Description |
|---|---|
| `BIGQUERY_EMULATOR_HOST` | SDK auto-discovery (e.g., `localhost:9050`) |
| `BIGQUERY_EMULATOR_PROJECT` | Alternative to `--project` flag |

## Development

```bash
make build       # Build binary
make test-race   # Run tests with race detector
make bench       # Run benchmarks
make lint        # golangci-lint
make docker      # Build Docker image
```

### Project Structure

```
cmd/bigquery-emulator/   CLI entrypoint
pkg/
  connection/            DuckDB connection manager (sync.RWMutex)
  query/                 SQL classifier, translator, executor, MERGE decomposition
  types/                 BQ <-> DuckDB <-> Arrow type mapping
  metadata/              Models + repository (stored in DuckDB _bq_* tables)
  permission/            IAM roles, ACLs, RLS, column masking
  job/                   Async job manager with result pagination
server/
  handlers               Dataset/table/job/tabledata/project HTTP handlers
  storage/               Arrow IPC Storage API
  apierror/              BQ-compatible error responses
  ddl_sync.go            Auto-register SQL-created schemas/tables in metadata
  helpers.go             Type-aware value formatting, error classification
tests/
  integration/           End-to-end HTTP tests
  sdk/                   Go + Node.js SDK compatibility tests
  benchmark/             Performance benchmarks
```

## FAQ

**How do I test BigQuery locally without a GCP account?**
Run this emulator (`binary`, Docker, or CI service container), point your SDK at `http://localhost:9050` with auth disabled, and use it like real BigQuery. Everything runs in-process on DuckDB.

**How is this different from [goccy/bigquery-emulator](https://github.com/goccy/bigquery-emulator)?**
goccy's emulator parses SQL with ZetaSQL (Google's own parser) on a SQLite backend — stricter SQL fidelity, heavier dependency chain. This project uses DuckDB (a columnar OLAP engine, much closer to BigQuery's execution model for analytical SQL) with a translation layer, adds an enforceable permission system (IAM roles, row-level security, column masking), the Arrow Storage API, and is a single static-ish Go binary that builds in seconds. Pick by your workload: maximum parser fidelity → goccy; analytical query performance, ARRAY/STRUCT-heavy SQL, and CI speed → this one.

**Does it support MERGE / STRUCT / UNNEST / TIMESTAMP_ADD / `* EXCEPT`?**
Yes — see the [compatibility matrix](COMPATIBILITY.md) for the complete list with translation details.

**What doesn't work?**
JavaScript UDFs (no JS engine in DuckDB — clear error returned), `SHA512`, `UNNEST ... WITH OFFSET`, BigQuery ML / BI Engine / external tables. `FARM_FINGERPRINT` is deterministic but doesn't match real FarmHash64 values.

**Is the data persistent?**
By default it's in-memory. Pass `--database=/path/file.duckdb` for persistence across restarts.

## Documentation

- [COMPATIBILITY.md](COMPATIBILITY.md) — supported SQL, functions, endpoints, and known divergences
- [openapi.yaml](openapi.yaml) — OpenAPI 3.0 spec of the implemented REST API surface
- [AGENTS.md](AGENTS.md) — build/test/run guide for AI coding agents (and humans)
- [ARCHITECTURE.md](ARCHITECTURE.md) — system layers, data flow, component details
- [DESIGN_DECISIONS.md](DESIGN_DECISIONS.md) — key technical decisions with rationale
- [SPEC.md](SPEC.md) — original DuckDB backend specification
- [CONTRIBUTING.md](CONTRIBUTING.md) — the bug-report format that gets gaps fixed fast
- [llms.txt](llms.txt) — condensed project summary for LLMs and AI agents

## Keywords

BigQuery emulator · local BigQuery · BigQuery testing · mock BigQuery · fake BigQuery server · BigQuery Docker · BigQuery CI · DuckDB · offline BigQuery development · BigQuery integration tests · Go · golang

## License

[MIT](LICENSE)
