# BigQuery Emulator (DuckDB Backend)

A high-performance local BigQuery emulator powered by DuckDB. Drop-in replacement for Google Cloud BigQuery during development and testing.

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
docker build -t bigquery-emulator .
docker run -p 9050:9050 bigquery-emulator --project=test-project
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

## Documentation

- [ARCHITECTURE.md](ARCHITECTURE.md) — system layers, data flow, component details
- [DESIGN_DECISIONS.md](DESIGN_DECISIONS.md) — key technical decisions with rationale
- [SPEC.md](SPEC.md) — original DuckDB backend specification

## License

[MIT](LICENSE)
