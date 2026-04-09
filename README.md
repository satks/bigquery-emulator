# BigQuery Emulator (DuckDB Backend)

A high-performance local BigQuery emulator powered by DuckDB.

## Features

- Full BigQuery REST API v2 compatibility (datasets, tables, jobs, tabledata)
- DuckDB-powered SQL execution (columnar OLAP engine)
- BigQuery SQL to DuckDB SQL translation (functions, types, syntax)
- Apache Arrow-based Storage API for high-throughput reads/writes
- Complete permission system (IAM roles, dataset ACLs, row-level security, column masking)
- In-memory or file-backed persistent storage
- Multi-stage Docker build for minimal image size

## Quick Start

### Using Go

```bash
# Build
make build

# Run
./bigquery-emulator --project=test-project --port=9050
```

### Using Docker

```bash
# Build the image
docker build -t bigquery-emulator .

# Run
docker run -p 9050:9050 bigquery-emulator --project=test-project --port=9050
```

### Client SDK Configuration

Point your BigQuery client SDK to the emulator:

```bash
export BIGQUERY_EMULATOR_HOST=localhost:9050
```

```go
// Go client example
import "cloud.google.com/go/bigquery"

client, _ := bigquery.NewClient(ctx, "test-project",
    option.WithEndpoint("http://localhost:9050"),
    option.WithoutAuthentication(),
)
```

### Environment Variables

| Variable | Description |
|---|---|
| `BIGQUERY_EMULATOR_HOST` | Set by client SDKs to point to the emulator (e.g., `localhost:9050`) |
| `BIGQUERY_EMULATOR_PROJECT` | Default project ID (alternative to `--project` flag) |

## API Endpoints

All endpoints are under `/bigquery/v2/projects/{projectId}`.

### Datasets

| Method | Path | Description |
|---|---|---|
| `GET` | `/datasets` | List datasets |
| `POST` | `/datasets` | Create dataset |
| `GET` | `/datasets/{datasetId}` | Get dataset |
| `PATCH` | `/datasets/{datasetId}` | Update dataset |
| `DELETE` | `/datasets/{datasetId}` | Delete dataset |

### Tables

| Method | Path | Description |
|---|---|---|
| `GET` | `/datasets/{datasetId}/tables` | List tables |
| `POST` | `/datasets/{datasetId}/tables` | Create table |
| `GET` | `/datasets/{datasetId}/tables/{tableId}` | Get table |
| `PATCH` | `/datasets/{datasetId}/tables/{tableId}` | Update table |
| `DELETE` | `/datasets/{datasetId}/tables/{tableId}` | Delete table |

### Table Data

| Method | Path | Description |
|---|---|---|
| `GET` | `/datasets/{datasetId}/tables/{tableId}/data` | List table data (with pagination) |
| `POST` | `/datasets/{datasetId}/tables/{tableId}/insertAll` | Streaming insert |

### Jobs

| Method | Path | Description |
|---|---|---|
| `GET` | `/jobs` | List jobs |
| `POST` | `/jobs` | Submit job (query, load) |
| `GET` | `/jobs/{jobId}` | Get job status |
| `POST` | `/jobs/{jobId}/cancel` | Cancel job |
| `GET` | `/queries/{jobId}` | Get query results (with pagination) |

### Storage API

| Method | Path | Description |
|---|---|---|
| `POST` | `/v1/projects/{project}/datasets/{dataset}/tables/{table}/readSessions` | Create read session |
| `GET` | `/v1/readStreams/{streamName}:readRows` | Read rows (Arrow IPC) |
| `POST` | `/v1/projects/{project}/datasets/{dataset}/tables/{table}/writeStreams` | Create write stream |
| `POST` | `/v1/writeStreams/{streamName}:appendRows` | Append rows |

### Health

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Health check |

## Configuration

### CLI Flags

| Flag | Default | Description |
|---|---|---|
| `--project` | (required) | Google Cloud project ID |
| `--port` | `9050` | HTTP server port |
| `--grpc-port` | `9060` | gRPC server port |
| `--database` | `:memory:` | DuckDB database path (`:memory:` for in-memory) |
| `--log-level` | `info` | Log level (`debug`, `info`, `warn`, `error`) |
| `--version` | | Show version and exit |

## Development

### Prerequisites

- Go 1.24+
- CGO enabled (required for DuckDB driver)
- golangci-lint (for linting)

### Make Commands

```bash
make build       # Build the binary
make test        # Run all tests
make test-race   # Run tests with race detector
make bench       # Run benchmarks
make lint        # Run golangci-lint
make coverage    # Generate HTML coverage report
make clean       # Remove build artifacts
make docker      # Build Docker image
make run         # Build and run locally
```

### Project Structure

```
cmd/bigquery-emulator/   # CLI entrypoint
pkg/
  connection/            # DuckDB connection manager (RWMutex for concurrent reads)
  metadata/              # Metadata models and repository (stored in DuckDB)
  query/                 # SQL classifier, translator (BQ -> DuckDB), executor
  types/                 # Type mapping (BQ <-> DuckDB <-> Arrow)
  permission/            # IAM roles, ACLs, row-level security, column masking
  job/                   # Async job manager
server/                  # HTTP handlers, router, middleware
  apierror/              # BigQuery-compatible error responses
  storage/               # Storage API (Arrow IPC read/write)
```

### Running Tests

```bash
# All tests
go test ./...

# With race detector (recommended for CI)
go test -race -count=1 ./...

# Specific package
go test ./pkg/query/...

# With verbose output
go test -v ./server/...
```

## Architecture

The emulator maps BigQuery concepts to DuckDB:

| BigQuery | DuckDB |
|---|---|
| Project | Catalog / ATTACH |
| Dataset | Schema |
| Table | Table |

SQL translation converts BigQuery-specific functions and syntax to DuckDB equivalents at the string/regex level, with an interface designed for future AST-level translation.

The permission system uses a pluggable noop/bypass pattern (default: bypass ON) for zero overhead in development, with full IAM role checking, dataset ACLs, row-level security (SQL subquery injection), and column masking (SQL expression rewrite) when enabled.

## License

See [LICENSE](LICENSE) for details.
