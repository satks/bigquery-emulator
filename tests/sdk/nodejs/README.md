# Node.js SDK Compatibility Tests

Manual tests to verify the BigQuery emulator works with the official
`@google-cloud/bigquery` Node.js client library.

## Prerequisites

```bash
node --version   # v18+ recommended
npm init -y
npm install @google-cloud/bigquery
```

## Running

1. Start the emulator:

```bash
# From the project root
go run ./cmd/bigquery-emulator --project=test-project --port=9050
```

2. Set environment variables and run the test:

```bash
export BIGQUERY_EMULATOR_HOST=localhost:9050
node test.js
```

## What the test covers

- Create a dataset
- Create a table with a typed schema
- Insert rows via the streaming insert API
- Run a SQL query and read results
- List datasets
- List tables in a dataset
- Error handling (querying a nonexistent table)

## Expected output

```
[PASS] Create dataset
[PASS] Create table
[PASS] Insert rows
[PASS] Query rows
[PASS] List datasets
[PASS] List tables
[PASS] Error handling
All tests passed!
```

## Troubleshooting

- **Connection refused**: Make sure the emulator is running on the port
  specified in `BIGQUERY_EMULATOR_HOST`.
- **Authentication errors**: The emulator runs in auth-bypass mode by default.
  No credentials are needed.
- **BIGQUERY_EMULATOR_HOST not set**: The test defaults to `localhost:9050`.
