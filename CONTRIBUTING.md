# Contributing

Bug reports with a failing SQL statement are the most valuable contribution — most emulator gaps reduce to "BigQuery accepts this, DuckDB doesn't."

## Reporting a gap

Include:

1. The SQL (or API call) that fails
2. The emulator's error message
3. What real BigQuery returns for the same input

You can reproduce most issues without any client SDK:

```bash
./bigquery-emulator --project=test-project --port=9050
curl -s -X POST http://localhost:9050/bigquery/v2/projects/test-project/queries \
  -H 'Content-Type: application/json' \
  -d '{"query": "<your sql here>", "useLegacySql": false}'
```

## Pull requests

```bash
go build ./... && go vet ./... && go test ./... -race
```

- Every fix needs a test: translation fixes get a table-driven case in `pkg/query/translator_test.go` (or `merge_test.go` / `functions_test.go`), behavior fixes get an integration subtest in `tests/integration/`.
- See [AGENTS.md](AGENTS.md) for the code map, translation-pass conventions, and known DuckDB/BigQuery semantic traps.
- Update [COMPATIBILITY.md](COMPATIBILITY.md) when support status changes.

MIT licensed; contributions are accepted under the same license.
