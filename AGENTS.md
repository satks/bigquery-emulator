# Agent Guide — BigQuery Emulator

Instructions for AI coding agents working in this repository. Humans welcome too.

## What this is

A local BigQuery emulator in Go: BigQuery REST API v2 on the outside, DuckDB on the inside, with a string-level BigQuery-SQL→DuckDB-SQL translator in between. Module layout is a conventional Go project; no code generation, no protobufs to compile.

## Build, test, run

```bash
go build ./...                                   # build everything
go test ./...                                    # full test suite (~30s, no network needed)
go test ./... -race                              # what CI runs
go vet ./...                                     # keep clean
go test ./pkg/query/ -run TestTranslator -v      # iterate on the SQL translator
make build && ./bigquery-emulator --project=test-project --port=9050
```

- The `--project` flag is **required** — the binary exits with usage output without it.
- All tests use in-memory DuckDB; no Docker, credentials, or external services required.
- Quick manual check of any SQL shape:

```bash
curl -s -X POST http://localhost:9050/bigquery/v2/projects/test-project/queries \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT 1", "useLegacySql": false}'
```

## Where things live

| Concern | Location |
|---|---|
| SQL translation passes (ordered) | `pkg/query/translator.go` → `Translate()` |
| Function mappings (renames + handlers) | `pkg/query/functions.go` → `NewFunctionRegistry()` |
| MERGE decomposition | `pkg/query/merge.go` |
| Statement classification (query vs DML vs DDL) | `pkg/query/classifier.go` |
| REST handlers | `server/*_handlers.go`, routes in `server/router.go` |
| BQ wire-format value encoding | `server/helpers.go` → `formatValue()` |
| SQL-created tables → REST metadata | `server/ddl_sync.go` |
| BQ↔DuckDB↔Arrow type mapping | `pkg/types/mapping.go`, `pkg/types/arrow.go` |
| DuckDB connection + init macros | `pkg/connection/manager.go` |
| End-to-end HTTP tests | `tests/integration/integration_test.go` |

## Conventions and gotchas

- **Adding a BigQuery function:** simple rename → add to `simpleRenames` in `functions.go`; anything needing arg manipulation → a `Handler` closure. A handler's output must NEVER contain its own function name in callable form (`name(`) — the rewrite loop would re-match it. Use `unhex(md5(x))`-style nesting only because the cursor logic skips past replacements; test nested calls.
- **Adding a translation pass:** `Translate()` runs passes in a deliberate order (identifiers → types → temporal → CAST → `* EXCEPT` → UNNEST aliases → function registry). Match existing pass style; use `findMatchingParen`/`splitArgs` (quote- and depth-aware) instead of greedy regexes for anything containing nested parens.
- **DuckDB semantics that differ from BigQuery** (already handled — don't regress): `uuid()` returns UUID (INT128), not VARCHAR; `IGNORE NULLS` is window-only; UNNEST alias binds the table, not the element; hash functions return hex VARCHAR, not BYTES; `CAST(ubigint AS BIGINT)` raises on overflow; there is **no native MERGE**.
- **DML/DDL vs queries:** `ClassifySQL()` first, then `executor.Execute()` for DDL/DML and `executor.Query()` for SELECT. Never run DDL through `Query()`.
- **Test style:** table-driven, `TestTranslator_Translate_<Feature>` in `pkg/query/translator_test.go`; HTTP-level coverage goes in `tests/integration/` using the `runQuery`/`queryRowValues` helpers. Every SQL-shape bug fix gets both a unit test and, if it involved execution behavior, an integration subtest.
- **Wire format:** rows are `{"rows":[{"f":[{"v":"..."}]}]}`; TIMESTAMP serializes as epoch **microseconds** string; BYTES as base64; JSON columns as JSON text. Changing `formatValue` affects both Go and Node SDK compatibility tests.
- `CLAUDE.md` / `.wolf/` are an OpenWolf context-management setup used by one maintainer's tooling; safe to ignore unless you are that tooling.

## What not to attempt

- JavaScript UDFs (`LANGUAGE js`) — DuckDB has no JS engine; the translator intentionally fails fast with a clear error.
- Exact FarmHash64 fidelity for `FARM_FINGERPRINT` — it's a documented stable-but-different macro.
- Native MERGE passthrough — the bundled DuckDB build has no MERGE; keep the decomposition in `merge.go`.

See [COMPATIBILITY.md](COMPATIBILITY.md) for the full supported/unsupported matrix.
