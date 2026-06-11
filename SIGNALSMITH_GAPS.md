# SignalSmith E2E — Gap Fixes

> **Status update (2026-06-11):** Gaps **#1–#6 fixed** (commits `eb8cb48`..`c13f08f`);
> every repro below re-verified live via `POST /queries`. #7 remains open
> (needs the signalsmith stack for a repro — re-check now that #6 is fixed).
> A pre-existing translator bug was found and fixed along the way:
> `translateFunctions` looped forever on self-matching output
> (`STARTS_WITH→starts_with`; see `.wolf/buglog.json` bug-050).
> Pending re-run of the full SignalSmith suite against the new emulator ref.

Findings from running the SignalSmith E2E suite against this emulator
(commit `3b40d55`, 2026-06-10). Suite results: `bigquery-foundation` **14/14**,
`bigquery-parallel` **388 passed / 30 failed / 124 skipped**. Every failure
below was reproduced live against the emulator via `POST /queries` — these are
verified verdicts, not log guesses.

Product-side SQL generator references point into the signalsmith repo
(`~/projects/signalsmith`).

---

## 1. ✅ FIXED — Unquoted hyphenated project IDs in table paths — the monster (≈157 errors)

| | |
|---|---|
| **Signature** | `Parser Error: syntax error at or near "-"` |
| **Repro** | `SELECT * FROM test-project.gap_test.t1` → ❌ &nbsp;(`` `test-project`.gap_test.t1 `` → ✅ in both query & DML paths) |
| **Root cause** | BigQuery legally accepts **unquoted dash-containing project IDs in table paths**. The translator (`pkg/query/translator.go`) only rewrites backtick-quoted identifiers (`backtickRe`); unquoted `proj-id.dataset.table` passes through and DuckDB parses `test-project` as subtraction. |
| **Fix** | Add a pre-pass that recognizes unquoted `name-with-dashes.dataset.table` references in table-path position (FROM / JOIN / INSERT / UPDATE / DELETE / CREATE targets) and routes them through the same rewrite as the backticked form. Token-aware, not a bare regex — must not touch `a - b` arithmetic. |
| **Unblocks** | The dominant share of the 30 failures + 157 cleanup-query errors (tests 06–13, 15–17, 19, 21–24, 28–33, 35, 42 paths). |

## 2. ✅ FIXED — `SELECT * EXCEPT(col)` → DuckDB `* EXCLUDE (col)`

| | |
|---|---|
| **Signature** | `Parser Error: syntax error at or near "_pp_rn"` |
| **Repro** | `SELECT * EXCEPT(_pp_rn) FROM (SELECT 1 AS a, 2 AS _pp_rn)` → ❌ |
| **Root cause** | BigQuery `* EXCEPT(...)` unsupported; DuckDB parses `EXCEPT` as a set operation. SignalSmith emits it in canvas post-processing (`internal/compiler/canvas_postprocess.go:59`). |
| **Fix** | Translator: rewrite `* EXCEPT(cols)` / `alias.* EXCEPT(cols)` → `* EXCLUDE (cols)`. Select-list position only. |
| **Unblocks** | Canvas Prioritize / Rank & Cap / per-node estimates (41, 42, 43 E/H/I). |

## 3. ✅ FIXED — `COUNTIF(cond)` → `count_if(cond)`

| | |
|---|---|
| **Signature** | `Catalog Error: Scalar Function with name countif does not exist! Did you mean "count_if"?` |
| **Repro** | `SELECT COUNTIF(x > 1) FROM (SELECT 2 AS x)` → ❌ |
| **Fix** | One-word function-name mapping in the translator (call position only). DuckDB's `count_if` is semantically identical. |
| **Unblocks** | 38-audience-overlap. |

## 4. ✅ FIXED — `FARM_FINGERPRINT(s)` — missing, must return signed INT64

| | |
|---|---|
| **Signature** | `Catalog Error: Scalar Function with name farm_fingerprint does not exist` |
| **Repro** | `SELECT MOD(ABS(FARM_FINGERPRINT(CAST('abc' AS STRING))), 100)` → ❌ |
| **Usage** | Deterministic bucketing: `MOD(ABS(FARM_FINGERPRINT(...)), N)` — journey AB-split (`internal/compiler/journey_compiler.go:682`) and canvas % split (`internal/compiler/canvas_postprocess.go:216`). |
| **Fix** | Register at connection init (e.g. `pkg/connection/manager.go`): `CREATE MACRO farm_fingerprint(s) AS CAST(hash(s) AS BIGINT)` — DuckDB `hash()` returns UBIGINT; the cast wraps to signed 64-bit, which satisfies `ABS/MOD`. ⚠️ Values won't equal real FarmHash64 — fine for bucketing tests (stability + distribution is what's asserted); implement true farmhash as a Go UDF later if cross-fidelity matters. Document the divergence. |
| **Unblocks** | Journey AB-split determinism/distribution (25, 27), canvas split D.2 (43). |

## 5. ✅ FIXED — `STRUCT(expr AS field, …)` — named-struct constructor

| | |
|---|---|
| **Signature** | `Parser Error: syntax error at or near "AS"` |
| **Repro** | `SELECT STRUCT(1 AS a, 'x' AS b)` → ❌ (and `ARRAY_AGG(STRUCT(o AS v))` → ❌, the exact trait-materialization shape) |
| **Root cause** | DuckDB doesn't allow `AS` inside function args. Trait materialization emits `ARRAY_AGG(STRUCT(_ranked.col AS col, …))`. Note `CREATE TABLE … AS WITH …` and `SAFE_CAST` are **fine** — verified; the STRUCT is the only failure in that statement. |
| **Fix** | Translator: rewrite `STRUCT(e1 AS n1, e2 AS n2)` → `{'n1': e1, 'n2': e2}` (or `struct_pack(n1 := e1, …)`). Needs paren-depth-aware tokenizing, not regex — args contain nested calls/commas. |
| **Unblocks** | Traits behavioral pipeline (10, 36, 37). |

## 6. ✅ FIXED — `MD5()` returns hex VARCHAR — should be BYTES/BLOB

| | |
|---|---|
| **Signature** | `Binder Error: to_base64(VARCHAR)… Candidate: to_base64(BLOB)` |
| **Repro** | `SELECT MD5('abc')` → `"900150983cd24fb0d6963f7d28e17f72"` (hex string; real BigQuery returns BYTES, rendered base64 `kAFQmDzST7DWlj99KOF/cg==` in JSON). `TO_BASE64(MD5('abc'))` → ❌ |
| **Fix** | Map `MD5(x)` → `unhex(md5(x))` so it yields BLOB; JSON encoding then naturally produces base64, and `TO_BASE64` binds. Audit `SHA1/SHA256/SHA512` for the same gap. |
| **Unblocks** | Sync row-hash comparison fidelity (`internal/connector/bigquery_sync.go:73`); prerequisite for #7. |

## 7. ⏳ OPEN — Golden-record reads: `Could not convert string '…==' to INT128` — needs a repro

| | |
|---|---|
| **Signature** | `Conversion Error: Could not convert string 'KBsWxukaRtatKAjU9XT08w==' to INT128` (signalsmith tests 18, 34) |
| **What's known** | The base64 values are identity `ss_id`s computed **in Go** by the SignalSmith API and inserted as literals; the failing reads are plain test-side queries on `_GOLDEN_RECORD` / `_IDENTITY_GRAPH`. Something in the emulator typed a column as HUGEINT (INT128) where BigQuery would have STRING — comparing/joining the base64 literal then forces a failing cast. Suspects: NULL-typed columns in CTAS inference, or a NUMERIC↔HUGEINT mapping (`pkg/types/mapping.go:59` maps DuckDB HUGEINT→BigNumeric on the way out — check the inbound direction). |
| **To pin down** | Bring the signalsmith stack up, run `18-golden-record-incremental`, then `DESCRIBE` the `_IDENTITY_GRAPH` / `_GOLDEN_RECORD` tables in the emulator and see which column became HUGEINT and from what CTAS expression. Fixing #6 first may change the picture. |

---

## Suggested order (impact-per-effort)

1. **#1 hyphenated projects** — biggest unlock, by far
2. **#3 COUNTIF + #4 FARM_FINGERPRINT** — trivial mappings, three test files
3. **#2 `* EXCEPT`** — moderate, unlocks canvas suite
4. **#5 STRUCT** — hardest rewrite, unlocks traits
5. **#6 MD5→BLOB**, then **#7 repro** — golden record last

## Verification loop

After each fix, from the signalsmith repo:

```bash
# push the emulator commit, then bump EMULATOR_REF in e2e/bigquery-emulator/Dockerfile
make e2e-bigquery-emulator-up
cd e2e && set -a && . ./.env.e2e.bigquery-emulator && set +a && \
  npx playwright test --project=bigquery-parallel
```

For quick iteration without the full stack, replay a repro directly:

```bash
curl -s -X POST http://localhost:9050/bigquery/v2/projects/test-project/queries \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT COUNTIF(x > 1) FROM (SELECT 2 AS x)", "useLegacySql": false}'
```
