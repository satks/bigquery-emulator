# SignalSmith E2E — Gap Fixes

> **Status update (2026-06-11):** Gaps **#1–#6 fixed** (commits `eb8cb48`..`c13f08f`);
> every repro below re-verified live via `POST /queries`. #7 root-caused and fixed 2026-06-11
> (GENERATE_UUID → DuckDB UUID/INT128 in CTAS; no stack needed — repro built from the signalsmith compiler source).
> A pre-existing translator bug was found and fixed along the way:
> `translateFunctions` looped forever on self-matching output
> (`STARTS_WITH→starts_with`; see `.wolf/buglog.json` bug-050).
>
> **Re-run #1 (2026-06-11, emulator ref `c12cbfc` — i.e. WITHOUT the #7 fix):**
> `bigquery-parallel` went **388 → 426 passed**, **30 → 21 failed**, 124 → 94 skipped,
> and 2× faster (6.8m → 3.4m). All six round-1 repros verified live. The remaining
> failures are a **second round of gaps — see "Round 2" below** (#8–#12, pinned from
> API logs + live replays). #7 (INT128) still reproduced in that run, as expected —
> its fix landed in `1c381eb`, after `c12cbfc`.
>
> **Re-run #2 (2026-06-11, emulator ref `1c381eb` — WITH the #7 fix):**
> **465 passed / 19 failed / 57 skipped (2.8m)**. INT128 is fully gone — tests 18 & 34
> pass, unlocking their serial dependents (+39 passing vs re-run #1). Every remaining
> failure is attributed: #9 TIMESTAMP_ADD (journey cluster, 12 of 19), #8 MERGE (06/08/35),
> #10 IGNORE NULLS (10-traits), #11 UNNEST alias (11-filters), #12 map[] serialization
> (27 AB-assignments), #13 JS UDF — structural (31-field-transforms), + 3 needs-investigation.
> Cumulative: **388 → 465 passed (+77), 30 → 19 failed, 6.8m → 2.8m (2.4×)**.

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

## 7. ✅ FIXED — Golden-record reads: `Could not convert string '…==' to INT128`

| | |
|---|---|
| **Signature** | `Conversion Error: Could not convert string 'KBsWxukaRtatKAjU9XT08w==' to INT128` (signalsmith tests 18, 34) |
| **Root cause (found 2026-06-11)** | The "computed in Go" assumption was wrong: `ss_id` is generated **in SQL** — `GENERATE_UUID() AS ss_id` inside the `_IDENTITY_GRAPH` CTAS (`internal/compiler/identity_compiler.go:631`). The emulator mapped `GENERATE_UUID → uuid`, and DuckDB's `uuid()` returns the UUID type, which is **physically INT128**. CTAS inferred `ss_id` as UUID; reading it back yielded 16 raw bytes that `formatValue` base64-encoded (the mystery `KBsWxukaRtatKAjU9XT08w==` decodes to exactly 16 bytes = a UUID). The test then interpolated that base64 string into `WHERE ss_id = '…'`, and DuckDB failed to cast it to INT128. |
| **Fix** | `GENERATE_UUID()` now translates to `CAST(uuid() AS VARCHAR)` (`pkg/query/functions.go`), so the column is STRING like BigQuery, values render as normal UUID strings, and literal lookups work. Repro verified live end-to-end; regression test `TestIntegration_GenerateUUID_StringRoundtrip`. |

---

# Round 2 — found after the `c12cbfc` re-run (2026-06-11)

> **Status (2026-06-11, commits `b977893`..`a8de98e`):** #8–#12 fixed; #13 now
> fails fast with a clear "JavaScript UDFs ... not supported" error (option (a),
> permanent limitation). Root-cause notes: #8 was the MERGE *parser* — aliases
> without `AS` + nested parens in the USING subquery broke the single-regex
> header (native MERGE confirmed unavailable in the bundled DuckDB; decomposition
> kept). #12 was DuckDB's json extension scanning JSON columns as Go maps. Also
> mapped `PARSE_JSON → json` (AB-assignment write path). All shapes guarded by
> `TestIntegration_Round2GapRepros`. Pending suite re-run.

The 21 remaining failures cluster into five new emulator gaps. All pinned from
SignalSmith API logs + live replays against the running emulator.

## 8. ✅ FIXED — `MERGE` statements not translated

| | |
|---|---|
| **Signature** | `SQL translation error: cannot parse MERGE statement` (API WARN: `audit flush failed (best-effort, continuing)` — repeats every flush) |
| **Impact** | Sync-audit `sync_snapshot` never gets rows → tests 06, 08, 35 time out waiting for snapshot state. Best-effort, so syncs themselves pass. |
| **Fix** | Translate BigQuery `MERGE INTO … WHEN MATCHED/NOT MATCHED` — DuckDB ≥1.4 has native `MERGE INTO`; if the bundled go-duckdb is older, rewrite as `DELETE … USING` + `INSERT … SELECT`. |

## 9. ✅ FIXED — `TIMESTAMP_ADD` (and the date/time `_ADD/_SUB` family) missing

| | |
|---|---|
| **Signature** | `Catalog Error: Scalar Function with name timestamp_add does not exist` (12× — journey `time_delay` + `hold_until` tiles) |
| **Impact** | Most of the journey cluster: 25, 27 (run status `failed`). |
| **Fix** | Translate `TIMESTAMP_ADD(ts, INTERVAL n unit)` → `ts + INTERVAL (n) unit`. Audit the whole family: `TIMESTAMP_SUB`, `DATE_ADD/SUB`, `DATETIME_ADD/SUB`, `TIME_ADD/SUB`. Note the `INTERVAL n unit` arg is special syntax, not a value — needs a translator rule, not a macro. |

## 10. ✅ FIXED — `IGNORE NULLS` inside `ARRAY_AGG`

| | |
|---|---|
| **Signature** | `Parser Error: syntax error at or near "ORDER"` in trait materialization |
| **Evidence** | `ARRAY_AGG(STRUCT(…) IGNORE NULLS ORDER BY _ranked._rn)` — DuckDB only allows `IGNORE NULLS` in window functions, so the parse dies at the token after it. |
| **Impact** | Trait evaluation pipeline (10, 36, 37 — `poll trait run to completion`). |
| **Fix** | Strip `IGNORE NULLS` from aggregate args (BigQuery STRUCT args are never NULL here, so it's a no-op); for scalar-arg fidelity translate to `… FILTER (WHERE arg IS NOT NULL)`. Also handle `RESPECT NULLS` and BigQuery's `LIMIT n` inside aggregates while in there. |

## 11. ✅ FIXED — `UNNEST(arr) AS alias` aliases the TABLE in DuckDB, the ELEMENT in BigQuery

| | |
|---|---|
| **Signature** | `Conversion Error: Type VARCHAR with value 'vip' can't be cast to the destination type STRUCT` on `… FROM UNNEST(parent."tags") AS _el WHERE _el = 'vip'` |
| **Impact** | Array audience filters (11: contains_any/contains_all/…); kills audience estimates that use them. |
| **Fix** | Translate `UNNEST(x) AS alias` → `UNNEST(x) AS _t(alias)` (table alias + column alias) so the bare alias binds to the element. Watch BigQuery's `WITH OFFSET` variant. |

## 12. ✅ FIXED — STRUCT/MAP cell values serialized as Go `fmt` strings

| | |
|---|---|
| **Signature** | Tests receive literal `"map[]"` / `"map[a:1 b:x]"` — e.g. `SyntaxError: Unexpected token 'm', "map[]" is not valid JSON` parsing `ab_assignments` |
| **Impact** | Journey context/memory/AB-assignment assertions (27 cluster). |
| **Fix** | In the result encoder (`formatValue` — same layer as the #7 fix): composite values must follow the BigQuery wire format — STRUCT → nested `{"v":{"f":[…]}}`, JSON-typed columns → JSON text. Never `fmt.Sprintf` a Go map. |

## 13. 🚧 STRUCTURAL (clean error since `b977893`) — JavaScript UDFs (`CREATE FUNCTION … LANGUAGE js`)

| | |
|---|---|
| **Signature** | 31-field-transforms `sync with JS custom-code transform pushes down…` times out waiting for webhook output. |
| **Root cause** | SignalSmith pushes JS custom-code transforms down as BigQuery JS UDFs (`internal/worker/pipeline.go:1616`: `CREATE OR REPLACE FUNCTION … LANGUAGE js AS r"""…"""`). DuckDB has no JavaScript engine — this is not a translation gap. |
| **Options** | (a) document as a permanent emulator limitation; (b) embed a JS interpreter (e.g. `goja`) and execute the UDF per-row in Go — significant work, exact-fidelity risk. Recommend (a) for now. |

## Needs investigation (may be product/test-side, 1× each)

- `ab_split branch percentages sum to 60, must equal 100` (27) — API-side validation; check whether an emulator misread feeds it.
- Canvas `priority node output cannot be used as a sub-filter` (43 I.4) — product validation, possibly a pre-existing test/product mismatch.
- 38-audience-overlap `correct counts` assertion — COUNTIF now works; recount after #11 lands (overlap inputs use array filters).

---

## Suggested order (impact-per-effort)

Round 1 (all fixed): **#1** hyphenated projects → **#3/#4** COUNTIF + FARM_FINGERPRINT → **#2** `* EXCEPT` → **#5** STRUCT → **#6** MD5→BLOB → **#7** GENERATE_UUID.

Round 2: **#9 TIMESTAMP_ADD** (12 errors, biggest cluster) → **#8 MERGE** (3 audit tests) → **#10 IGNORE NULLS** (3 trait tests) → **#11 UNNEST alias** (array filters) → **#12 STRUCT serialization** (journey context assertions).

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
