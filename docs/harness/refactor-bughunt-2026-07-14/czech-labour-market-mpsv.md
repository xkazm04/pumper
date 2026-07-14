# Czech Labour Market (MPSV) — refactor + bug-hunt findings

> Total: 5 findings (Critical: 1, High: 1, Medium: 2, Low: 1)
> Files scanned: `crates/apps/mpsv-vpm/src/lib.rs`, `crates/apps/mpsv-ispv/src/lib.rs`, `crates/core/src/extract.rs` (confirm), `crates/core/src/datasets.rs` (confirm `history` ordering)

## 1. Monthly salaries silently discarded because `is_monthly()` string-matches an unverified coded field
- **Severity**: Critical
- **Lens**: bug-hunter
- **Category**: silent-failure
- **File**: `crates/apps/mpsv-vpm/src/lib.rs:806-827` (`is_monthly` 806-812, `monthly_salary_point` 816-827; field decl 707)
- **Scenario**: `monthly_salary_point()` returns a value only if `is_monthly()` is true, and `is_monthly()` requires `self.typMzdy.id` to *contain the substring* `"mesic"`. Every other `IdRef` in the same `Posting` struct is a codebook URI of the form `"Name/id"` — `profeseCzIsco` = `"CzIsco/93291"`, `kraj` = `"Kraj/108"` (see the struct and the verified source-contract comment at lines 22-27). By that same convention `typMzdy.id` is almost certainly `"TypMzdy/<n>"`, which does **not** contain `"mesic"`. When that holds, `is_monthly()` returns `false` for *every* posting, `monthly_salary_point()` returns `None` for every posting, and `Cell.add(None)` bumps `count` but never pushes a salary. Result: `salaryCount` is 0 in **every** `role_region_agg` / `region_agg` cell, every percentile (`salaryMin/P25/Median/P75/Max`) is `null`, and `salary_gap` is skipped (all `gap_cells` have `< min_count` salaries). The run reports success with healthy posting counts while the app's entire reason to exist — reference salary distributions — is empty.
- **Root cause**: The salary source fields are `mesicniMzdaOd` / `mesicniMzdaDo` — literally "monthly wage from/to", already monthly by definition. The extra `typMzdy.contains("mesic")` gate was added on an assumption about a field that the "verified 2026-07-05" source contract does not even list, and that contradicts the codebook-URI shape of every sibling field.
- **Impact**: wrong value / empty-as-success — total silent loss of the salary distribution product (and the downstream posted-vs-official gap), with no error surfaced.
- **Fix sketch**: Drop the `typMzdy` substring gate; trust the explicitly-monthly `mesicniMzda*` fields and keep only the sanity band (`SALARY_MIN..=SALARY_MAX`). If hourly rows must be excluded, match the actual `typMzdy` codebook id (e.g. equals the "hodinová" code) rather than a text substring, and add a fixture-backed test.

## 2. Region roll-ups drop every posting that lacks a CZ-ISCO code, biasing the "true regional distribution"
- **Severity**: High
- **Lens**: bug-hunter
- **Category**: silent-failure
- **File**: `crates/apps/mpsv-vpm/src/lib.rs:192-222`
- **Scenario**: The main loop does `let czisco = match p.czisco() { Some(c) => c, None => continue };` at line 192-195 — an early `continue` for any posting with no `profeseCzIsco.id`. The `regions` roll-ups (lines 214-222) are computed *after* that `continue`, even though they key only on `(krajId, orgType)` / `(krajId, "all")` and never use the occupation code. So a posting that has a valid `kraj` and a valid monthly salary but no CZ-ISCO classification is excluded from `region_agg` entirely. The `region_agg` dataset is documented (lines 165-166) as "the true regional salary distribution powering the locality map headline", yet it silently omits an entire class of real vacancies.
- **Root cause**: Occupation-independent aggregates (region roll-ups) are placed downstream of an occupation-required guard clause. Only the `cells` / `gap_cells` / sample products actually need `czisco`.
- **Impact**: wrong value — regional salary distributions and posting counts are undercounted/biased by the share of unclassified postings (open-vacancy feeds routinely have some). Magnitude scales with that share; the headline is silently skewed low if unclassified postings differ in pay.
- **Fix sketch**: Accumulate the `regions` roll-ups *before* the `czisco` `continue` (they don't need the code), or restructure so only the occupation-keyed products are gated on `czisco.is_some()`.

## 3. `mpsv-ispv` (and the vpm feed) treat a missing/empty `polozky` as a successful zero-row run
- **Severity**: Medium
- **Lens**: bug-hunter
- **Category**: silent-failure
- **File**: `crates/apps/mpsv-ispv/src/lib.rs:55-84` (esp. 55-59); parallel in `crates/apps/mpsv-vpm/src/lib.rs:680-682,144`
- **Scenario**: After the HTTP-status check, ISPV does `parsed.get("polozky").and_then(Value::as_array).cloned().unwrap_or_default()`. If the feed returns HTTP 200 with a drifted shape (key renamed, wrapped under another object, or `{}`), `rows` becomes an empty `Vec`, `items` is empty, and `upsert_many("wages", &[])` is a no-op that returns `stored: 0, new: 0` — reported as success. Because `upsert_many` is a *partial* upsert (per project convention), the stale `wages` rows are retained, so there's no data-loss alarm either. The cross-app consumer (`mpsv-vpm`'s `salary_gap`, lines 349-352) then reads those stale/empty official wages and quietly skips or benchmarks against outdated data. The vpm feed has the same posture via `#[serde(default)] polozky` (line 680-681): a shape change yields `total = 0` and a clean "success".
- **Root cause**: Absence of a shape/plausibility assertion after a successful HTTP fetch — "parsed OK" is conflated with "contains data".
- **Impact**: silent staleness/degradation of the salary-calibration anchor and the posted-vs-official gap; operators see green runs.
- **Fix sketch**: Treat a missing `polozky` key (as opposed to a present-but-empty array) as an `Error::App`, or fail/warn loudly when `rows.len()` is 0 or collapses far below the prior run's count.

## 4. `official_wage_index` reads ISPV salary stats with `as_f64()` only — no fallback for string-encoded numbers
- **Severity**: Medium
- **Lens**: bug-hunter
- **Category**: number-parse
- **File**: `crates/apps/mpsv-vpm/src/lib.rs:534-549` (call site 349-352); source rows built verbatim in `crates/apps/mpsv-ispv/src/lib.rs:65-72`
- **Scenario**: `mpsv-ispv` stores each source row **verbatim** (`r.clone()` at line 70) with no numeric coercion. `mpsv-vpm.official_wage_index` then reads `medianMzda`/`mzdaPrumer` strictly via `Value::as_f64()` (lines 541-545), which returns `None` for any JSON *string*. If the ISPV source ever encodes these figures as strings — plain (`"111959"`) or Czech-formatted with a decimal comma / thin-space thousands (`"111 959,00"`) — `as_f64()` yields `None`, the row is dropped by the `let Some(median) = … else { continue }`, and if enough rows drop the whole index is empty, so `salary_gap` is skipped entirely (lines 351-352) with a benign-looking "no official ISPV wages" message. There is no safety net anywhere on this path because neither app normalizes numbers (unlike `core::extract::to_number`, which the JSON-typed apps never invoke). Note `to_number`/`parse_first_number` would itself mis-handle a Czech decimal comma (treats `,` as a thousands separator), so it is not a drop-in fix.
- **Root cause**: A strict JSON-number assumption on cross-app data that is persisted un-coerced, combined with a silent per-row `continue` and a silent whole-product skip when the index is empty.
- **Impact**: potential total, silent disappearance of the `cz-labour/salary_gap` benchmark if upstream encoding drifts to strings; failure mode is invisible (reported as "skipped, run mpsv-ispv first").
- **Fix sketch**: Parse `medianMzda`/`mzdaPrumer` through a numeric helper that accepts both JSON numbers and Czech-formatted numeric strings (strip spaces/NBSP, comma→dot), and warn (not silently `continue`) when a present-but-unparseable value is seen.

## 5. `Cell::to_value` and `Cell::to_region_value` are near-duplicate percentile serializers
- **Severity**: Low
- **Lens**: code-refactor
- **Category**: duplication
- **File**: `crates/apps/mpsv-vpm/src/lib.rs:905-934`
- **Scenario**: `to_value` (905-919) and `to_region_value` (921-934) are byte-for-byte identical except that `to_value` also emits `"czIsco"`. Both recompute the same `stats()` sort + the same five percentile fields (`salaryMin/P25/Median/P75/Max`, `count`, `salaryCount`). Any change to the salary-distribution shape (e.g. adding P10/P90, changing rounding) must be edited in two places and kept in sync. (Cross-app note: the `czIsco|sfera` key + `sphere_for_org` logic is also split between the two apps, but at different granularities — ISPV keys the raw code, vpm re-derives the 4-digit `unit_group` — so that pair is *not* cleanly extractable and is best left as-is.)
- **Root cause**: The regional variant was copied from the occupation variant and diverged only by one field.
- **Impact**: wasted maintenance / drift risk in the salary-distribution serialization.
- **Fix sketch**: Extract a single `salary_distribution(&self) -> serde_json::Map`/helper that emits `count`, `salaryCount`, and the five percentiles; have both callers spread it and add their extra key(s) (`czIsco`, `krajId`, `orgType`).
