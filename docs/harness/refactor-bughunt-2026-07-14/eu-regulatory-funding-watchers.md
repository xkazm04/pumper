# EU & Regulatory Funding Watchers — refactor + bug-hunt findings

> Total: 5 findings (Critical: 0, High: 1, Medium: 4, Low: 0)
> Files scanned: `crates/apps/eu-sedia/src/lib.rs`, `crates/apps/cms-fee-schedule/src/lib.rs` (+ confirmed against `crates/apps/grants-gov/src/lib.rs`, `crates/apps/grants-common/src/lib.rs`, `crates/core/src/extract.rs`)

## 1. eu-sedia has no "positive total, zero parsed rows" drift guard — silent empty run on schema drift
- **Severity**: High
- **Lens**: bug-hunter
- **Category**: silent-failure
- **File**: `crates/apps/eu-sedia/src/lib.rs:99-129`
- **Scenario**: SEDIA renames or nests the `results` array (a realistic upstream change; the module note already flags results as "volatile"). Line 107-111 does `parsed.get("results").and_then(Value::as_array).cloned().unwrap_or_default()`, so a missing/renamed array silently becomes `[]`. `got = 0 < page_size` breaks the loop after page 1 (line 123), `records` is empty, and `upsert_many` (line 129) returns Ok with `fetched: 0`. The run is reported as a success while extracting nothing.
- **Root cause**: `total` (`totalResults`) is read at line 102 but never cross-checked against the parsed row count. The sibling app `grants-gov` explicitly guards this exact case (`if hit_count > 0 && hits.is_empty() { return Err(...) }`, grants-gov lib.rs:148-153); eu-sedia omits the equivalent guard entirely.
- **Impact**: A masked upstream break — monitoring sees "success", the `opportunities` dataset silently stops receiving new EU calls. (Because `upsert_many` is used, existing rows aren't wrongly removed, but they go stale and the failure is invisible.)
- **Fix sketch**: After the fetch loop, add `if total > 0 && records.is_empty() { return Err(Error::App("eu-sedia schema drift: totalResults={total} but parsed 0 results")) }`, mirroring grants-gov's guard.

## 2. eu-sedia pagination silently truncates (maxPages cap + missing `totalResults`)
- **Severity**: Medium
- **Lens**: bug-hunter
- **Category**: pagination-truncation
- **File**: `crates/apps/eu-sedia/src/lib.rs:50,101-127`
- **Scenario**: Two truncation paths. (a) `default_params` sets `maxPages: 10, pageSize: 100` (line 50) → a hard cap of 1000 records. A pan-EU open-calls feed (Horizon + Erasmus+ + CERV + LIFE + Digital, per the module header) can plausibly exceed 1000 open topics; when `pages_fetched >= max_pages` fires (line 123) the run stops and reports `totalResults: 1500, fetched: 1000` with no warning that it was capped. (b) If `totalResults` is absent/renamed but `results` still returns full pages, `total` defaults to 0 (line 102) so `(pages_fetched * page_size) >= total` → `100 >= 0` is true after page 1, capturing only the first 100 records.
- **Root cause**: The loop trusts `total` (an optionally-present count) as a termination signal, and the default page budget is half the sibling's (grants-gov defaults `maxPages: 25`). Neither cap surfaces a warning, unlike the `grants-common::drift_warnings` path the other grant apps emit.
- **Impact**: Wrong/incomplete dataset — records beyond the cap are never upserted and go stale (not removed, since `upsert_many` is partial), and the truncation is invisible to consumers.
- **Fix sketch**: Treat `got < page_size` as the authoritative stop (don't terminate on a possibly-absent `total`); raise the default `maxPages`; and push a warning into the result when `fetched < total` or when the `maxPages` cap is hit.

## 3. cms-fee-schedule can fabricate a bogus "latest" release from an unbounded `rvuNN[a-d]` substring match
- **Severity**: Medium
- **Lens**: bug-hunter
- **Category**: parse-edge-case
- **File**: `crates/apps/cms-fee-schedule/src/lib.rs:94-120`
- **Scenario**: `detect_releases` scans the lowercased page for any `"rvu"` occurrence and accepts the next three bytes as `digit digit [a-d]` (lines 100-113) with no boundary check before or after `rvu`. A CMS page is large and script-heavy; any incidental token such as `srvu99a`, a cache-buster, analytics id, or CSS class containing `rvu99b` matches. `latest()` (line 118-120) then picks it as the newest by year (2099 > real years), so the app reports `latest_release: "RVU99B"`, a 404 `zip_url`, and `is_newer_than_known: true`.
- **Root cause**: Token detection has no word/href boundary and no plausible-year sanity bound; it matches the shape anywhere in the blob.
- **Impact**: False freshness signal → a spurious "newer release available" that can drive a needless (and failing) `ingest-cms-pfs.mjs` run pointed at a non-existent ZIP.
- **Fix sketch**: Require `rvu` to be preceded by a non-alphanumeric byte (or restrict to `href="…/rvuNNx"` matches), and/or bound `year` to a sane window (e.g. `2015..=current+1`).

## 4. eu-sedia emits an empty-string key when both `identifier` and `reference` are absent
- **Severity**: Medium
- **Lens**: bug-hunter
- **Category**: edge-case / key-collision
- **File**: `crates/apps/eu-sedia/src/lib.rs:163-196`
- **Scenario**: `identifier = first(&m, "identifier").unwrap_or(reference)` (line 166) with `reference` defaulting to `""` (line 165). A hit lacking both `metadata.identifier` and top-level `reference` (plausible for `type=2` PROSPECT entries, which carry a different metadata shape) yields key `""`. Multiple such hits all map to key `""` in `upsert_many`, so all but the last are dropped, and a junk row keyed `""` is written into `opportunities`.
- **Root cause**: The fallback chain never fails closed — it always produces a key, even an empty one, instead of skipping unkeyable hits.
- **Impact**: Dataset corruption (records silently collapse onto one empty key; a meaningless `""` record persists).
- **Fix sketch**: Change `normalize` to return `Option<(String, Value)>` and `filter_map` the hits, dropping any hit whose `identifier`/`reference` is empty (as `grants-common::normalize_*` already do via `str_of(...)?`).

## 5. eu-sedia is not wired into the cross-source `grants/unified` layer
- **Severity**: Medium
- **Lens**: code-refactor
- **Category**: coverage-gap / duplication
- **File**: `crates/apps/eu-sedia/src/lib.rs:21,129` (vs `crates/apps/grants-common/src/lib.rs:57-93`, `grants-gov` lib.rs:176-185)
- **Scenario**: `grants-common` exists so every grant-source app normalizes into ONE canonical `grants/unified` dataset (search, dedup, deadline digest, `sweep_closed`, `drift_warnings`). `grants-gov` and `ca-grants` call `sync_unified` / `sweep_closed` / `link_duplicates` / `drift_warnings`. eu-sedia does not `use grants_common` at all (line 21) and only writes its raw `opportunities` dataset (line 129); there is no `normalize_eu_sedia` in `grants-common`.
- **Root cause**: The EU source re-implements the "raw opportunities" half of the pattern without the shared canonical half — either an unfinished integration or an undocumented deliberate exclusion.
- **Impact**: EU grants are invisible to cross-source search/dedup/closing-soon digests, and the app carries the same "own opportunities dataset" boilerplate as its siblings without their shared benefits (wasted maintenance + missing coverage).
- **Fix sketch**: Add `normalize_eu_sedia` to `grants-common` and call `sync_unified` + `drift_warnings` from eu-sedia's `run`, or add a one-line rationale in the module header documenting why EU is intentionally excluded.
