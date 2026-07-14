# Dataset Store & Change Detection — refactor + bug-hunt findings

> Total: 5 findings (Critical: 0, High: 1, Medium: 4, Low: 0)
> Files scanned: `crates/core/src/datasets.rs`, `crates/core/src/storage.rs`, `crates/core/src/simhash.rs` (plus migrations `0002_platform.sql`, `0004_simhash.sql`, `0005_change_intelligence.sql` and callers `crates/core/src/app.rs`, `crates/server/src/{routes,worker}.rs` for confirmation)

## 1. `upsert` is a non-atomic read-modify-write — concurrent writers to the same key corrupt the diff chain and abort batches
- **Severity**: High
- **Lens**: bug-hunter
- **Category**: race-condition / non-atomic-rmw
- **File**: `crates/core/src/datasets.rs:132-203` (SELECT at 142-149, UPDATE/INSERT + `add_revision` at 151-202)
- **Scenario**: `upsert` runs four separate autocommit statements: (1) `SELECT hash, data, removed_at`, then (2) an `UPDATE`/`INSERT` on `records`, then (3) `add_revision`. There is no enclosing transaction. Two jobs of the *same app* can run concurrently (`worker.rs:119-137` — per-app cap is `app_concurrency`/`default_app_concurrency`, configurable above 1), so two `upsert`s can target the same `(app, dataset, key)`:
  - **New-key collision**: both SELECT return `None`; both take the `None` arm and `INSERT`. The second `INSERT` violates the `PRIMARY KEY (app, dataset, key)` (migration `0005`), so `upsert` returns `Err`. Inside `upsert_many` (`:399-414`) that `?` aborts the whole batch mid-loop, after earlier records + revisions were already committed — a partial, inconsistent apply with the summary discarded.
  - **Change diff-chain corruption**: both SELECT read the same base `V0`; each computes `diff_values(V0, Vi)` (`:178-179`) and appends a `changed` revision. The record ends at whichever `UPDATE` lands last, but the history now holds two revisions both diffed *from `V0`* instead of `V0→V1` then `V1→V2`. Any consumer replaying `record_revisions` diffs (the documented "time-travel"/change-feed substrate) reconstructs wrong intermediate states.
- **Root cause**: the check (SELECT) and the act (UPDATE/INSERT + revision) are distinct statements with no transaction, so the row can change underneath between them (classic TOCTOU); each revision's diff base is a stale snapshot rather than the current row.
- **Impact**: corrupted revision/diff history (silent wrong results downstream) and `upsert_many` batch aborts leaving records partially applied.
- **Fix sketch**: wrap the SELECT + write + `add_revision` in a single `BEGIN IMMEDIATE` transaction per key (serializing writers on the row), or make the write an `INSERT ... ON CONFLICT(app,dataset,key) DO UPDATE ... RETURNING` that computes change-kind and diff from the pre-image atomically. At minimum, treat the `INSERT` PK-violation as "someone inserted first — re-read and fall through to the update path" instead of erroring.

## 2. `detect_removed` with an empty `present` set silently tombstones the entire dataset
- **Severity**: Medium
- **Lens**: bug-hunter
- **Category**: edge-case / silent-failure
- **File**: `crates/core/src/datasets.rs:361-396` (reached via `AppContext::sync_many`, `app.rs:283-295`)
- **Scenario**: `sync_many` forwards `present = items.keys()` straight into `detect_removed` (`app.rs:289-292`) with no size guard. If a scraper returns `Ok(vec![])` on a soft failure (source down but HTTP 200, an empty listing page, a parser that yields nothing), `present` is empty, so every live record (`removed_at IS NULL`) fails the `present.contains` check (`:379`) and gets `removed_at` stamped plus a `removed` revision appended (`:382-393`). One transient empty fetch marks the whole dataset removed, emits a `removed` revision per record, and fans out a change-webhook per record.
- **Root cause**: the store can't distinguish "the snapshot is genuinely empty" from "the fetch failed and returned nothing"; there is no floor/threshold on how much of a dataset a single sync may retire, and the per-key loop is also un-transactioned (a crash mid-loop leaves a partial tombstone set).
- **Impact**: mass false removals + `removed`-revision storm + webhook-delivery storm across an entire dataset from one bad scrape (large blast radius).
- **Fix sketch**: refuse (or require an explicit opt-in) when `present.is_empty()`, or cap the fraction of live rows a single `detect_removed` may retire (e.g. abort if it would remove >N% and let the caller confirm); wrap the removals in one transaction so the tombstone set is all-or-nothing.

## 3. `JsonFilter::Gte`/`Lte` silently return wrong results on numeric JSON fields (SQLite numeric-below-text sort)
- **Severity**: Medium
- **Lens**: bug-hunter
- **Category**: edge-case / type-coercion
- **File**: `crates/core/src/datasets.rs:83-86` (variants) and `:578-589` (query build)
- **Scenario**: `Gte`/`Lte` bind `value` as TEXT (`value.as_str()`, `:581`/`:587`) and compare it to `json_extract(data, path)`. When the JSON field holds a *number*, `json_extract` yields an INTEGER/REAL storage-class value; the bound comparand is TEXT. SQLite orders every numeric value below every text value and applies no affinity to an expression result, so `json_extract(numeric) >= 'text'` is **always false** and `<= 'text'` is **always true** — regardless of the actual numeric magnitudes. A caller filtering, say, `amount >= "1000"` via `Gte` gets an empty result set with no error. (Current callers — `routes.rs:2653/2657/2733/2734` — only use `Gte`/`Lte` on `$.close_date` ISO strings, where TEXT-vs-TEXT lexicographic ordering is correct, so this is latent, not currently firing.)
- **Root cause**: text-only comparands make `Gte`/`Lte` correct solely for text/date fields; the `NumGteAny` variant exists *because* numeric comparison needs the `json_type IN ('integer','real')` guard (`:596-598`), but nothing steers or prevents `Gte`/`Lte` from being pointed at a numeric field.
- **Impact**: a future numeric range filter built on `Gte`/`Lte` returns silently empty/complete result sets (wrong data, no signal).
- **Fix sketch**: document `Gte`/`Lte` as text/date-only (already implied) and either add numeric `Gte`/`Lte` variants mirroring `NumGteAny`'s `json_type` guard, or coerce with `CAST(json_extract(...) AS TEXT)` on both sides so the comparison is unambiguously lexicographic.

## 4. Persisted SimHash is derived from `DefaultHasher`, whose output std does not guarantee stable across Rust versions
- **Severity**: Medium
- **Lens**: bug-hunter
- **Category**: silent-failure / edge-case
- **File**: `crates/core/src/simhash.rs:74-78` (`hash_token` via `DefaultHasher`) feeding `simhash`/`simhash_value` (`:13-45`); value stored at `datasets.rs:140` and compared at `duplicate_pairs` (`:441`)
- **Scenario**: the `simhash` column is written once per upsert and later compared *across records hashed at different times*. `hash_token` uses `std::collections::hash_map::DefaultHasher`, whose algorithm the standard library explicitly documents as unspecified and not guaranteed stable across releases. If the binary is rebuilt on a Rust toolchain whose SipHash output differs, records ingested after the upgrade get fingerprints from a different hash function than the ones already stored. `duplicate_pairs` then compares old-hasher fingerprints against new-hasher ones, so genuinely near-duplicate old/new record pairs land at large Hamming distances and are missed — silently — even though the module's doc advertises determinism as a feature.
- **Root cause**: a non-portable, version-unstable hash is persisted and compared over time, rather than a fixed-spec hash (the content hash correctly uses `sha2`; only the simhash path uses the std hasher).
- **Impact**: duplicate detection silently degrades across a Rust/toolchain upgrade until every record is re-hashed; results are non-reproducible across builds.
- **Fix sketch**: replace `DefaultHasher` with an explicit, versioned hash (e.g. a fixed-seed SipHash from a pinned crate, or FNV/xxhash), and/or store a `simhash_version` so mixed-version fingerprints can be detected and re-derived.

## 5. `duplicate_pairs` scans removed (tombstoned) records and is unbounded in both work and result size
- **Severity**: Medium
- **Lens**: bug-hunter + code-refactor
- **Category**: edge-case / unbounded-query
- **File**: `crates/core/src/datasets.rs:420-453` (exposed via HTTP at `routes.rs:1312`; called by `grants-common/src/lib.rs:175`)
- **Scenario**: the query at `:426-431` selects `key, simhash` for the whole `(app, dataset)` partition with **no `removed_at IS NULL` filter and no `LIMIT`**. Two consequences: (a) for datasets that use `sync_many`, tombstoned records are still compared, so dedup reports pairs involving records that no longer exist in the live view (`list`/`list_filtered` exclude them, so the two views disagree); (b) the O(n²) double loop (`:433-450`) and the unbounded `pairs` Vec grow without limit — an HTTP caller hitting `routes.rs:1312` with a large `distance` makes nearly every pair match, allocating ~n²/2 `DupPair`s (each two `String` clones), which can exhaust memory on a large dataset. The `record_count` guard (`:457-465`) is advisory and lives outside this function.
- **Root cause**: the scan trades correctness/bounds for simplicity ("O(n²), fine for local datasets") but neither filters the dead rows nor caps its output, and the guard it relies on is not enforced here.
- **Impact**: wrong/confusing dedup results (dead records surfaced) plus a memory/CPU blow-up path reachable from the HTTP endpoint.
- **Fix sketch**: add `AND removed_at IS NULL` to the SELECT so dedup matches the live view, and take a `max_pairs`/`limit` cap (or enforce the `record_count` bound internally) so both the comparison set and the result Vec are bounded.

---

### Minor (not in the top 5)
- **Duplication**: `ts`, `parse_ts` (and `now`) are byte-identical in `datasets.rs:746-754` and `storage.rs:1462-1474`; worth hoisting into one shared timestamp helper module within `crates/core`.
