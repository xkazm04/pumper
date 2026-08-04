# Dataset store & change intelligence

Persistent, queryable record store (`records` table): apps upsert typed JSON records keyed `(app, dataset, key)`; the store hashes each value (sha256 canonical JSON + 64-bit SimHash) and reports `new | changed | unchanged`.

## Change intelligence

- **Revisions** (`record_revisions`): every New/Changed upsert appends a revision with a **field-level diff** vs the previous snapshot (dot-notation paths, `{"from":…,"to":…}`, root `$`; `diff_values` exported from core). 'Removed' revisions carry no data.
- **Removal detection** (inferred): `AppContext::sync_many` treats the batch as a **full snapshot** — previously-live keys absent from it get `records.removed_at` set + a `removed` revision; reappearing records are revived and reported Changed. `upsert_many` (partial batches) never marks removals — do not conflate them. `detect_removed` refuses an **empty** batch outright (a failed scrape must not tombstone everything), and `sync_many` downgrades itself to `upsert_many` when the source's health state suppresses removals — a *partial* batch is the case the empty-batch guard does not cover.
  - **The health check is a precondition of the store, not a courtesy of the caller.** `Datasets::detect_removed` takes a `RemovalGuard`, and the only public way to obtain one is `RemovalGuard::for_source_state(state)`, which returns `None` for a degrading source. An app that hand-rolls `upsert_many` + removal detection therefore cannot skip the check — it has to ask the health state to get the token. An inventory test (`crates/core/tests/removal_guard.rs`) pins the call sites.
- **Removal by name** (not inferred): `Datasets::tombstone_keys(app, dataset, keys)` tombstones exactly the keys given — same `removed_at` + `removed` revision, same change-feed signal — for callers that already hold the per-record removal fact (the `peer` app applying an origin feed's `removed` revisions). Nothing is inferred from a snapshot, so no guard applies; a caller who wants "everything except this list" wants `sync_many`.
- **APIs**: `GET /datasets/{app}/{ds}/changes?since=&limit=&trust=` (change feed, newest first, diffs included), `GET /datasets/{app}/{ds}/history?key=` (per-record revision trail).

## Trust

Records and revisions carry a `trust` stamp recording how much the write is stood behind: `stable`, `provisional` (written while its source was degrading) or `quarantined`. Stamping comes from extraction health — see [resilient-extraction.md](resilient-extraction.md) — and only happens when `[resilience] enforce = true`.

Stored `NULL` **means** `stable`. That is a semantic default, not a sentinel: every row written before the column existed is correct by construction, so migration 0020 needs no backfill (the lesson from `0004_simhash.sql`, whose `DEFAULT 0` sentinel silently disabled near-dup detection for 3,367 rows). `datasets::trust_label` is the one place that decides the equivalence, and readers must not re-derive it.

Filtering follows push-versus-pull: **pushes suppress, pulls filter**. A webhook cannot be recalled, so watches/triggers are dropped at the source; a pull API is re-readable, so it filters and stays inspectable.

- `GET /datasets/{app}/{ds}/changes?trust=` defaults to **`stable`** — accepts `all`, `provisional`, `quarantined`.
- `GET /datasets/{app}/{ds}?trust=` defaults to **`all`**: each record carries its own stamp, so the raw dataset view stays complete. Honored identically whether or not `filter=` is present — see the read-path unification below.
- `GET /datasets/{app}/{ds}/export?trust=` defaults to **`all`** (a complete copy by default; the stamp rides in the payload) but now honors an explicit value instead of silently ignoring it.
- `GET /grants?trust=` defaults to **`all`**, same vocabulary.

All four read shapes — the plain list, its cursor page, a `filter=`-narrowed read, and `/export` — share one function, `Datasets::list_records_view`, which applies the one shared `TRUST_PREDICATE` (and the tombstone toggle below) so none of them can drift from what the others call "stable" or "live". `list_filtered`/`list_filtered_trust` remain as a separate, narrower entry point used by `/grants` and a handful of apps that only ever want the live, unfiltered-by-tombstone view.

A quarantined source writes to the shadow dataset `<ds>@q`, which is an ordinary dataset — listing, changes, export and duplicates all work on it unchanged.

## Tombstones (`removed_at`)

`GET /datasets/{app}/{ds}` and `.../export` both take **`removed=include|exclude`**, default **`exclude`** — tombstoned records (`removed_at` set) are left out unless asked for.

> **Behavior change.** Before this, the unfiltered `GET /datasets/{app}/{ds}` page (no `cursor=`, no `filter=`) and its cursor-paged form always **included** removed records, while adding `?filter=` silently switched to **excluding** them (and so did `/export`'s json-array format, inconsistently with its own ndjson/csv). A client that started filtering — or started paging — got a materially different dataset with no other change on its end. `removed=` is now the one explicit knob, `exclude` is the one default across every shape, and it matches what `/grants` (built on the same live-only path) already did. Pass `removed=include` to get the old always-included behavior back.

## Querying & export

- `GET /datasets/{app}/{ds}?limit=&cursor=&trust=&removed=` — records newest-updated first; `cursor=` (even empty) switches to `{items, next_cursor}` keyset pagination (`updated_at|key`); absent = legacy bare array.
- `GET /datasets/{app}/{ds}/export?format=json|ndjson|csv&trust=&removed=` — all three formats **stream** in keyset-paged 1000-row batches with content-disposition (CSV: fixed columns key/timestamps/data-as-JSON, RFC-4180 quoted); none is buffered or capped. A mid-stream store failure aborts the HTTP response without its clean terminator (no closing `]` for json) — chunked-encoding client libraries surface that as a transfer error, not a 200 with a plausible-looking short body — and is logged at `error`. A per-row JSON serialization failure is counted and logged at `error` rather than silently skipped.
- `GET /apps/{name}/datasets` — dataset names per app. `GET /datasets/{app}/{ds}/duplicates?distance=` — SimHash near-duplicate pairs.

## Conventions

- Keys are stable external ids (opportunity id, URL, `czisco|kraj|org`). Timestamps are fixed-width RFC 3339 UTC micros (`ts()` helpers) so lexicographic SQL comparison = chronological.
- **Batch writes are set-shaped.** `upsert_many` commits in chunks of 500 on one held connection, and within a chunk issues a bounded number of statements (two batched reads + multi-row writes) rather than one triple per record — the statement count per chunk *is* the write-lock hold time other apps wait on. Consequence for consumers: **every record in one chunk shares one `last_seen`/`updated_at`/revision `created_at` stamp**. Ordered reads already tiebreak that — `/datasets/{app}/{ds}` by `key`, `/changes` by rowid — so paging stays stable; do not rely on records within a batch having distinct timestamps.
- **Virtual namespaces**: several apps may feed one cross-source dataset by passing an explicit app name to `ctx.datasets` (e.g. `grants/unified`, `census/market_blend`, `cz-labour/salary_gap`) with source-prefixed keys.
- Big payloads go to `ctx.save_artifact` (files under `data/artifacts/<app>/<job>/`); records and results stay compact.

## Retention

Everything here is **off by default** and enabled per key under `[storage]`. Each
deletion is data loss of a different kind, and this service runs local-first with
no second operator to ask, so retention is something you turn on — never something
that happens to you. The single `retention_janitor` in `main.rs` (one loop, every
6h) runs all of it.

| Key | Bounds | Scoped so this survives |
| --- | --- | --- |
| `revision_retention_days` | `record_revisions` past the window | the newest `revision_retention_keep_min` revisions of every record |
| `artifact_retention_days` | bodies under `artifacts_dir` past the window | **any body a replayable revision points at**, plus VCR cassettes |
| `artifact_retention_include_cassettes` | (flag) lets retention reclaim `cassette.ndjson` too | — |
| `cost_event_retention_days` | `cost_events` | events of jobs still `queued`/`running` (they back the budget ceiling) |
| `webhook_delivery_retention_days` | `delivered` rows in `webhook_deliveries` | `pending`/`failed` — the live retry queue and the replayable DLQ |
| `webhook_dead_letter_retention_days` | `dead` rows (the exhausted DLQ tail) | as above |
| `job_yield_retention_days` | `job_yield` (backs `GET /economics`) | — |
| `saved_search_seen_retention_days` | `saved_search_seen` | — ⚠ pruning a `seen` row makes an already-alerted doc look new, so a still-matching doc **re-fires its webhook** |

**The pinning rule.** An archived body is reclaimable only when *no replayable
revision points at it* — replayable meaning `artifact_sha` **and** `rules_hash` are
both stamped, i.e. exactly what `POST /provenance/{app}/{ds}/{key}/rederive`
requires. Age proposes; the provenance graph vetoes. Both halves are pinned: the
snapshot a replayable revision carries (where the body was when it was written)
and the record's current `job_id`/`artifact_path` (where re-derivation looks
today, after a crawl revisit moved the body to a new job dir). Without the pin,
retention would quietly turn reproducible records into permanent
`archived body unavailable` answers.

Because pins are held by revisions, config validation rejects
`revision_retention_days < artifact_retention_days` — history pruned first would
un-pin bodies before their own window was up.

**Dry run.** `GET /retention/preview?days=` reports, without deleting anything:
per-app `files`/`bytes` for the artifact tree split into
`reclaimable` / `pinned` / `cassette`, the totals, current row counts of the
append-only ledgers, and the configured windows. `days` defaults to the configured
`artifact_retention_days`, so you can model a window the deployment has not
enabled. The preview and the janitor call the **same** plan builder, so they cannot
disagree. Both walk the whole artifact tree — on-demand only, never a hot path.

## `datasets doctor` — store integrity report

`GET /datasets/doctor?skip_artifacts=` (`just doctor`) — **read-only**. Every query
is a `SELECT`, every filesystem touch is a `stat`; it reports and never repairs,
so you can always tell whether the store was healthy or merely healed. It performs
**full scans** (`record_revisions`, `records`, the whole artifact tree), so it is
an on-demand operator tool — never on a hot path, never on a timer.
`skip_artifacts=true` drops the tree walk and the per-body checks.

**A healthy store returns `findings: []` and `healthy: true`.** Descriptive
numbers that are not problems live outside `findings` (`coverage`, `tables`,
`artifacts`), because a report that always says something gets ignored.

Each finding carries a concrete remediation — the binary to run, the config key
to set, the route to call — never a bare count, plus up to 10 examples.

| Check | Fires when | Remediation |
| --- | --- | --- |
| `missing_artifact_bodies` | a replayable revision's stamped body is not on disk — `rederive` will answer 409 for that key | re-run the producing job; check for manual deletion (retention pins replayable bodies, so it was not retention) |
| `half_stamped_provenance` | a revision stamps exactly one of `artifact_sha` / `rules_hash` | fix the app's write path to stamp both or neither; stamps are never rewritten retroactively |
| `unregistered_rulesets` | a stamped `rules_hash` is absent from `rules_versions` | register at write time (`INSERT OR IGNORE`); unrecoverable rulesets stay non-replayable rather than replayed against today's rules |
| `records_without_simhash` | live records with `simhash = 0`, silently skipped by `/duplicates` | `just reindex` with the server stopped |
| `unbounded_table_growth` | an append-only table has retention off **and** rows older than 180 days | set the named `[storage]` key, confirm with `GET /retention/preview` |
| `orphan_derived_specs` | a derived spec's source dataset holds no records | `POST /derived/{id}/backfill` once it has records, or `DELETE /derived/{id}` |
| `stale_rebuild_tables` | a `*_new` table-rebuild scaffold survived in `sqlite_master` | restore from backup and re-run migrations; do not drop it by hand |

`artifacts.per_app` reports files and bytes on disk per app — the numbers that
make the retention decisions above inspectable.

> **On `triggers_new`:** it is **not** a stale table. Migration 0021 rebuilds
> `triggers` through a `triggers_new` scaffold (SQLite cannot `ALTER` a `CHECK`
> constraint) and `RENAME`s it into place; migrations run in a transaction, so the
> scaffold is never observable afterwards. CRUD correctly targets `triggers`.
> `stale_rebuild_tables` exists to catch a rebuild that genuinely did not land —
> it is empty on every correctly-migrated database, which
> `the_triggers_rebuild_scaffold_does_not_survive_migration` pins.

## Known gaps

- **Duplicate scan** uses banded SimHash bucketing (`simhash::BandedIndex`, shared with the crawler's near-dup gate): candidates come from `distance + 1` contiguous bit-bands and are then verified by exact Hamming, so the pair set, the `MAX_DUP_PAIRS`=10,000 cap and the result ordering are identical to the all-pairs scan it replaced. Bands are `64 / (distance + 1)` bits wide, so **the index turns banding off above distance 5** and verifies against a plain walk — same answers, linear candidate generation. At the distance every real caller uses (3: the `/duplicates` default and grants `link_duplicates`) a 50k-record scan measured **~0.8s vs ~23s** for the all-pairs sweep.
- No Parquet export. `changes_since` scans per app — fine for SQLite scale.
