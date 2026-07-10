# Vision Scan Fix Wave 1 — Activate Change Intelligence

> 5 commits, 5 ideas closed (theme T1: change-feeds & diff products).
> Baseline preserved: build clean → build clean; tests 19 → 31 (+12, all passing, 0 failed across 54 suites).

## Commits

| # | Commit | Idea | Title |
|---|---|---|---|
| 1 | `1e1d336` | c738f8b5 | Versioned record history with field-level diffs |
| 2 | `6e68382` | 15724e65 | Detect disappeared records via full-snapshot sync |
| 3 | `0e774c5` | cc719161 | Change-triggered webhooks via dataset watches |
| 4 | `071c161` | Generic scheduled change-watch app (`watch`) | 516e407d |
| 5 | `bd06fca` | 710594e3 | Closing-soon deadline digest for open grants |

## What was built (one vertical, bottom-up)

1. **Revision substrate** (`record_revisions` table, migration 0005): every New/Changed upsert appends a revision carrying a field-level JSON diff (dot-notation paths, `{from,to}` pairs, root = `$`). APIs: `GET /datasets/{app}/{ds}/changes?since=&limit=` (feed) and `GET /datasets/{app}/{ds}/history?key=` (per-record trail). Pure `diff_values()` exported from core with 3 unit tests.
2. **Removal detection**: `records.removed_at` + `AppContext::sync_many` (upsert batch, then mark absent keys removed with a 'removed' revision; reappearing records are revived and reported Changed). `UpsertSummary.removed`.
3. **Dataset watches** (`watches` table, migration 0006): standing subscriptions (`app` + `dataset` or `*`). After a successful job, the worker groups that run's revisions by dataset and fires signed `dataset.changed` webhooks (same HMAC-SHA256 + 3-retry contract as job callbacks). CRUD: `GET/POST /watches`, `DELETE /watches/{id}`, `POST /watches/{id}/enabled`.
4. **`watch` app**: generic Visualping-style URL monitor — tiered fetch → markdown → compact fingerprint (title/chars/sha256/600-char excerpt) upserted into `pages` keyed by URL; job result includes ChangeKind + the field diff. Compose with `/schedules` + `/watches` for scheduled monitors with webhook alerts.
5. **Grants digest**: grants-gov runs now emit `closingSoon` (posted opps closing within `digestDays`, default 14, soonest-first, daysLeft computed; top 25 in result, full list as `closing_soon.json` artifact). Tolerates `MM/DD/YYYY` and ISO dates.

## Patterns established

1. **Revision-on-upsert** — change intelligence belongs in the storage substrate, not per-app: one `add_revision` hook in `upsert` gave every existing app history/diffs for free.
2. **Snapshot-vs-stream upsert split** — `upsert_many` (partial batches) never marks removals; `sync_many` (full snapshots) does. Conflating them would fabricate removals from filtered scrapes.
3. **Compact fingerprint records for page monitors** — diff on `{title, chars, hash, excerpt}` not the raw markdown blob; full content goes to artifacts. Keeps revisions small and diffs readable.

## What remains (INDEX themes)

T2 cost/budget spine (top-scored idea: fetch-tier cost ledger), T4 search activation, T6 crawler maturity, T7 API surface hardening, T5 AI-assisted extraction, T9 domain data products, T10 platform plays.

## Follow-ups from this wave

- `FetchRequest` has no `no_cache` — the `watch` app sees the HTTP cache's TTL'd body; a monitor wants a bypass flag (small core change).
- Unified text diff (line-level) for the `watch` app excerpt would be more readable than field diff on long excerpts.
- `changes_since` scans by `created_at DESC` per app; fine for SQLite scale, revisit if feeds grow.
