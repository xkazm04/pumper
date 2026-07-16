# Perf-Feature Scan — Fix Wave 1: Grants Coverage & Truth

> 4 commits, 4 findings closed (theme E) + 1 deferred-with-reason.
> Baseline preserved: build clean, tests **211 → 216** (0 regressions).
> Branch `vibeman/perf-feature-2026-07-16` (continues after Wave 3). Not pushed.

## Theme

The `grants/unified` corpus is the most product-shaped surface pumper has, and
Wave 1 makes it stop lying: the pan-EU source was invisible, the two US sources
silently truncated their corpus, and the cross-source closing-soon view returned
an arbitrary slice instead of the soonest deadlines. Ordered truth-first: close
the Critical, then coverage, then the query surface.

## Commits

| # | Commit | Finding | Severity | Files |
|---|---|---|---|---|
| 1 | `46e7e01` | eu-sedia #1 — unwired from grants/unified | **Critical** | grants-common, eu-sedia (+Cargo), routes.rs doc, apps.md |
| 2 | `496d62a` | grants-gov #2 (+ca-grants) — page cap & silent truncation | High | grants-gov, ca-grants |
| 3 | `4228588` | eu-sedia #2 — non-deterministic paging | High | eu-sedia |
| 4 | `aa8d541` | http-api-routes #1 — closing-soon wrong-column order | High | datasets.rs, routes.rs, tests |

## What was fixed

1. **eu-sedia joins grants/unified (the Critical).** eu-sedia predated the unified
   layer and was never backfilled — no `grants-common` dep, no `finalize_unified`
   call, no `normalize_eu_sedia`. The largest, most differentiated source (27
   member states, up to ~1000–5000 topics/run) contributed **zero** rows to
   `grants/unified`, so it was invisible to `GET /grants`, `closing-soon`,
   `sweep_closed`, cross-source SimHash dedup, and per-opportunity search. Added
   `grants_common::normalize_eu_sedia` (from the eu-sedia app's already-cleaned
   `opportunities` record) + the `finalize_unified` call. **Three corruption traps
   the naive wiring would have hit, all handled + tested:**
   - status is a **numeric code** (`31094502`/`31094501`); `norm_status` passes
     unknowns through, so a naive map would write the literal digits into `status`
     and break every `?status=open` filter and the sweep predicate. Mapped
     explicitly; unknown → `Null`.
   - `budgetOverview` is **EUR** and unified has no currency dimension → money
     stays `Null` (per user decision), never filed as USD.
   - `deadlineDate` is a **multi-stage array** → `close_date` resolves to the
     earliest still-upcoming cutoff (else the latest), not `[0]`, so a two-stage
     call isn't flipped `closed` when its first cutoff passes.

2. **US sources: page size 1000, honest truncation.** grants-gov and ca-grants
   defaulted to 100-row pages against 1000-max APIs — 25 round-trips per sync and
   a silent 2,500-record ceiling. Raised the default to 1000 (3 round-trips, 25k
   ceiling) and made the cap honest: `truncated:true` + a coverage warning when
   the run stops on `maxPages` with records remaining, instead of returning `Ok`
   like a full sweep. The warning is appended after `merge_into` (which owns
   `warnings`).

3. **eu-sedia paging: flag it, cover it, don't guess it.** SEDIA's match-all has
   no stable sort, so its window is non-deterministic; the old `maxPages=10`
   (1000 topics) let topics drift in and out between runs, producing phantom
   `new` rows and stale-frozen topics. Added `truncated` + warning, raised the
   default `maxPages` to 50 (5000 topics) so the corpus fits in practice, and did
   **not** invent a `sortBy` param — SEDIA exposes no stable sort we could verify
   against the saved `page1.json`. Also stopped cloning the whole `results` array
   (tens of KB of HTML per hit) each page.

4. **closing-soon ordered in SQL.** `GET /grants/closing-soon` pulled 1000 rows
   ordered by `updated_at DESC`, then sorted by `close_date` in memory — so the
   LIMIT chose the rows before the date sort, and past 1000 matches a grant
   closing tomorrow was dropped if its record was stale; `count` saturated at the
   cap. Added `list_filtered_ordered` (ORDER BY a JSON path ASC + LIMIT in one
   pass) and `count_filtered` (true window total), sharing the extracted
   `push_json_filters` predicate builder. `list_filtered` is unchanged.

## Deferred (with reason)

- **grants-gov #1 — federal money enrichment via `fetchOpportunity`** (High).
  Deferred deliberately: (a) it writes code against the `fetchOpportunity`
  response shape, which cannot be verified without a live call — the same
  "don't guess the upstream contract" caution the reports raise for the SEDIA
  sort param; and (b) the "spread the backfill across days" cap stalls unless a
  backfill-drain (re-enrich money-null unified rows) is designed, which also
  wants a live probe to size. Should be picked up once a saved `fetchOpportunity`
  artifact confirms the field names + nesting. Value is real (money is the field
  users sort on) — it's a verification blocker, not a priority call.

## Verification

| Gate | Before Wave 1 | After Wave 1 |
|---|---|---|
| `cargo build --workspace` | clean | clean |
| `cargo test --workspace` | 211 / 0 | 216 / 0 |

New tests: 5 eu-sedia normalization (status-code map, EUR→Null, multi-stage
deadline earliest-upcoming, unknown-code→Null, no-id skip) + 1 datasets
integration (`list_filtered_ordered` returns the soonest rows past the cap;
`count_filtered` reports the true total).

## Patterns established (catalogue additions)

5. **Numeric status codes vs a word vocabulary.** When a source encodes status as
   opaque codes and the canonical normalizer passes unknowns through, an explicit
   code→word map is mandatory — a pass-through silently poisons every predicate
   built on the canonical value (`?status=open`, the sweep).
6. **Currency without a dimension → Null, not a number.** Money from a
   different-currency source has no honest home in a currency-less schema; Null is
   correct, a converted-or-raw number is a money-truth bug (cf. the EUR-as-USD
   class from grant-writing-nonprofits).
7. **LIMIT before ORDER is a silent wrong-answer.** Any "top-N by a data field"
   must sort in SQL before the LIMIT; an in-memory sort after a smaller LIMIT only
   reorders the wrong subset. Pair the capped list with a separate COUNT so the
   total isn't the cap.
8. **Don't invent an upstream param to fix pagination.** When a stable sort key
   can't be verified against a saved response, widen the page budget + flag
   truncation instead of guessing a `sortBy` name that may silently do nothing.

## What remains (per the INDEX)

- **Deferred this wave:** grants-gov #1 (money enrichment, needs live shape),
  eu-sedia #3 (CMS self-baselining watcher, Medium), grants-gov #3 (run the
  corpus-global unified tail once per sync — now 3× with eu-sedia added; the cheap
  mitigation is a `list_filtered` `status IN (open,forecasted)` predicate on
  `sweep_closed`).
- **Next highest-value wave:** Wave 2 — Write amplification (full-dataset reindex
  per job, per-record `upsert_many`/`detect_removed` transactions, quadratic crawl
  checkpoint). Grants #3 folds naturally in there.
- Standalone bug (out-of-lens): crawl `artifact_name` resets to 0 on checkpoint
  resume → overwrites prior `page-NNNN.html`. Fix regardless of wave.
