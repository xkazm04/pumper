---
name: maintenance-tooling
type: perfect/context
group: Job Orchestration
category: config
opportunity: 4
last_proposed: 2026-08-13
cooldown_until: round 19
directions: ["[[doc-sync-hook-fires]]", "[[backfill-purges-ghosts]]", "[[doctor-sees-search]]"]
---

## Current state (scout brief, 2026-08-13 — "very thorough"; supersedes the r11 medium scout)

Surface: `crates/server/src/bin/{reindex,search-backfill}.rs`, `scripts/docs/check-doc-sync.mjs`,
the `justfile` (19 recipes), `scripts/smoke.ps1` (736 lines), and four read-only operational
routes (`/datasets/doctor`, `/retention/preview`, `/enforcement/preview`,
`/datahub/governance/preview` — all verified write-free by reading the handler bodies, not the
names).

**The one genuinely well-designed piece:** `/retention/preview` shares its exact plan builder with
the destructive janitor, so preview and reality cannot disagree.

**Credit where due** (these DO report honestly): backfill prints indexed + purged + resulting
`doc_count`; `/datasets/doctor` publishes `bodies_checked` vs `check_limit` so a bounded check
says it was bounded; `/enforcement/preview` reports `unjudged_runs` rather than crediting them;
`/datahub/governance/preview` collects `read_errors` instead of going dark on one bad URN.

**`reindex` is safer than it looks and worse than it reports.** Single transaction (so an
interrupt rolls back cleanly — the strongest property in the context) and idempotent. But: no
scope/dry-run/confirmation/running-server check; no backup on the common path (`backup.rs` takes
one only when migrations are pending); an unbounded full-table read materializing every record's
JSON in RAM; and it prints rows *changed* but never rows *scanned* nor the resolved DB path —
while `Config::load()` silently defaults to a **CWD-relative** `data/pumper.db` that
`Storage::connect` will happily create. Run the raw command the docs give from the wrong
directory and you get `0 record fingerprint(s) rewritten` against a brand-new empty database.
(`just reindex` is safe — `just` runs from the justfile's directory.)

**justfile vs the routes it calls:** `just doctor` omits `skip_artifacts` (the one knob that makes
the report survivable on a large archive); `just retention-preview` and `just enforcement-preview`
**hardcode port 8088** while `doctor`/`datahub-preview` take one; all four use `curl -s` with no
`--fail`, so a 409/503 body prints as a blob and the recipe **exits 0**. Against a stopped server
they fail with a bare `exit code 7` and never say "start the server".

**Untested:** `reindex.rs` — zero tests. `resolve_targets` — zero tests. The backfill main loop —
untested (its three tests cover a **hand-rolled reimplementation** of the loop body, so by
construction they cannot catch a target-resolution bug). `/datasets/doctor` and
`/retention/preview` routes — **zero Rust tests**, and the live smoke assertions on them are
vacuous (`smoke.ps1:704-705` literally asserts `$true`). No test anywhere spawns either
maintenance binary as a process. The two operations most likely to run at 2am have the thinnest
coverage in the repo.

**Doc drift:** `ONBOARDING.md:440-444` says "two read-only operator recipes"; there are four plus
`just smoke`. `reindex` has no dedicated documentation anywhere and no `feature-doc-map.json`
entry. No `docs/features/` page owns this context.

## Direction history

### Round 17 (2026-08-13) — gate: director-self-gated (autonomous, Athena-dispatched)

**The decay rule paid in the opposite direction from usual.** r11 banked the `check-doc-sync`
scan-window hole as anchor **#4, "lesser … dev-loop only"**. This round's scout *measured* it
instead of reading it, and it became the round's strongest finding. Re-verifying banked seeds
keeps earning — this time by promoting one, not shrinking it.

**ACCEPTED 3:**
- [[doc-sync-hook-fires]] — the repo's entire same-session doc-drift defense has **never fired**.
  Director-verified by replaying the script's algorithm on this session's own transcript: 55 of 58
  `type:'user'` entries are tool results, 3 edits in the file, **scan window saw 0**. The scout
  independently replayed all 31 project transcripts: 1,128 edits, zero detections, every time.
  Two of this round's other five directions exist because docs drifted unchecked — this is the
  upstream cause.
- [[backfill-purges-ghosts]] — r11 anchor #1, re-confirmed at the SQL by the Director
  (`datasets.rs:1854` has `WHERE removed_at IS NULL`, `:1865` does not). The documented full
  recovery cannot purge the exact ghosts it exists to purge, and says "complete". Absorbs r11
  anchor #3 (the silent 1M cap) and the typo'd-scope success as same-function riders.
- [[doctor-sees-search]] — r11 anchor #2. The 2am store-integrity report has seven checks and
  none about search, while search has the repo's most manual recovery story. Carries the
  `records_without_simhash`-never-clears defect as a second AC (same file, same
  findings-honesty theme). The slate's one **feature** lens.

**REJECTED, with reasoning:**
- *`reindex` guard-rails* (r11 anchor #4, scout's #3) — strong, and deliberately deferred rather
  than dismissed. Two reasons: (a) the *destructiveness* framing is overstated — it is
  transactional, idempotent, and rewrites only a derived column, which the scout itself calls the
  context's strongest property; (b) what remains is really an **argv/reporting redesign** (dry-run,
  `--app` scope, print scanned + resolved DB path, chunk the scan, probe for a running server),
  and it collides with [[backfill-purges-ghosts]]'s write set in `datasets.rs` and the bin layer.
  **Banked as this context's anchor**, narrowed to: report scanned + resolved DB path, chunk the
  unbounded read. The wrong-database ambiguity is the part that actually bites.
- *doctor/retention route tests + vacuous smoke assertions* — the finding that `smoke.ps1:704-705`
  asserts `$true` is real and slightly embarrassing. Not slated as its own direction because
  [[doctor-sees-search]] AC5 already requires route-level coverage for `/datasets/doctor`, which
  is half of it; the `/retention/preview` half is banked.
- *justfile recipe gaps* (hardcoded ports, missing `skip_artifacts`, `curl -s` without `--fail`
  exiting 0 on a 503) — genuine operator-experience defects, but small and cosmetic-adjacent
  against the taste filter. Banked as a rider for whichever future direction next opens the
  justfile.
- *ONBOARDING "two recipes" / missing reindex docs* — doc-only drift, and fixing it before
  [[doc-sync-hook-fires]] lands would just re-drift. Banked to ride the next pass here.

## Shipped
- (via search-engine r7 for backfill internals, `4ca9cc4`.)
- Round 17: (in flight)
