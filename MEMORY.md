# MEMORY.md

Repo-local, cross-session memory. Read this at session start; add to it at session
end. It is an **index into `.perfect/`** plus the invariants that are expensive to
rediscover. Anything you can learn from a five-minute skim does not belong here.

> Some agents also keep machine-local memory under
> `~/.claude/projects/<slug>/memory/`. That is invisible to every other clone of
> this repo — durable facts belong **here** or in `.perfect/`.

## Durable state (`.perfect/`)

| File | What it holds |
| --- | --- |
| [.perfect/Architect/backlog.md](.perfect/Architect/backlog.md) | The architectural queue: pending / shipped / abandoned, ranked by (reach × payoff) / (risk × effort). **Check "Pending" before proposing structural work.** |
| [.perfect/Architect/decisions/](.perfect/Architect/decisions/) | ADR-style records, one per shipped decision, with the commits that carried it. |
| [.perfect/Architect/coverage.md](.perfect/Architect/coverage.md) | Which themes/areas have been scanned and when — the anti-rescan ledger. |
| [.perfect/Architect/strong-patterns.md](.perfect/Architect/strong-patterns.md) | Patterns codified as house style. Follow them; don't reinvent. |
| [.perfect/Architect/weak-patterns.md](.perfect/Architect/weak-patterns.md) | Known anti-patterns and where they still live. |
| [.perfect/Architect/architect-preferences.md](.perfect/Architect/architect-preferences.md) | How the user wants scans run and proposals framed. |
| [.perfect/Lessons/](.perfect/Lessons/) | Per-run retrospectives (what the loop got wrong). |
| [.perfect/Perfect/](.perfect/Perfect/) | The `/perfect` loop's vault: `contexts/`, `directions/`, `sessions/` — where the last build run stopped. |

Other durable references live outside `.perfect/`:
`docs/harness/harness-learnings.md` (structural facts + pattern catalogue),
`docs/features/` (what the product does today), `context-map.json` (file → feature).

## Invariants and gotchas

Five things this repo does that are **not** derivable from a skim, and that prose
elsewhere gets wrong.

1. **CORS is OFF by default — README.md and ONBOARDING.md §2 say the opposite.**
   Both still advertise "permissive CORS", but `crates/server/src/routes/mod.rs`
   ships same-origin only: an allow-all on an unauthenticated, mutating,
   data-bearing API lets any site the operator visits drive it cross-origin (DNS
   rebinding defeats the localhost assumption). A trusted local UI opts in
   explicitly via `[server] cors_allowed_origins`. If a local frontend "can't reach
   the API", this is why — do not re-add a blanket allow-all.

2. **Extraction-health enforcement ships OFF (soak mode).** `[resilience]` defaults
   to `enabled = true, enforce = false` (`crates/core/src/config.rs`). Every verdict
   is computed and stored, and **nothing is gated**: no trust stamps, no `<dataset>@q`
   quarantine shadow datasets, no suppressed webhook pushes, no `sync_many` downgrade,
   no skipped search indexing. So a test that expects quarantine behaviour must turn
   `enforce` on, and a bug report of "the health system did nothing" is usually
   correct-by-design. Enforcement is meant to be enabled only after `source_runs`
   shows an acceptable false-positive rate on real data — on an unattended box a
   false quarantine that silently stops a working pipeline is worse than a detection
   a week late.

3. **The tier router learns per host, and the memory decays.** `crates/core/src/tiers.rs`:
   **3 consecutive** HTTP-tier losses (failure or thin content) pin a host to start at
   the browser tier; **one HTTP win clears the record**; the pin and the strikes age out
   after `[fetcher] host_memory_ttl_secs` (default **7 days**). Consequence: the same
   fetch can take a different tier — and cost a different amount — depending on state in
   the `host_profiles` table, so a fetch-tier test that doesn't reset that table is
   order-dependent. Inspect the learned state with `GET /hosts`.

4. **A wiped search index does not self-heal.** `TantivyIndex::new` rebuilds the index
   **EMPTY** whenever the on-disk schema doesn't match the build's (a field was added,
   or `body` isn't stored) — queries keep returning `200` with fewer hits, which looks
   healthy. The worker's incremental path is delta-driven off the change feed, so it
   only refills rows that change from then on. Since r12 (`63db76f`) every search
   answer carries `index: {enabled, doc_count, degraded, reason}`, so the wiped state
   is visible on the query itself; `GET /search/status` remains the telemetry view.
   The recovery is `cargo run -p pumper-server --bin search-backfill` **with the
   server stopped** (Tantivy holds an exclusive writer lock).

5. **Startup is CWD-relative and the `.env` loader never clobbers.**
   `crates/server/src/main.rs` reads `./.env` before anything touches the environment,
   and **existing env vars win** — exporting `CENSUS_API_KEY` in your shell silently
   overrides the `.env` value, and running the binary from anywhere but the repo root
   loads neither `.env` nor `config.toml` (config path is `$PUMPER_CONFIG` or
   `./config.toml`). Also worth knowing: storage is a **single SQLite file in WAL mode**
   with `max_connections = 8` and a 5s `busy_timeout` — writers serialize, so a
   long-running write transaction is a workspace-wide stall, not a local one.

## How to extend this file

Add an entry only when the fact is (a) durable across sessions, (b) not obvious from
reading the file it lives in, and (c) something a future agent would otherwise get
wrong. Architectural *decisions* go to `.perfect/Architect/decisions/` and get a
backlog line; this file is for invariants and traps.
