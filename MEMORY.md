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
`docs/harness/moonshot-2026-09-01/INDEX.md` (generation-2 moonshot deck: 37 L/XL items ranked by scout corroboration, decisions pending — M01–M44 of the 2026-07-30 scan are all shipped, do not re-propose them),
`docs/features/` (what the product does today), `context-map.json` (file → feature).

## Invariants and gotchas

Eleven things this repo does that are **not** derivable from a skim, and that prose
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

6. **`target/` grows without bound, and `data/` never does.** Cargo garbage-collects
   `target/` **never**: every dep bump and feature flip mints a fresh hash-suffixed
   artifact and the old one stays forever. Measured 2026-08-26 it had reached
   **280.8 GB in one month** — against **0.28 GB** of actual scraped data — with 7-16
   stale generations per target and 105.9 GB of it PDBs, because ~200 test binaries
   each statically link the whole workspace. So when disk is the complaint, `data/`
   is almost certainly not the answer: run `just disk` before theorising about
   dataset compression or retention. `[profile.dev]` in the root `Cargo.toml` now
   caps the per-build cost (`line-tables-only`, deps at `debug = 0`, incremental off —
   `just check` is the ONE recipe that re-enables incremental, and it may stay that way
   only because `cargo check` units carry their own fingerprints), and `just disk-check`
   is a self-healing rung of `just ci`. Do not simplify `line-tables-only` to
   `debug = 0`: backtrace frames stop resolving to source, which is verified by a
   planted-panic probe, not assumed.

7. **Door-inventory tests read a file's "production half" as everything before the
   first `#[cfg(test)]`.** `mcp::tests::every_door_that_creates_work_runs_the_shared_params_check`
   (and the EXPECTED-diff idiom generally) scan source text, so a `#[cfg(test)]` item placed
   high in a door file silently hides every door below it from the test that polices them.
   Found 2026-09-02 (N15 build): a test-gated `handle_rpc` near the top of `mcp/mod.rs` hid the
   `enqueue` doors. Keep test-gated items at the bottom of door files.

8. **Parallel worktree builders all pick the same migration number.** Four of five wave-1
   builders took 0041; the coordinator renumbers at merge and greps docs/tests for the old
   name. `config.rs` conflicts on every merge because every builder appends a section at the
   end — resolve by keeping both sides, and re-check for braces lost at the hunk boundary
   (`5bfad5d`).

9. **The mesh wire format lives in `crates/core/src/mesh.rs` — one implementation, for
   everyone.** Signing bytes, ed25519 verification, the key fingerprint, the live-set
   digest, `ghost_keys` and the bundle shapes (`weather_entries`, `exportable_recipe`,
   `importable_recipe`) are all there; `app_peer::envelope` is a re-export of it, kept so
   the paths the app and the server have always used keep resolving. Consumers:
   `crates/server/src/{node.rs, routes/mesh.rs, routes/host_weather.rs, routes/recipes.rs,
   routes/datasets.rs}` and `crates/apps/peer`. Do **not** re-implement any of it: two
   implementations of "what bytes a bundle is" is an interoperability bug that shows up only
   between two nodes on different builds. The module is behind core's `storage` feature
   (the weather bundle is typed by `tiers::WeatherEntry`), and `ring`/`hex` are optional
   core deps that feature turns on, so a `default-features = false` embedder still links
   neither. Related traps: node identity is memoised per **key-file path**, never a
   `OnceLock` (the two-node e2e runs both nodes in one process and a shared keypair makes
   the forgery test pass for the wrong reason); a corrupt `node.key` is a hard error, never
   a silent re-key; and `[[peer]]` is reconciled into schedules at **boot only**, unlike the
   catalog.

10. **An empty replay ring reports "nothing missed", not "reset".** `EventBus::replay`
   answers `Events([])` when the ring is empty ("no buffered events, so no loss possible"),
   which is right for a fresh process that has emitted nothing and catastrophically wrong for
   a process that has just *restarted*: a client resuming with `Last-Event-ID` got an empty
   answer, no events and no `reset`. Any code reading the ring for a resume must check that
   the first buffered event is exactly `after + 1` before trusting it — that is what
   `routes::events::replay_or_log` does before falling through to the `events` table. Found
   2026-09-02 (N05 build), by the restart e2e.

   Two more N05 traps in the same area: the durable log is written **asynchronously** —
   `emit` queues, and the transaction happens at the next outbox pass (`subscriptions::drain`,
   on the scheduler tick and at the end of a job's fan-out), so a test that reads the table
   right after an emit must `persist_pending` first. And the drain must run **after**
   `finalize_with_stages`, which is what publishes `job.succeeded` — the most-subscribed kind
   there is; draining before it left that event in the queue until the next tick.

11. **The nine invariants above were numbered 1–9 before wave 3.** Renumbering on merge is
   normal here; cite invariants by their text, not their number.

## How to extend this file

Add an entry only when the fact is (a) durable across sessions, (b) not obvious from
reading the file it lives in, and (c) something a future agent would otherwise get
wrong. Architectural *decisions* go to `.perfect/Architect/decisions/` and get a
backlog line; this file is for invariants and traps.
