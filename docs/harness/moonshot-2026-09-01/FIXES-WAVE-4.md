# Wave 4 — Executors and Domain Products (2026-09-02)

Five builders (four items + the carry-forward builder P), all merged into `master`. Each reported
the five cargo gates green in its worktree; the coordinator ran `cargo check` (and server/core
tests where the merge touched the server) after each merge and the full gate set after the last.

## Merges (in landing order)

| Item | Branch | Builder commits | Notes |
| --- | --- | --- | --- |
| N27 Corpus graph intelligence | `moonshot/n27-corpus-graph` | af50265 … 791391c (6) | no migration; clean |
| N29 Program registry | `moonshot/n29-programs` | dc2615b, e993ec7, b5e1931 | no migration; clean |
| N33 State × trade market profile | `moonshot/n33-market-profile` | 7 | no migration; `query.rs` + catalog appends both kept |
| N18 Elastic executor plane | `moonshot/n18-executors` | 8 | migration **0051**; route inventory + doc table appends both kept |
| P Carry-forwards | `moonshot/p-carry-forwards` | 6eb1863 … b964854 (6) | no migration; clean |

Master migrations now end at **0051**.

## What master can do now

- **Executor plane** (`[executors] enabled = true` + secret): `pumper --executor --coordinator <url>`
  processes long-poll `POST /executors/claim` and run executor-eligible (`ScrapeApp::executor()`,
  result-only, inventory-enforced) apps; heartbeat/checkpoint/progress/finish are fenced on
  `(status, attempts, executor_id)`; the coordinator finalizes; a dead executor is reaped and the
  job re-claimed with its checkpoint. `readable` is the first eligible app.
- **`grants/programs`**: the program (ALN set / Horizon family / agency|title) as an entity with
  recurrence projection, extension rate, win history; `program_key` stamped on unified rows as a
  DerivedPath; `GET /grants/programs`, `program=` on `/grants`.
- **`market/profile`** keyed `ST:trade`: economics + density + succession + formation with honest
  `coverage` and `density_grain: naics4`; `GET /market/profile/{state}/{trade}` and MCP
  `market_profile`; `market` virtual namespace.
- **Crawl graph**: `mode: "graph"` PageRank over current-vintage `edges` → `crawl/page_rank`;
  `importance_weight` on revisits (0 = today's ordering, pinned); observatory `sample_by: rank`;
  `crawl/structure_changes`; `just graph-indegree`.
- **Carry-forwards closed**: mesh wire format in `pumper_core::mesh` (optional under the `storage`
  feature); extractor `profile:` door with `profile_version` stamping; `registry::apps(&config)`
  so apps read their own `[section]`; `AppContext::request_schedule` → `managed_by = "app:<name>"`
  rows capped by `[worker] max_app_schedules_per_run`; event-log prune runs in the store janitor
  under the activity gate; `trigger-plugins.md` cross-reference; `ingress.md` doc-map entry.

## Findings worth keeping

- **`edges` is upsert-only, so out-degree cannot be read off it** (N27): the first drift detector
  reported zero because dropped links still had rows. Fixed by classifying each edge's vintage
  against the page's producing `job_id`; the graph ranks today's site map, not every crawl ever run.
- **`detect_removed` is a guarded seam** (N29): the removal guard test refused a direct call from
  the rollup; departed programs are retired by name with `tombstone_keys`.
- **A new `AppManifest` field is not additive** (N18): 35 struct literals. The eligibility flag went
  on the trait (`ScrapeApp::executor()`, default false) instead.
- **`index_datasets` declarations are pinned per crate** (N33): only `census-density` declares
  `market/profile` today; the other eight publishers republish it unindexed. Tripwire test added.
- **Two doors disagreed on a blank `profile` string** (P): extracted `root_declared` so both read it
  the same way.
- **FIXES-WAVE-3 §3's premise was wrong**: `store_janitor` does not return early (that is
  `retention_janitor`); the real defect was cadence tied to job completions.

## Carry-forward seams (not built)

1. Eight `market/profile` publishers do not index it (two one-line additions + four pinned tests in
   crates outside S's row). Small; wave 5 or campaign close.
2. `RevisitSeed.importance` as a core field and the app-side `CADENCE_PRIOR_SECS` mirror of core's
   private constant — the seams a core-side pass would clean up.
3. Executor-side VCR, RPC `Datasets` client, cancel arm on the heartbeat — out of v1, documented.
4. `program_key` joins the unified record body, shifting SimHash fingerprints once; effect on
   `DUP_DISTANCE = 3` linking during the transition cycle unmeasured.
5. Three `app_peer::envelope::…` import paths in the server still resolve through the re-export.
6. Nothing booted; `just ci` / `scripts/smoke.ps1` still owed at campaign close.

## Verification on master

The first full run after the P merge failed to compile: two **semantic** collisions that no per-branch gate could see — P added two fields to `AppContext` while N18 introduced a new struct literal in `executor_main.rs` (`a2758e7`), and P changed `registry::apps()` to take the config while N18's new test called it bare (`91af912`). Both fixed by the coordinator; then, on `master`, each in its own invocation:

| gate | result |
| --- | --- |
| `cargo fmt --all -- --check` | exit 0 |
| `cargo check --workspace` | exit 0 |
| `cargo clippy --workspace --all-targets -- -D warnings` | exit 0 |
| `cargo test --workspace` | exit 0, no FAILED/panicked |

Lesson: a wave whose builders change a shared constructor signature or add struct fields needs the coordinator's full gate BEFORE the next wave branches — wave 5 branched from the fixed commit.
