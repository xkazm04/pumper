# Batch 1 Design — Domain Data Products + Archive Tier (2026-07-30)

> Execution branch: `vibeman/moonshot-exec-2026-07-30` (off master `ecd4d5f`). Baseline: `cargo check` clean, `cargo test --workspace` all green.
> 5 items built by 5 parallel agents. Agents DO NOT run git. Orchestrator runs the full gate and commits per item.
> Source of truth for each item's intent: the finding entries in this directory (funding-grants.md, economic-data.md, scraping-engines.md). This doc adds the *coordination* contract.

## Shared rules (all agents)

1. **Read the codebase patterns before writing.** Every new app copies an existing sibling's shape: `requires()`, `default_params()`, `schedule()`, doc-header pinning the upstream API contract, `Config::validate`-style guards, honest suppressed→`Null` (never $0), `minCount` privacy floors where personal-scale data appears.
2. **Catalog drift-gate**: the server crate has a test that cross-checks every live `schedule()` against `catalog/data-sources.toml` BOTH directions. Any new app with a schedule MUST add its catalog row (market/status/category/cron matching exactly) or the gate fails.
3. **Tests**: pure logic (classifiers, lineage grammar, diff/aggregation math) gets unit tests in-crate. Match existing test style (`&[&Value]` helpers to dodge chrono, r## raw strings when `"#` appears).
4. **Cargo**: you MAY run `cargo check -p <your-crate>` (it may block briefly on the shared target-dir lock — that's normal, wait). Do NOT run `cargo test --workspace` or touch other crates' code.
5. **Datasets**: expose new data through existing surfaces (`?filter=`, search, triggers) — no new bespoke routes unless the item explicitly requires one.
6. **No git.** Leave the working tree edited. Reply with: files created/modified, dataset names, params added, catalog row added (y/n), registration needs, test count added, and any deviation from this doc.
7. **Non-goals**: no dependency additions beyond what the item needs (justify each), no CI changes, no edits outside your file scope.

## File-scope partition (conflict avoidance — HARD boundaries)

| Agent | Owns (may create/edit) | MUST NOT touch |
|---|---|---|
| A. M34 | `crates/apps/grants-common/**`, `crates/apps/grants-gov/**`, `crates/apps/ca-grants/**` | eu-sedia, registry.rs, catalog |
| B. M31 | NEW `crates/apps/cordis/**`, `crates/apps/eu-sedia/**` | **grants-common** (if a change seems needed, write it in your reply instead), registry.rs, root Cargo.toml, catalog — registration is applied by the orchestrator from your reply |
| C. M39+M40 | NEW `crates/apps/census-nesd/**`, NEW `crates/apps/census-bfs/**`, `crates/apps/census-density/**`, `crates/apps/census-common/**` (additive only), `crates/server/src/registry.rs` (append your 2 apps), root `Cargo.toml` [workspace.dependencies] + server Cargo.toml deps, `catalog/data-sources.toml` (append your 2 rows) | grants apps, mpsv, core |
| D. M37 | `crates/apps/mpsv-vpm/**` | mpsv-ispv, core, everything else |
| E. M18 | NEW `crates/engine-archive/**`, `crates/core/src/fetcher.rs`, `crates/core/src/tiers.rs`, root `Cargo.toml` [workspace.dependencies] (append engine-archive only), `crates/server` wiring for engine construction + `config.toml` `[archive]` section | apps/**, engine-http internals (reuse via imports), catalog |

Agents C and E both append one line to root `Cargo.toml` `[workspace.dependencies]` — C appends nothing there (crates/apps/* is a workspace-member glob; app crates are added to the SERVER crate's Cargo.toml deps + registry). E appends `pumper-engine-archive`. No overlap.

## Item specs

### A — M34 Amendment radar (grants lifecycle events)

Follow the Path in funding-grants.md §US-2 exactly. Key decisions:
- `classify_events(old: &Value, new: &Value, observed_at) -> Vec<GrantEvent>` PURE, in grants-common; v1 taxonomy (6): `deadline_extended`, `deadline_accelerated`, `forecast_posted`, `award_raised`, `reopened`, `closed_early`. Both values must parse for date/number comparisons — unparseable ⇒ no event (never guess).
- Hook: inside `finalize_unified` where changed keys are already known; fetch prior revision via the existing revision/history API (`changes_since`/`history` — whichever the code actually offers; read it first).
- Persist to `grants/events`, key `{opportunity_key}:{observed_at_date}:{kind}` (append-only). Set a sane revision-retention expectation in the doc comment (janitor exists, OFF by default).
- Debounce: within one run, a field that flips A→B→A emits nothing.
- Unit tests: one per event type + unparseable-date no-event + flip-flop debounce.

### B — M31 CORDIS win-intelligence

Follow funding-grants.md §EU-1 Path. Key decisions:
- New `cordis` ScrapeApp. **Start with the CORDIS REST/JSON extraction API, NOT the bulk CSV dump** (http engine has no binary/streaming body — that's deferred engine-traits#2). Page politely; cap per run (`max_projects` param, default conservative). Dataset `cordis/projects` keyed by RCN; store topic identifier, EU contribution, coordinator, participants, start year.
- `topic_lineage(identifier) -> Option<String>` PURE in eu-sedia (or cordis — pick one, say which): strip year+counter per the Horizon identifier grammar; Horizon-only, return None for non-Horizon. Unit-test with real-looking identifiers incl. exceptions.
- Aggregate `cordis/topic_stats` per family: project count, total/mean EU contribution, top participant orgs (bounded list ≤10).
- eu-sedia run: join `topic_stats` onto each normalized opportunity as a `history` block (read via Datasets get — no cross-app fetch). Missing stats ⇒ no block, never a fabricated zero.
- Do NOT register the app or edit catalog — put the exact registry line + catalog row in your reply.

### C — M39+M40 Succession-wave + formation-velocity

Follow economic-data.md §Census-1 and §Census-2 Paths. Key decisions:
- `census-nesd`: clone census-nonemp shape (same key handling via census-common, header-by-name parsing, suppression-tolerant). Dataset `census/owner_age` keyed `state|naics|age_band`. Then extend the density blend with `pct_owners_55plus` + `succession_receipts` (55+ share × receipts) + optional enterprise-value expression via valuation SDE bands **only if** the blend already reads valuation data — do not add a new cross-app dependency if it doesn't.
- `census-bfs`: BFS timeseries endpoint (`/data/timeseries/bfs`); VERIFY the parameter contract on the doc-header level (pin it like siblings do). Dataset `census/formations` keyed `state|sector|period`; derived `census/formation_velocity` (T12M sum, YoY delta, acceleration) per state×sector. Weekly `schedule()`. Honest grain labeling: `grain: "naics_sector"` in records — no trade-level inference.
- Register BOTH apps (registry.rs append, server Cargo.toml deps, catalog rows: bfs weekly, nesd annual).
- Unit tests: suppression handling, velocity math (incl. partial-year windows), age-band share math.

### D — M37 Vacancy survival ledger

Follow economic-data.md §MPSV-1 Path. Key decisions:
- Ledger = ONE artifact per run (`vacancy-ledger.json.gz` or plain chunked JSON — match existing artifact discipline in this app), ~300k compact tuples `{id, czIsco, kraj, salary_band, first_seen, last_seen, seen_count, ico}`. Load prior run's ledger via `read_source_artifact`-style path or a dedicated bounded dataset record holding the artifact pointer — read how mpsv-vpm handles artifacts today and reuse.
- Diff rules: missing today ⇒ `closed_at = today` (label is **time-to-close**, NOT time-to-fill — disappearance conflates filled/withdrawn/expired; say so in the dataset doc). **Max-gap tolerance**: if `today - last_run > max_gap_days` (param, default 3), do NOT close anything — carry forward and log.
- Repost detection: closed posting matching (IČO, czIsco, kraj, salary_band) reappearing within `repost_window_days` (default 30) ⇒ mark repost, link ids.
- Aggregate `cz-labour/vacancy_lifecycle`: czIsco×kraj median/p75 days-open, repost share, churn; apply the existing `minCount` floor.
- Unit tests: diff transitions (new/ongoing/closed/gap-tolerance), repost matcher, aggregation percentiles.

### E — M18 Tier-zero archive engine

Follow scraping-engines.md §Fetch-2 Path. Key decisions:
- New crate `crates/engine-archive` implementing the same client trait engine-http implements (read `crates/core/src/engine.rs` first). Wayback only in v1 (Common Crawl deferred). CDX query `?url=&limit=1&sort=reverse` (+ `&to=` when as-of requested), then raw body via `/web/<ts>id_/<url>`; reuse core's capped/charset-aware body reader if importable, else mirror it minimally.
- `HttpRequest` gains `archive_max_age: Option<u64>` (secs). Tiered fetcher: when set, try archive BEFORE live HTTP; snapshot older than the window ⇒ fall through to live. Response marked `fetched_via: "archive"` + snapshot timestamp (whatever provenance mechanism HttpResponse supports — header map or field; read it first).
- Politeness: archive.org goes through the existing governor like any host.
- Config: `[archive] enabled = false` default-OFF (config.toml + Config struct + validate). Wire construction in server main/state where other engines are built.
- Backfill job type is OUT OF SCOPE for this batch (needs worker changes — Batch 3 territory). Note the seam in a doc comment.
- Unit tests: CDX response parsing, as-of window logic, fall-through decision. Integration behind `#[ignore]` if it hits the network.

## Orchestrator protocol

1. Dispatch A–E in parallel (Fable subagents, no-git).
2. On each return: review reply → apply any deferred registrations (B) → `cargo check --workspace` → targeted `cargo test -p` → commit that item (`vibeman(moonshot): M<id> <title>`).
3. After all five: full `cargo test --workspace` + drift-gate; fix-forward; write FIXES-BATCH-1.md; update vault ledgers (rows → Fixed with SHAs).
