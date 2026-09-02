# Wave 5 Design — Domain Products and the Consumer Plane (FINAL, 2026-09-02)

Four builders off `master` after wave 4 is merged. N23 goes last in the merge order because it
touches every route file. [DESIGN-WAVE-1.md](DESIGN-WAVE-1.md) §Shared rules and §Shared surfaces
apply unchanged. Read `FIXES-WAVE-4.md` first.

## File-scope partition (HARD boundaries)

| Builder | Item | Owns | Must not touch |
| --- | --- | --- | --- |
| U | **N31** Applicant fit engine | `crates/apps/grants-common/src/fit.rs` (new) + the corpus-pass call site, `crates/server/src/routes/query.rs` (`/grants/profiles`, `/grants/fits`, `profile=` on `/grants`) — append at the end, migration (or `grants/profiles` as a dataset — pick and say why), `catalog/data-sources.toml`, `docs/features/apps.md` §grants, `docs/features/http-api.md` | the source apps; R's `programs` module beyond reading `program_key` |
| V | **N35** Sub-state launch atlas | `crates/apps/census-density/src/**`, `crates/apps/census-nonemp/src/**` (county probe), `crates/apps/homewyse-pricing/src/**` (driver only), `crates/apps/census-common/src/**` (CBSA crosswalk table — additive), `catalog/data-sources.toml`, `docs/features/apps.md` §census | `trades-common` (S owns `market/profile`; V adds `county` to the profile ONLY via a documented follow-up, not this wave) |
| W | **N36** Codebook-resolved skills | `crates/apps/mpsv-ciselniky/` (new crate), `crates/apps/mpsv-vpm/src/lib.rs` (label stamping + `INDEXED_DATASETS`), `crates/server/src/registry.rs` (register the app), `catalog/data-sources.toml`, `docs/features/apps.md` §labour | `mpsv-ispv` beyond reading |
| X | **N23** Consumer plane v2 | every `crates/server/src/routes/*.rs` (typed response DTOs only — no behaviour change), `crates/server/src/routes/mod.rs` (schema-coverage test), `clients/**` (generated TS + new python/rust/cli), `justfile` (`sdk` recipe), `.github/workflows/ci.yml` (generated-client drift check), `docs/features/sdk-typescript.md` → `sdks.md` + map entry, `docs/features/http-api.md` | any handler's behaviour; `crates/core`; app crates |

X **must not** merge before U/V/W: it types the response envelopes those builders add. The
coordinator merges X last and X rebases onto the merged tree before its final gate run (the one
exception to "never rebase" — X's branch is documentation of the tree, not a feature).

## Item specs (v1 slices — do NOT exceed)

### U — N31 Applicant fit engine (L, contract) — card GI4

**v1 slice.** `grants/profiles` rows `{name, org_type (nonprofit|gov|tribal|smb|university|
individual), country, state?, ein?, uei?, ntee?, budget_band, focus_tags[], cost_share_capacity,
programs_watched[]}` via `POST /grants/profiles` (validated JSON schema; admin scope) + `GET`.
Pure `fit(profile, unified, detail?) -> Fit {verdict: eligible|likely|blocked|unknown, score,
reasons[], blockers[]}` with hard gates on `applicant_types`/`eligibilities` vocab maps, geography,
cost-share vs capacity, award band vs budget; `unknown` whenever the fields are Null (never a
fabricated `eligible`); `x_not_y` tests. Corpus pass: for each profile, evaluate this cycle's
new/changed unified keys (delta, not corpus), upsert `grants/fits` keyed `{profile}:{unified_key}`;
`fresh` rows are the alert set. `GET /grants/fits?profile=&verdict=`, `GET /grants?profile=`;
`grants/fits` in `index_datasets`. Result block `fits: {profiles, evaluated, eligible, likely,
blocked, unknown}`.
**Out of v1:** Claude refinement on `eligibility_text`, EIN verification against IRS BMF.
**Gate to prove:** a profile with unknown cost-share capacity gets `unknown`, not `blocked`; a
vocabulary miss is logged and yields `unknown`; the delta pass does not re-evaluate unchanged keys.

### V — N35 Sub-state launch atlas (L, contract) — card MD4

**v1 slice.** (1) Probe NES county availability with one keyed request per default NAICS, **record
the verdict in the app doc header** (the BFS contract precedent) — this is the first build step and
its answer shapes the rest. (2) `blend_market` gains a `geo` dimension: cells `{naics4}:{geo_fips}`
for state and county; solo side joined at county when NES serves it, else state-carried with
`solo_grain: state_carried` (never a fabricated per-county number). (3) `census/atlas`: top-N
counties per trade by saturation and by `total_market_per_10k`, with the `vintages` block;
county runs scoped to the top-K states by formation velocity (`[census] atlas_states_k`, default
10). (4) `metro_pricing` schedule recipe enqueuing `homewyse-pricing` for the atlas's top-N metros
via a small static county→CBSA table, budget-capped. Every block grain-labelled.
**Out of v1:** raising `BLEND_READ_LIMIT` beyond pagination, `county` on `market/profile`.
**Gate to prove:** a county cell with no NES row carries `solo_grain: state_carried`; the atlas
never contains a county outside the top-K states; a fixture run's `metro_pricing` plan lists the
expected CBSA names.

### W — N36 Codebook-resolved skills demand (L, none) — card MD5

**v1 slice.** New app `mpsv-ciselniky` (key-free, quarterly cron in the catalog): fetches the
Dovednost / Vzdelani / Kraj / TypMzdy codebooks from data.mpsv.cz, upserts `codebooks/{name}`
keyed by URI with `label_cs`/`label_en?`, drift-loud floors per codebook (the mpsv-ispv shape).
`mpsv-vpm`: load the codebooks once per run and stamp `skillLabel`, `educationLabel`, `krajName`,
`title` on `skill_demand`/`education_agg`/region rows; `kraj_label` renders the name; unresolved
ids stay as raw URI with `label: null`, never dropped. Add `skill_demand` and `education_agg` to
`INDEXED_DATASETS` with a weekly rollup dataset (`skill_demand_weekly`) so daily churn does not
index the same cell 365 times; update the inventory test. Result carries `labelled_share` per
dataset.
**Out of v1:** the ESCO crosswalk (document the door), English labels if the codebook lacks them.
**Gate to prove:** a codebook fetch returning fewer rows than the floor is a drift refusal; a
row whose URI is missing from the codebook keeps its URI and gets `label: null`; `kraj_label`
renders `Jihomoravský kraj` for `Kraj/116` (fixture).

### X — N23 Consumer plane v2 (L, contract) — card HA5

**v1 slice.** `#[derive(Serialize, ToSchema)]` DTOs for every response envelope (Job, Record page,
Revision page, Schedule + health, Trigger, Delivery, Receipt, Economics, Sources, Doctor, Workflow
run, Transaction, Principal, Subscription, Event page, Mesh, Node, …) registered as
`components.schemas`; a test asserts every 200 response in the spec references a schema (extend
`spec_covers_exactly_the_registered_routes`) and **fails before** on the current tree. **No
behaviour change**: dual-mode/legacy shapes stay; only schemas are added. `just sdk` generates
`clients/typescript` types from the spec (replacing hand types, keeping `PumperSync`), a
`clients/python` (`pumper-sync`) with the same watermark loop, and `clients/cli` (`pumper jobs ls`,
`pumper datasets export`, `pumper triggers test`) — Rust client crate only if time allows. CI diffs
generated clients against committed output. `GET /datasets/{app}/{ds}/stream` is **out** (N05 gave
`GET /events` + `subscribe(cursor)`; say so).
**Out of v1:** Rust client (unless trivial), SSE in SDKs, retry policy beyond exponential backoff
on the shared error-code map.
**Gate to prove:** the schema-coverage test red-then-green; generated TS compiles
(`npm --prefix clients/typescript run typecheck`); the fixture-conformance test becomes a
generated-vs-served check.

## Merge order (coordinator)

W (N36) → V (N35) → U (N31) → X (N23, after rebasing onto the merged tree). Full gates after all,
then `just ci` and `scripts/smoke.ps1` for the campaign close, then `FIXES-WAVE-5.md` and a
campaign summary in `INDEX.md`.
