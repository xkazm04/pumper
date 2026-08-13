---
slug: trades-join-derived-and-visible
type: perfect/direction
context: "[[trades-operator-economics]]"
lens: robustness
status: accepted
size: M
proposed: 2026-08-13
accepted: 2026-08-13
shipped: —
commit: —
---

## What & why
`trades/operator_economics` is the product of this whole family — the joined per-trade view of wage
band + pricing + tax + compliance + valuation that round 1 built `trades-common` to produce
(`[[trades-common-unified]]`). It is also **the one dataset in the family a consumer would actually
watch, and nothing about it is watchable.**

**It is invisible to the entire fan-out.** `worker::load_run_changes` is scoped by
`run_indexed_apps` (`worker.rs:1339-1358, 1572-1580`) = the job's own app plus the apps named in the
result's `index_datasets`. **Zero of the six apps declare `index_datasets`** (grep: 0 hits). So every
`trades/operator_economics` and `trades/compliance` revision is never loaded: **no watch, no dataset
trigger, no `enforce_contracts` evaluation, no search doc, no DataHub lineage.** Five apps refresh it
all week and a webhook set on it never fires. On top of that `trades` is absent from
`registry::VIRTUAL_NAMESPACES` (`registry.rs:78-90` — `grants` is the only entry), so on a fresh
install `POST /watches {app:"trades"}` 404s. Both halves are solved elsewhere in this repo: the
census family fixed exactly this with `with_product_index`, and `cz-labour` has it recorded as a
known bootstrap gap.

**And it is a pure derived dataset written as if it were a source.** Every block in the record —
`wage_band`, `pricing`, `tax`, `compliance`, `valuation` (`trades-common:1053-1096`) — is derived
from another dataset, which is precisely the disease `DerivedPaths` was built for. The write is
`ctx.datasets.upsert_many(UNIFIED_APP, OPERATOR_ECONOMICS, &items)` (`:1099-1102`), which means:
- `DerivedPaths::NONE` → a Texas pricing refresh marks ~10 rows `changed` that contain no new Texas
  fact, and a `state-tax` refresh re-hashes all ~260 rows. The change feed for the one dataset a
  consumer reads is untrustworthy. (`DerivedPaths` hit count across the seven crates: **0**.
  Adopters elsewhere: `eu-sedia`, `grants-gov`, `plugin/observatory`.)
- Raw `ctx.datasets` rather than `ctx.upsert_many` → **no `Provenance` and no `job_id`**
  (`app.rs:619-622` stamps `job_id` only on the `ctx.*` path). The most-derived dataset in the family
  carries the least provenance. `census_common::derived_provenance` solves this for the census family.
- It also bypasses `write_target` (`app.rs:704-710`), so a quarantined source cannot divert these
  writes to `@q`.
- The join is **recomputed 5× per refresh cycle**, once at the end of each of the five apps' runs.
  `grants-common` solved this with a once-per-cycle claim on `grants/maintenance` key `corpus_pass`.

**Rider — silently truncatable reads.** Every read the join makes is a hard cap with no at-cap
detection: `COMPLIANCE_READ_LIMIT = 5_000` (`:962`), `PRICING_READ_LIMIT = 50_000` (`:967`),
`state-tax/tax` at 200 (`:988`), `taxonomy` at 1_000 (`:412`). Census's `inputs_truncated` idiom
exists in-repo and was not adopted.

## Evidence
- `crates/apps/trades-common/src/lib.rs:1099-1102` — the raw `ctx.datasets.upsert_many` write
- `crates/apps/trades-common/src/lib.rs:1053-1096` — every block derived from another dataset
- grep `DerivedPaths` across the seven crates → **0 hits**
- grep `index_datasets` across the seven crates → **0 hits**; 19 files elsewhere use it
- `crates/server/src/worker.rs:1339-1358, 1572-1580` — `run_indexed_apps` scopes the whole fan-out
- `crates/server/src/registry.rs:78-90` — `VIRTUAL_NAMESPACES`, `grants` only
- `crates/core/src/app.rs:619-622` — `job_id` stamped only on the `ctx.*` path
- `crates/core/src/app.rs:704-710` — `write_target` bypassed by the raw path
- `crates/apps/state-licensing/src/lib.rs:305-308` — second raw write, `trust: None`
- `crates/apps/trades-common/src/lib.rs:962, 967, 988, 412` — the four uncapped-detection read caps
- call sites of the join: `state-tax:262`, `state-licensing:339`, `trade-wages:212`,
  `homewyse-pricing:284`, `valuation-multiples:190`

## Acceptance criteria
1. `trades/operator_economics` (and `trades/compliance`) become visible to the fan-out: the apps
   declare `index_datasets` so `run_indexed_apps` widens and watches/triggers/contracts/search
   actually see revisions. A test proves the declaration reaches the result.
2. The join's write goes through the provenance-carrying path so records carry `Provenance` and
   `job_id`, and `write_target` is honoured (a quarantined source diverts to `@q`). Follow
   `census_common::derived_provenance` rather than inventing a fourth spelling.
3. `DerivedPaths` is adopted for the join so a refresh of one input stops marking rows `changed` that
   contain no new fact from that input. A test proves a byte-identical re-derivation reads
   `unchanged`. **Verify the path spelling against the implementation before writing it** — a wrong
   spelling makes the seam *look* adopted and do nothing, which is the exact failure this direction
   exists to kill.
4. The join is not recomputed five times per cycle, or if it is, that is a deliberate documented
   choice with the cost stated. `grants-common`'s once-per-cycle claim is the in-repo precedent —
   read it before deciding.
5. At-cap reads are detectable: a read that hits its limit is reported rather than silently
   truncating the join. Follow census's `inputs_truncated`.
6. **You may not edit `crates/server/src/registry.rs`** (it drags `ONBOARDING.md` + `runtime.md`,
   which belong to the sibling lot). The `trades` → `VIRTUAL_NAMESPACES` entry is real and needed —
   **report the exact edit** and the Director applies it.

## Risks / non-goals
- **Non-goal:** building a consumer for `trades/operator_economics`. It has none in-repo, and
  inventing one is the zero-consumer product invention this loop has rejected before. This direction
  makes the dataset *reachable* by the mechanisms that already exist.
- **Non-goal:** changing `worker.rs` or core's fan-out. Everything here is declarable from the apps.
- Risk: adopting `index_datasets` turns on search indexing for these datasets for the first time —
  check the echo is bounded (the r12/r19 `records`-echo lesson) and report the row counts involved.
- Risk: `trades-common` is consumed by three census apps for `taxonomy::registry_naics`. Prefer
  additive change; report any signature break.

## Build record
(filled during build)
