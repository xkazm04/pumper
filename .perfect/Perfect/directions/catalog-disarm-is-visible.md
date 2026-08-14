---
slug: catalog-disarm-is-visible
type: perfect/direction
context: "[[data-pipeline-catalog]]"
lens: robustness
status: accepted
size: M
proposed: 2026-08-14
accepted: 2026-08-14
shipped: —
commit: —
---

## The banked claim, re-verified — CONFIRMED on mechanism, NARROWED twice on blast radius

r22 banked: *"the malformed-TOML fleet-wide contract fail-open (`worker.rs:1430-1436`), one typo
silently disables every declared data contract."*

**Fleet-wide is CONFIRMED.** `crates/core/src/catalog.rs:264-283` reads exactly one file
(`$PUMPER_CATALOG` or `catalog/data-sources.toml`) and hands the whole document to `toml::from_str`.
No per-row parse, no partial recovery — one syntax error anywhere fails the whole document.
`enforce_contracts` then returns before reaching the per-pair loop (`worker.rs:1457`) and
`contract_for` (`catalog.rs:292-296`) is never called for any pair. **12 of 30 sources declare a
`[source.contract]` block; all 12 die together.**

**Line-number correction:** the banked cite `worker.rs:1430-1436` is the *doc comment* (which, to the
repo's credit, self-documents the behavior at `:1438-1439`: *"Fail-open: an unreadable catalog skips
evaluation, it never blocks delivery"*). The code is `worker.rs:1445-1451`:
```rust
let catalog = match pumper_core::Catalog::load() {
    Ok(c) => c,
    Err(e) => {
        warn!(job = %job.id, "contract check skipped (catalog unreadable): {e}");
        return;
    }
};
```
Note the asymmetry: a *missing* file is `Ok(Catalog::default())` (`catalog.rs:269-275`) — also zero
contracts, also fail-open, but by design.

### NARROW 1 — on defaults it gates nothing anyway

`[contracts] enforce` defaults **false** (`crates/core/src/config.rs:235-242`, doc: *"Default OFF —
soak mode"*). In the shipping posture `Block` (`worker.rs:1492-1500`) is unreachable; verdicts are
recorded and surfaced only. A typo costs **visibility**, not enforcement. It becomes a data-integrity
failure only once an operator sets `enforce = true` — where the fleet-wide silent disarm is real and
severe.

### NARROW 2 — it is not silent; two surfaces 500 loudly

`/catalog/sources` and `/catalog/health` both call `Catalog::load()` directly and map the error to
HTTP 500 (`crates/server/src/routes/query.rs:288-293`, `:330-335`). An operator polling
`/catalog/health` sees the break immediately.

## The defect actually worth building — and it is a different one

**`/sources` serves a stale green light.** `crates/server/src/routes/health.rs:53-91` never loads the
catalog. It renders `contracts_enforce: state.config.contracts.enforce` (`:86`) and attaches per-row
verdicts from `state.contract_verdicts` (`:341-343`) — an in-memory `HashMap` that is only ever
`insert`ed (`worker.rs:1479-1483`) and **never cleared**. After a typo lands, `/sources` keeps serving
the last-good `{"verdict": "pass"}` **forever**, alongside `contracts_enforce: true`, while nothing is
being checked.

That is the honest framing: **not silence, but a green light.** A 500 on a different endpoint does not
undo a `pass` on this one.

**Also absent:** no boot-time validation at all — `crates/server/src/main.rs` never calls
`Catalog::load()`; the only boot-adjacent loader is `scheduler.rs:481` in `catalog_reconcile_plan`.
`/metrics` (`routes/meta.rs`) emits no contract series, and `routes/doctor.rs` has zero occurrences
of `contract`/`catalog`/`checkpoint`.

## Acceptance criteria (for whoever builds this)

1. A stale `pass` verdict is never served as current after the catalog becomes unreadable — the
   verdict map is invalidated or explicitly marked stale.
2. `/sources` tells an operator that contracts are disarmed, in the same response that reports
   `contracts_enforce`.
3. Boot-time `Catalog::load()` validation, so a typo is caught at start rather than at the first job.
4. A test proves a malformed catalog cannot produce a green `/sources`.

## Why REJECTED this round

**On the 6-direction cap, and honestly on severity ordering.** Severity is conditional on a
non-default flag, and the loudest surface (`/catalog/health`) already 500s. It lost the slot to six
directions whose damage is unconditional on defaults — money spent after a cancel, a documented
config causing a 10,000-record sweep, a permanently-dead trigger edge, ~10 GB of re-download churn.

**Re-banked in the sharper form**: build the stale-green `/sources`, not "the TOML fails open". The
fail-open is documented and deliberate; the green light is the lie.

## Files a builder would touch

`crates/server/src/worker.rs` (record the state, not just `warn!`) · `crates/server/src/state.rs`
(a catalog-load-status cell beside `contract_verdicts:116-120`) · `crates/server/src/routes/health.rs`
(surface it; stop rendering stale verdicts as current) · optionally `crates/server/src/main.rs`
(boot validation) and `crates/server/src/routes/meta.rs` (a `pumper_catalog_loaded` gauge).

**Sizing note: M, and the design call is the work** — deciding what a stale verdict *means* once the
catalog is unreadable (invalidate vs mark-stale) and threading that through `/sources`. The
`worker.rs` guard itself is S.

---

## r24 re-verification (2026-08-14) — core CONFIRMED, sub-claim REFUTED, and the direction is REFRAMED

**CONFIRMED.** `/sources` (`routes/health.rs:53-91`, `list_sources`) reads only `health_store`
(`:57`), renders `"contracts_enforce": state.config.contracts.enforce` (`:86`), and attaches verdicts
via `contract_verdict` (`:76-78`, defined `:341-345`). No `Catalog::load()` in the file. Same join on
the detail view (`:155`). `contract_verdicts` is **insert-only — exactly one mutation workspace-wide**
(`worker.rs:1479-1483`); no `remove`/`clear`/`retain`/`drain` exists. Readers: `health.rs:342`,
`query.rs:348`, `receipt.rs:327`. The worker's fail-open early return (`worker.rs:1445-1451`) is
per job, so a malformed catalog skips contract evaluation for the whole fleet. `main.rs` contains
**zero** occurrences of `catalog`/`Catalog`; the only boot-adjacent load is `scheduler.rs:560-566`
`boot_reconcile`, which also warns and returns. `routes/doctor.rs` has no catalog check.

**Net at HEAD: a malformed catalog produces two log lines, two 500s, and one green 200.**

### REFUTED — "there is no staleness marker"

There is. `worker.rs:1470-1478` writes `{verdict, violations, records, removed, enforced, job_id,
checked_at}` — **`job_id` at `:1476`, `checked_at` at `:1477`** — and both ride verbatim onto
`/sources` (`health.rs:77`) and `/catalog/health` (`query.rs:356`). **The defect is not a missing
timestamp; it is that no consumer derives anything from it.** And the idiom is *in the same
function*: `query.rs:399-406` computes `age_secs` + `stale` against `expected_max_age_secs` for
*dataset* freshness, ~45 lines below `query.rs:351-358` where it inserts the raw verdict blob without
computing the identical pair for *verdict* freshness.

### The reframed direction — name the real lie

**`contracts_enforce` at `health.rs:86` reports configured INTENT, not observed enforcement.** It
reads `true` while zero contracts are being evaluated. That, plus a `pass` with no age, is the green
light. Two further instances of the same root cause, both worth closing in the same pass:

- **A contracted dataset that stops producing is never re-evaluated.** `worker.rs:1456-1457` iterates
  only *this* run's `by_dataset`, so its last `pass` sits in the map indefinitely — while
  `query.rs:400-406` correctly flags `stale: true` in the **same JSON object**. The freshness half is
  honest, the contract half is not, and they render side by side.
- **A retired source keeps serving a verdict.** `worker.rs:1483` keys `{app}/{dataset}` with no
  catalog generation. `/catalog/health` is immune (it iterates `catalog.live()`, `query.rs:339`), but
  `/sources` joins by health-store id (`health.rs:76`), which retains rows through `retired`
  (`health.rs:21`). Two surfaces, two truths, one map.

**Sizing:** three emit sites (`health.rs:83-90`, `health.rs:153-162`, `query.rs:419-428`), two
per-row insert points (`health.rs:76-78`, `query.rs:351-358`), one state cell. Copy-targets:
`routes/provisioner.rs:67-88` (`proposal_summary` age/`expired` against a config max-age,
`proposal_is_expired` `:70`) and `query.rs:396-406`.

## Acceptance criteria (r24)

1. A verdict rendered on `/sources` **carries its own age** and is marked stale rather than served as
   an unqualified `pass` forever. Use the existing `checked_at`/`job_id` — do not add a new timestamp.
2. `contracts_enforce` (or a companion field beside it) distinguishes **configured intent** from
   **observed enforcement**, so a fleet whose catalog will not parse cannot render a green
   enforcement claim.
3. The catalog is validated somewhere an operator will see it **before** a job runs — boot, doctor, or
   both. Fail-open stays fail-open (`worker.rs:1438-1439` documents that on purpose); this is about
   *visibility*, not about blocking delivery. If you make anything blocking, stop and report.
4. A test proving a stale/absent verdict is distinguishable from a fresh `pass`, named after the
   anti-pattern.
5. `docs/features/catalog.md` + `docs/features/http-api.md` describe the new fields and say plainly
   what `/sources` does and does not observe.

**ACCEPTED r24** — rejected in r23 in its "malformed TOML is silent" form (it is not silent: two
surfaces 500). Accepted now in its **stale-green** form, which is the defect that survived
verification. Gate: director-self-gated (autonomous, Athena-dispatched).
