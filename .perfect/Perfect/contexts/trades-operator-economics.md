---
name: trades-operator-economics
type: perfect/context
group: Market Data
category: lib
opportunity: 6
last_proposed: 2026-08-13
cooldown_until: after-round-23
directions: ["[[trades-partial-run-cannot-delete]]", "[[trades-join-derived-and-visible]]"]
alias_of_old_map: "[[us-trades-wages-tax-valuation]] (round-1 pass covered these files)"
---

## Current state (scout brief digest, 2026-08-13, round 21 — first sweep on the 46-map)
Scouted "very thorough" **together with [[trades-pricing]]** as one crate family (7 crates, ~5,200
lines), because `trades-common` is the shared spine and a per-context scout would have seen half of
each defect. Read end to end: all 7 `src/lib.rs` + 7 `Cargo.toml` + `core/{app,error,catalog,
datasets,cache,config,vcr,resilience/store}.rs`, `engine-claude`, `server/{registry,worker,routes/mod}.rs`,
`catalog/data-sources.toml`, `docs/features/{apps,catalog,runtime}.md`.
**Opportunity re-scored 4 → 6** on the strength of the tombstone finding.

**Inherited history is real and must not be re-mined blind.** Round 1 (2026-07-13) shipped three
directions over these files under the old vault name: [[trades-meter-research]] (the `ctx.research`
metering migration — the scout confirms the chokepoint is still sound, pinned by
`core/tests/llm_chokepoint.rs`), [[trades-output-guards]] (json_schema + salvage + range/monotonicity
validation + completeness *reporting*), [[trades-common-unified]] (the shared taxonomy + the
`trades/operator_economics` join). **Two of this round's three directions are the unfinished halves
of that round-1 work** — `missing_states` was made to *report* and never to *enforce*; the join was
built and never made watchable — which is exactly the kind of residue a sweep is for.

**What the family does better than its siblings.** No clock, run-id, elapsed-ms or spend value is
written into any record, so a byte-identical re-harvest genuinely reads `unchanged` — ahead of
`plugin/observatory` and `grants-gov`. Caching/no-op detection is the strongest part: two layers
(domain freshness gates *before* the metered call, then the 24h `ResearchCache`). `cms-fee-schedule`
and `trades-common::taxonomy` are the best-tested surfaces in scope. `US_JURISDICTIONS` is duplicated
on purpose with the reason in the code — a good fork, left alone.

**The destructive finding (Director-verified, and SHARPER than the brief).** A `state-tax` run
returning 30 of 51 jurisdictions **tombstones the other 21 and reports SUCCESS**, with no `removed`
count anywhere in the result. Core's own doc names the hole (`app.rs:641-643`: *"`detect_removed`
already refuses an empty batch; a partial batch is the case that guard does not cover"*). The sharper
part is the comment at `state-tax:250-256`: the app **deliberately routed through `sync_many_with_
provenance` to get the removal guard**, saying so — and the guard structurally cannot engage, for two
independent reasons: `enforced_state` returns `Healthy` whenever `enforce` is off (the shipping
default), and **no app in the family calls `observe_extraction`** (grep: 0 hits), so there is no
health history to enforce against even if it were on. The code claims a protection it does not have.

**The family's other spine-level problem** is that `trades/operator_economics` — the joined product
five apps exist to produce — is invisible to the entire fan-out: zero `index_datasets` declarations
means `run_indexed_apps` never widens, so no watch, trigger, contract evaluation, search doc or
DataHub lineage ever sees it; and `trades` is absent from `VIRTUAL_NAMESPACES`, so watching it 404s
on a fresh install. It is also a pure derived dataset written raw (`DerivedPaths::NONE`, no
`Provenance`, no `job_id`, `write_target` bypassed) and recomputed 5× per refresh cycle.

## Direction history
Round 21 (2026-08-13) — gate: director-self-gated (autonomous, Athena-dispatched). 3 accepted across
this context + [[trades-pricing]], 4 rejected-deferred.

**ACCEPTED (this context)**
- [[trades-partial-run-cannot-delete]] — robustness · M. The round's highest-value item.
- [[trades-join-derived-and-visible]] — robustness · M.
(The third, [[trades-numbers-mean-what-they-say]], is filed under [[trades-pricing]] because its
reference implementation and its one fabrication both live in `homewyse-pricing`.)

**REJECTED-DEFERRED (banked, with why now-is-wrong)**
- **the JSON schema is advisory in practice** — `ClaudeEngine` falls back to `parse_loose_json`
  when `structured_output` is absent (`engine-claude:380-383`), then `research_json_named` falls back
  again to `salvage_json` (`trades-common:58-66`); **neither re-validates**, and every plausibility
  validator skips absent fields by design. So `"required": [...]` describes what the CLI was *asked*
  to enforce, not what reached the store: `{"trade":"Plumbing"}` becomes a stored wage band with every
  figure null. Real and family-wide, but it is a design pass on the validation seam (and partly lives
  in `engine-claude`, outside the lot). Partially mitigated by [[trades-numbers-mean-what-they-say]],
  which fixes what gets *stored*. **Banked here as this context's anchor for r23+.**
- **deterministic refusals classified retryable** — `state-licensing:173-178` (unknown trade label)
  and `cms:675-679` (unsupported schedule) are pure functions of frozen params raised as `Error::App`;
  and reachable from all five agentic apps, `engine-claude:{41-46,53-63,152-154}` raise
  `Error::Claude{Spawn}`. None is terminal, so each burns the whole backoff ladder re-deriving the
  identical refusal. **Amplified by the 24h research cache**: an `Error::App` retry re-reads identical
  cached bytes and fails in the same place at $0 — attempts 2..N are pure ladder burn. This is the
  **fifth** instance of the class r17/r18/r19/r20 each killed once, but the value is concentrated in
  the `engine-claude` half, which is outside this lot. **Bank on claude-engine.**
- **no per-run spend ceiling anywhere in the family** — no app sets `request.max_budget_usd`, config
  defaults it `None`, and neither shipped role sets one; `state-licensing` makes up to **five**
  uncapped calls per run at 600s each. Same disease as [[research-run-budget-is-real]] in different
  code. Deferred to keep this lot's write set from growing a fourth direction; **build it in the same
  round as the research fix's follow-up so the two ceilings get one shape.**
- **`observe_extraction` fleet-wide adoption** — 0 hits across the family means the whole
  health/quarantine/trust ladder never engages. [[trades-partial-run-cannot-delete]] deliberately
  offers this as one of its two levers; if the builder takes the completeness-floor lever instead,
  this stays banked as the fleet-wide sweep it really is.

**Riders folded into accepted directions rather than banked** (r19's standing rule — the bank is for
things that need their own design, riders are for things that need a line): near-total rejection
reporting SUCCESS, the tombstone leak-back through `Datasets::list`, the four uncapped-detection read
limits in the join, `state-licensing`'s `unwrap_or(0.0)` cost, `homewyse`'s `unwrap_or("flat")`,
`income_tax_type`'s unclosed vocabulary.

## Shipped
- (round 21 in flight — filled at wrap)
- (inherited, pre-46-map — see [[us-trades-wages-tax-valuation]])
