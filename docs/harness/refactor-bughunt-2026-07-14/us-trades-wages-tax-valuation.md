# US Trades Wages, Tax & Valuation — refactor + bug-hunt findings

> Total: 5 findings (Critical: 0, High: 2, Medium: 3, Low: 0)
> Files scanned: `crates/apps/trade-wages/src/lib.rs`, `crates/apps/homewyse-pricing/src/lib.rs`, `crates/apps/state-tax/src/lib.rs`, `crates/apps/valuation-multiples/src/lib.rs`, `crates/apps/trades-common/src/lib.rs` (confirming crate)

## 1. Pricing rows keyed on model-generated free text + `upsert_many` → unbounded stale/duplicate accumulation
- **Severity**: High
- **Lens**: bug-hunter
- **Category**: sync-misuse / non-idempotent
- **File**: `crates/apps/homewyse-pricing/src/lib.rs:147-186` (consumed at `crates/apps/trades-common/src/lib.rs:477-505`)
- **Scenario**: The pricing key is `format!("{locality}:{trade}:{job}")` where `job` is the model's free-text label ("Water heater installation"). Each refresh the agent is prompted for "3-4 representative jobs" per trade, and the phrasing drifts run-to-run ("Install water heater" / "Water heater replacement" / "Replace water heater tank"). Because it persists with `upsert_many("pricing", …)` (partial — never prunes), every run *adds* new job keys and *never removes* the previous run's. After N scheduled refreshes the `pricing` dataset holds N× the intended rows: near-duplicates plus stale prices from months-old runs. `unified::summarize_pricing` then `list(…, 1000)`s ALL of them and folds every row into the low/median/high envelope and `jobs_priced` — so the operator-facing pricing benchmark in `operator_economics` is computed over stale + duplicated jobs and steadily drifts.
- **Root cause**: `upsert_many` (partial-merge) semantics were applied to what is conceptually a full per-run snapshot, keyed on an unstable model-authored string. The three sibling apps avoid this by keying on stable identifiers (`US:{trade}`, `state:{ST}`, `federal:US`) so their re-runs overwrite in place; only pricing has drifting keys.
- **Impact**: wrong money value — pricing envelopes shown to operators progressively diverge from the latest research; `jobs_priced` inflates; cost of every refresh compounds the corruption. Guaranteed on every re-run, independent of model quality.
- **Fix sketch**: Persist pricing as a full snapshot with `sync_many("pricing", …)` (scoped to the locality) so each run replaces the prior set, and/or derive a stable key by slugifying the job onto a small canonical job taxonomy rather than raw model text.

## 2. Priced values stored raw (`j.get("low")`) instead of the validated number → string-quoted prices silently dropped from the unified rollup
- **Severity**: High
- **Lens**: bug-hunter
- **Category**: silent-failure / grain-mismatch
- **File**: `crates/apps/homewyse-pricing/src/lib.rs:148-172` (consumed at `crates/apps/trades-common/src/lib.rs:485-493`)
- **Scenario**: Validation parses each price via `validate::num`, which *tolerates numeric strings* (`"$150"`, `"1,200"` → 150.0, `lib.rs:100-106`), so a job the model returns as `"low":"150","median":"300","high":"500"` PASSES `require_positive`/`require_monotone` and upserts as a "valid" row. But the code then stores the RAW value — `"low": j.get("low")` (line 169-171) — i.e. the string `"150"`, not the parsed number. Downstream, `unified::summarize_pricing` reads it with `Value::as_f64()` (lib.rs:485-491), which returns `None` for a JSON string, so the row is silently skipped from the low/median/high envelope; if all of a trade's jobs are string-priced, `summarize_pricing` returns `None` and the trade can drop out of `operator_economics` entirely. This is live precisely on the `salvage_json` fallback path (the documented ~1/3 of runs where `output.json` is `None` and no `--json-schema` coercion applied), which is exactly when the model tends to emit loosely-typed JSON. The same class affects state-tax: `top_marginal_rate` stored raw, then `unified` (lib.rs:386) drops string rates from `median_state_rate` via `as_f64()`.
- **Root cause**: the tolerant validation grain (`num` accepts strings) and the persistence/consumption grain (`as_f64` requires a JSON number) disagree; the validated numeric is discarded instead of being what gets written.
- **Impact**: wrong metric — priced jobs (or whole trades / state rates) counted as "accepted" yet invisible to the cross-source rollup that grounds the product read; no rejection, no report entry.
- **Fix sketch**: Store the already-parsed numbers — `"low": json!(low), "median": json!(median), "high": json!(high)` — so persisted prices are always JSON numbers; do the same normalization for numeric fields in the other three apps (or have `unified` fall back to `validate::num` when `as_f64` is `None`).

## 3. state-tax persists a full 50-state+DC snapshot with `upsert_many` → stale states linger and pollute the unified median
- **Severity**: Medium
- **Lens**: bug-hunter
- **Category**: sync-misuse
- **File**: `crates/apps/state-tax/src/lib.rs:176-188` (consumed at `crates/apps/trades-common/src/lib.rs:382-389`)
- **Scenario**: The app's own docs say ONE call returns all 52 jurisdictions — a full snapshot. It persists with `upsert_many("tax", …)`. On a degraded/partial run (model returns only 40 states, or drops CA), the missing states keep their PRIOR records; completeness is only *reported* (`missing_states`, lib.rs:176-180, 199-201), never reconciled. `unified::sync_operator_economics` then `list("state-tax","tax",200)`s every `level=="state"` row and takes the median top-marginal rate over whatever is present — mixing this run's fresh rates with stale ones left from an earlier, more-complete run.
- **Root cause**: `sync_many` (full-snapshot, prunes absent keys) is the tool for a fixed-roster snapshot; `upsert_many` was chosen (reasonably, to keep last-known + report gaps) but its stale residue silently feeds the illustrative-state median.
- **Impact**: wrong metric — `illustrative_state_top_marginal_rate_median` can reflect a jurisdiction the latest research no longer returned.
- **Fix sketch**: Use `sync_many("tax", …)` so absent jurisdictions are pruned (keeping the `missing_states` report), or have `median_state_rate` filter records to the current `year` before aggregating.

## 4. Copy-pasted artifact-save + JSON-extract/salvage block across all four apps belongs in `trades_common`
- **Severity**: Medium
- **Lens**: code-refactor
- **Category**: duplication
- **File**: `crates/apps/trade-wages/src/lib.rs:102-118`; `crates/apps/homewyse-pricing/src/lib.rs:104-122`; `crates/apps/state-tax/src/lib.rs:107-123`; `crates/apps/valuation-multiples/src/lib.rs:95-111`
- **Scenario**: The "serialize `output.json` (or raw text) → `save_artifact("research.json", …)` → take `output.json` else `salvage_json(&output.text)` else `Error::App("<app>: agent did not return JSON …")`" block is byte-for-byte identical in all four apps, differing only by the app-name literal in the error string. The surrounding `ResearchRequest` wiring (`model`/`effort`/`json_schema`) and param parsing (`year`/`role`/`max_turns`) are also near-duplicated. `trades_common` already hosts `salvage_json` and `validate` but not this wrapper — the exact "parse logic copy-pasted per app that should be in the shared crate" pattern.
- **Root cause**: shared crate captured the leaf helpers but not the orchestration that uses them identically four times.
- **Impact**: wasted maintenance — any change to artifact naming, salvage policy, or the error message must be made in four places and can silently drift.
- **Fix sketch**: Add `trades_common::extract_research_data(&ctx, &output, app_name) -> Result<Value>` that saves the artifact and returns the parsed-or-salvaged object; each app replaces ~16 lines with one call.

## 5. Federal dollar constants (`standard_deduction_single`, `section_179_limit`) are never validated → garbage federal record upserts and flows into `operator_economics`
- **Severity**: Medium
- **Lens**: bug-hunter
- **Category**: silent-failure
- **File**: `crates/apps/state-tax/src/lib.rs:128-145` (only the three rate fields are guarded at line 132-134)
- **Scenario**: Federal validation runs `require_rate` on `self_employment_tax_rate`, `qbi_deduction_pct`, `top_marginal_rate` only. The two dollar magnitudes — `standard_deduction_single` and `section_179_limit` — get NO check (unlike trade-wages/valuation, which `require_positive` every magnitude). A model that returns `section_179_limit: 0`, a negative, or a percentage-shaped `standard_deduction_single: 15` (mis-grained) yields a federal record that passes, upserts as `federal:US`, and is lifted verbatim into `operator_economics.tax.federal` by `unified::tax_context` (lib.rs:452-463) for every trade. Separately, `require_rate([0,100])` also accepts a fraction-grained rate (SE tax `0.153` instead of `15.3`) — 100× too small, unflagged.
- **Root cause**: the plausibility gate for the federal record covers only the fields shaped as rates; dollar constants were left unguarded, so a whole class of nonsensical federal values is treated as valid.
- **Impact**: wrong money value — corrupted federal set-aside/QBI/§179 constants propagate silently to every trade's economics row with no rejection and no report entry.
- **Fix sketch**: Add `require_positive` (and a plausibility ceiling) for `standard_deduction_single` and `section_179_limit`, and consider a min-magnitude floor on the rate fields to catch fraction-vs-percent grain errors.
