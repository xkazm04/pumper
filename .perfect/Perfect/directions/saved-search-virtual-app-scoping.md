---
slug: saved-search-virtual-app-scoping
type: perfect/direction
context: "[[job-worker]]"
lens: robustness
status: shipped
size: S
proposed: 2026-08-03
accepted: 2026-08-03
shipped: 2026-08-03
commit: 21c838d
---
## What & why
`notify_saved_searches` skips any saved search whose `app` differs from `job.app`. But the grants
unified layer indexes its documents under a VIRTUAL app (`grants`), not under the producing source
app, so a saved search scoped to `grants` — the only scope matching how those docs were indexed — is
silently skipped on every `ca-grants` / `eu-sedia` run. The alert never fires and nothing logs why.
Round-3 banked seed, now confirmed in code.

## Evidence
- Filter: `crates/server/src/worker.rs:836` (`search.app != job.app` → continue).
- Virtual app constant: `crates/apps/grants-common/src/lib.rs:29` (`UNIFIED_APP = "grants"`),
  `index_datasets` spec at `:87-90`.
- Docs built from the SPEC's app, not the job's: `crates/server/src/worker.rs:1080-1097`.
- Unguarded: `crates/core/tests/saved_search_views.rs` covers materialization only.

## Acceptance criteria
- The scoping decision is an extracted, named predicate over the apps this run actually indexed
  under (job app + any `index_datasets` virtual apps), not an inline comparison.
- Test named after the anti-pattern (e.g. `alert_scoped_to_virtual_app_is_not_skipped`).
- A saved search that matches nothing logs the reason at debug — silence is the bug's signature.
- No duplicate alerts when several source apps feed one virtual app (`saved_search_seen` dedupe holds).

## Risks / non-goals
Do not widen scoping to "all apps" — an explicitly scoped search must still exclude unrelated apps.

## Build record
Builder extracted `run_indexed_apps(job_app, index_specs)` + `search_covers_run(...)` and threaded the
app list into `notify_saved_searches`. Four tests incl. `alert_scoped_to_virtual_app_is_not_skipped`
and `unrelated_app_scope_is_still_excluded` (scoping NOT widened to all apps). Three previously
silent paths now log at debug. **Docs-vs-code catch:** `docs/features/search.md` had documented the
bug as intended behaviour ("scope by dataset and leave app unset") — corrected in the same commit.
Director review: read the full diff; predicate placement, dedupe via `claim_unseen`, and the
unscoped-search case all correct. Gates green on master (251 server tests).
