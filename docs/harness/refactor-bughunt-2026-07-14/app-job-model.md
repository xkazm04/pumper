# App & Job Model — refactor + bug-hunt findings

> Total: 5 findings (Critical: 0, High: 1, Medium: 3, Low: 1)
> Files scanned: `crates/core/src/app.rs`, `crates/core/src/job.rs`, `crates/core/src/lib.rs`, `crates/core/src/config.rs`, `crates/core/src/error.rs` (+ confirming reads: `fetcher.rs`, `costs.rs`, `crawl.rs`, `apps/extractor`, `apps/census-nonemp`, other `apps/*`)

## 1. Metering + budget seam is bypassable; the `extractor` app spends paid Claude through the raw fetcher, so `budget_usd` and the cost ledger are silently defeated
- **Severity**: High
- **Lens**: bug-hunter
- **Category**: silent-failure / budget-ceiling bypass
- **File**: `crates/core/src/app.rs:43,95-208` (design) — confirmed at `crates/apps/extractor/src/lib.rs:206-228`
- **Scenario**: `AppContext::fetch` (the metered wrapper) is the *only* place cost is recorded and `budget_usd` is enforced, but `engines` is a `pub` field (`app.rs:43`) and the docstring only *asks* callers to "prefer this over calling the fetcher directly". The `extractor` app ignores that: it clones `ctx.engines.fetch` and calls `.fetch()` directly (`extractor:214-228`) with a caller-selectable `strategy` that includes `FetchStrategy::AutoWithResearch` (`extractor:206-211`, reachable via `params.strategy = "auto_with_research"`). A job submitted with `budget_usd: Some(1.0)` and that strategy escalates to the Claude tier and spends real money, but no `cost_events` row is written and the `remaining_budget_usd` ceiling is never consulted.
- **Root cause**: The design assumption is "metering lives at the `AppContext::fetch`/`::research` seam." That invariant is unenforced — the raw engines are public, so the seam is opt-in. Any app touching `ctx.engines.fetch`/`ctx.engines.claude` (present tense: extractor does) escapes both metering and budget governance.
- **Impact**: Wrong result (cost ledger under-reports spend) + real money overspend past the configured per-job ceiling; the `budget_usd` safety mechanism is a no-op for these code paths.
- **Fix sketch**: Either make `engines` non-`pub` (force all fetches through `AppContext::fetch`), or fix the extractor to call `ctx.fetch(req)` instead of `ctx.engines.fetch.fetch(req)`; ideally both — plus a grep-based lint forbidding `ctx.engines.fetch`/`.claude.research` outside `app.rs`.

## 2. A fetch that fails on *every* tier records neither a tier-loss strike nor a cost event — the learned "skip HTTP" routing never learns from total failures
- **Severity**: Medium
- **Lens**: bug-hunter
- **Category**: silent-failure / learning-bias
- **File**: `crates/core/src/app.rs:146,178-206`
- **Scenario**: In `AppContext::fetch`, `self.engines.fetch.fetch(req).await?` (`app.rs:146`) uses `?`. When HTTP is thin/blocked *and* the browser tier also fails and there is no Claude tier (strategy `Auto`), `Fetcher::fetch` returns `Err(Error::App("all fetch tiers exhausted …"))` (`fetcher.rs:456-460`). The `?` propagates immediately, so the tier-memory write (`tiers.record(host, …, http_lost)`, `app.rs:178-191`) and the cost write (`app.rs:193-206`) are both skipped. Thus the hosts that fail *hardest* — HTTP fails on every job — never accrue the strikes that would pin them to the browser tier; strikes are only recorded when a *later* tier succeeds and the call returns `Ok`.
- **Root cause**: Learning and metering are appended after the fallible engine call and only run on the `Ok` path. The design treats "all tiers failed" as nothing-happened, but an HTTP loss did happen and is exactly the signal the router needs.
- **Impact**: Wrong long-run behaviour + wasted work: a permanently HTTP-blocking host gets its (slow, doomed) HTTP tier retried on every single job forever, adding latency and load. Secondary: on the `AutoWithResearch` error path, any partial Claude spend before the failure is never metered against the budget.
- **Fix sketch**: Capture the outcome/error before returning — on `Err`, still record an HTTP strike for the host when the escalation trail shows an HTTP thin/blocked/error verdict (and record any known cost), then propagate. Consider a `match` instead of `?` so the learning/metering tail runs on both arms.

## 3. `save_artifact` does no filename sanitization — an unsafe `name` escapes the per-job artifacts dir via `PathBuf::join`
- **Severity**: Medium
- **Lens**: bug-hunter
- **Category**: validation-gap / path-traversal (trust boundary)
- **File**: `crates/core/src/app.rs:64-69`
- **Scenario**: `save_artifact` does `self.artifacts_dir.join(name)` then `tokio::fs::write` with no validation of `name`. `Path::join` with an absolute path *replaces* the base, and a `name` containing `..` walks out of `data/artifacts/<app>/<job_id>/`. `name` is caller-supplied and at least two apps interpolate job-param data into it: `census-nonemp` builds `format!("nonemp-{naics}.json")` where `naics` comes straight from `ctx.params["naics"]` unvalidated (`census-nonemp:78-96`, used at `:171-175`), and `census-density` does the same with `cbp-{naics}.json`. Job params are an external trust boundary (`POST /apps/<name>/jobs`), so a value like `naics: "../../evil"` would target a write outside the sandbox. (In census-nonemp specifically the malicious value would fail the upstream Census API call before reaching `save_artifact`, so that one app is gated by luck, not by design — the seam itself is unguarded and any app that names an artifact after scraped/param text before a validating gate is exposed.)
- **Root cause**: The seam's docstring promises files land "under `data/artifacts/<app>/<job_id>/`" but enforces nothing; confinement is silently delegated to every caller.
- **Impact**: Arbitrary file overwrite within the process's write scope; also same-name artifacts silently clobber each other (no uniqueness/append).
- **Fix sketch**: Reject or sanitize `name` in `save_artifact` — take only the final path component (`Path::new(name).file_name()`), reject `..`/separators/absolute paths, and return `Error::App` on a bad name; then assert the resolved path starts with `artifacts_dir`.

## 4. Job budget ceiling is an advisory check-then-spend with a post-hoc ledger write — concurrent metered calls in one job can overshoot `budget_usd` by up to Nx
- **Severity**: Medium
- **Lens**: bug-hunter
- **Category**: race-condition / TOCTOU
- **File**: `crates/core/src/app.rs:72-90,128-144,193-206,237-247`
- **Scenario**: `remaining_budget_usd`/`require_budget` read `costs.job_total(job_id)` and clamp `req.max_budget_usd` to the remaining headroom *before* the spend; the `cost_events` row is written only *after* the call returns (`app.rs:193-206`, `241-247`). If a single job issues two or more metered calls concurrently (e.g. an app that `join!`s several `ctx.research`/`ctx.fetch(AutoWithResearch)` calls, or the seam-bypass in Finding #1 that spends without recording at all), each call reads the same `job_total`, each sees the full remaining headroom, and each clamps its per-call ceiling to that same full amount — so total job spend can reach N × the ceiling. There is no reservation/lock between the check and the ledger write.
- **Root cause**: Budget enforcement is a read-only advisory clamp against a ledger that is only written post-hoc; nothing reserves headroom, so overlapping calls double-count the same budget. (Latent today: no in-tree app issues concurrent *metered* `ctx` calls — the extractor and crawler both use the raw fetcher — but the seam permits it and one paid-strategy fan-out would trip it.)
- **Impact**: Real-money overspend beyond the configured per-job ceiling under concurrency; the ceiling behaves as "per-call" rather than "per-job".
- **Fix sketch**: Reserve budget atomically — insert a pending/hold ledger row (or an in-`AppContext` atomic running-total guarded by a mutex) at check time and reconcile to actual cost after, so concurrent callers subtract from a shared, already-decremented remaining.

## 5. Duplicated remaining-budget clamp expression; `require_budget` is used in only one of the two metered paths
- **Severity**: Low
- **Lens**: code-refactor
- **Category**: duplication
- **File**: `crates/core/src/app.rs:82-90,138-141,237-239`
- **Scenario**: The clamp `req.max_budget_usd = Some(req.max_budget_usd.map_or(remaining, |b| b.min(remaining)))` appears verbatim in both `fetch` (`app.rs:138-141`) and `research` (`app.rs:238`). Separately, the "remaining ≤ 0 ⇒ stop" check is factored into `require_budget` (`app.rs:82-90`) and used by `research`, but `fetch` re-implements the same `Some(remaining) if remaining <= 0.0` test inline (`app.rs:130-137`) because it downgrades rather than errors. The two spend-governed paths drift apart despite sharing intent.
- **Root cause**: Budget logic grew per-call rather than being centralized; `require_budget`'s error-vs-downgrade split blocked reuse.
- **Impact**: Wasted maintenance — a future budget-semantics change must be edited in two-to-three places and is easy to get subtly inconsistent (this is the same surface as Findings #1/#4).
- **Fix sketch**: Extract a small helper (e.g. `clamp_call_budget(req_budget, remaining) -> Option<f64>`) used by both paths, and factor the "remaining headroom or None" fetch so `fetch` and `research` share one budget-resolution step that each then handles (downgrade vs error) explicitly.
