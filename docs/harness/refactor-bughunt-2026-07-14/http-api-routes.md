# HTTP API & Routes — refactor + bug-hunt findings

> Total: 5 findings (Critical: 0, High: 1, Medium: 3, Low: 1)
> Files scanned: `crates/server/src/routes.rs`, `crates/server/src/main.rs`, `crates/server/src/state.rs` (confirmatory: `crates/core/src/storage.rs`, `crates/server/src/worker.rs`)

## 1. Fully-permissive CORS over an unauthenticated, mutating, data-bearing API
- **Severity**: High
- **Lens**: bug-hunter
- **Category**: input-validation / data-exposure / config
- **File**: `crates/server/src/routes.rs:110-117`
- **Scenario**: `router()` wraps the whole surface in `CorsLayer::permissive()`, which emits `Access-Control-Allow-Origin: *` (plus all methods/headers) on every route. The API uses no credentials, so the wildcard origin lets a browser read every response. A user who has pumper running locally then visits any malicious web page: that page's JavaScript can issue cross-origin `fetch` calls that succeed *and are readable* — e.g. `GET /datasets/{app}/{dataset}/export` to exfiltrate the entire scraped corpus, `POST /apps/{name}/jobs` to enqueue arbitrary work, or `DELETE /schedules/{id}` / `DELETE /hosts/{host}/memory` to destroy state. Because auth is deferred, nothing else gates these. The "localhost only" assumption is also defeatable via DNS-rebinding, which reaches `127.0.0.1:<port>` from a remote origin.
- **Root cause**: `permissive()` was chosen for "local power mode" convenience, but it pairs a wide-open origin policy with an API that is both unauthenticated *and* has destructive/exfiltrating routes — the two decisions compound.
- **Impact**: drive-by cross-origin read of all scraped data + remote-triggered mutation/deletion from any website the operator visits.
- **Fix sketch**: Replace `permissive()` with an allow-list of explicit local origins (e.g. reflect only `http://localhost:*`/`127.0.0.1:*` origins), or gate mutating/export routes; at minimum drop the wildcard for anything beyond `GET /health`/`/metrics`. Independently, bind the listener to loopback by default. (This is the specific "dangerous unauthenticated route class," not a blanket add-auth request.)

## 2. Legacy (non-cursor) list branches ignore `limit` and return the entire table
- **Severity**: Medium
- **Lens**: bug-hunter
- **Category**: dos / input-validation
- **File**: `crates/server/src/routes.rs:929-931, 1452-1454, 1578-1580, 2036-2038`
- **Scenario**: `list_schedules`, `list_watches`, `list_triggers`, and `list_saved_searches` each define a `limit` param (default 50) but only honor it in the `cursor` branch. When no `cursor` is present they call the un-paged store fn — `storage.list_watches(app)` etc. — which run `SELECT … ORDER BY …` with **no LIMIT clause** (confirmed in `crates/core/src/storage.rs:625-632, 729-738, 834-843, 953-962`). Watches, triggers, and saved-searches are all created through unauthenticated `POST` routes, so a client can insert an unbounded number of rows and then `GET /watches` (no cursor) forces the server to load and serialize every row into one response. `?limit=10` is silently ignored on that path.
- **Root cause**: dual-mode design kept a "legacy bare-array/object" branch that predates the `limit`/keyset work and was never wired to the cap; the cursor branch clamps (`clamp(1, 500)`) but the fall-through does not.
- **Impact**: unbounded memory + response size (DoS) on a self-inflatable dataset; plus a correctness surprise — the caller's `limit` is honored only when a `cursor` is also sent.
- **Fix sketch**: Have the non-cursor branches call a capped store fn (or pass `query.limit.clamp(1, 500)` through to a `LIMIT`-bearing query), so the legacy shape is still bounded.

## 3. `max_attempts` accepted from the request with no upper bound → non-terminating retry loop
- **Severity**: Medium
- **Lens**: bug-hunter
- **Category**: input-validation / dos
- **File**: `crates/server/src/routes.rs:504/549` (enqueue), `955/1005` (schedule), `1612/1677` (trigger)
- **Scenario**: `EnqueueBody.max_attempts` (and the schedule/trigger equivalents) is an `Option<i64>` taken verbatim. The store only clamps the *lower* bound (`opts.max_attempts.max(1)` at `storage.rs:163`; `t.max_attempts.max(1)` at `:814`) — there is no upper bound. A client can `POST /apps/{name}/jobs` with `{"max_attempts": 9223372036854775807}` for an app that reliably fails; the job then re-queues on every failure essentially forever (`job.attempts < job.max_attempts` at `storage.rs:274`), permanently occupying a queue slot and re-hitting the target/engine on each backoff tick. `budget_usd` is optional, so a non-Claude (HTTP) app has no cost ceiling to halt it.
- **Root cause**: the boundary validates the floor (avoids a dead 0/negative job) but assumes callers pick a sane ceiling; nothing enforces one.
- **Impact**: an effectively immortal job per request — slow-drip resource/queue/target load with no operator kill-switch short of manual DB edit.
- **Fix sketch**: Clamp `max_attempts` to a sane ceiling (e.g. `clamp(1, 20)`) at enqueue/schedule/trigger creation, mirroring the `limit` clamps already used elsewhere.

## 4. Dual-mode list handler boilerplate duplicated across ~11 endpoints
- **Severity**: Medium
- **Lens**: code-refactor
- **Category**: duplication
- **File**: `crates/server/src/routes.rs` — `list_jobs:616`, `list_schedules:925`, `list_records:1096`, `dataset_changes:1347`, `record_history:1404`, `list_watches:1448`, `list_triggers:1574`, `list_deliveries:1872`, `list_saved_searches:2032`, `list_hosts:2219`, `list_grants:2681`
- **Scenario**: Every list endpoint repeats the identical shape: `let Some(cursor) = &query.cursor else { <legacy shape> }; let limit = clamp(...); let after = parse_cursor(cursor); let items = store.<x>_page(...); let next_cursor = keyset_cursor(...); Ok(Json(json!({items, next_cursor})))`. `keyset_cursor` was already extracted, but the clamp + cursor-vs-legacy branch + `{items, next_cursor}` envelope is hand-copied 11 times, each a place for the `limit` cap (see finding 2) or the response key to drift.
- **Root cause**: no shared helper/generic for "dual-mode keyset page" — each handler open-codes the control flow around the two store calls.
- **Impact**: wasted maintenance and drift risk; finding 2's missing cap is exactly the kind of per-copy divergence this invites.
- **Fix sketch**: Extract a generic helper (e.g. `dual_mode_page(cursor, limit_bounds, legacy_fn, page_fn, encode)`) that owns the clamp, branch, and envelope, so each handler supplies only the two store closures and the encoder.

## 5. `since` RFC-3339 parsing closure duplicated verbatim
- **Severity**: Low
- **Lens**: code-refactor
- **Category**: duplication
- **File**: `crates/server/src/routes.rs:894-902` (`cost_summary`) and `1352-1360` (`dataset_changes`)
- **Scenario**: Both handlers contain the identical `query.since.as_deref().map(|s| chrono::DateTime::parse_from_rfc3339(s).map(|d| d.with_timezone(&Utc)).map_err(|e| ApiError(BAD_REQUEST, format!("invalid 'since': {e}")))).transpose()?` block. Any change to the error wording or accepted format must be made in two places.
- **Root cause**: no shared `parse_since(&Option<String>) -> Result<Option<DateTime<Utc>>, ApiError>` helper, unlike the grants surface which already factored `parse_grant_date`/`filter_value`.
- **Impact**: minor maintenance duplication; risk of the two `since` parsers diverging.
- **Fix sketch**: Extract a `parse_since` helper alongside `parse_grant_date` and call it from both handlers.
