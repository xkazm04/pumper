---
slug: session-vault
type: perfect/direction
context: "[[Fetch Engines (HTTP / Browser / Claude)]]"
lens: wildcard
status: shipped
size: M
proposed: 2026-07-13
accepted: 2026-07-13
shipped: 2026-07-13
commit: 50e03ba
---

## What & why
HTTP cookies die with the process (in-memory jar); the browser has exactly one shared unnamed profile — no per-site login isolation, no login selection per fetch. Ship backlog #139 phase 1: named profiles (persistent cookie jar for HTTP + per-profile user_data_dir for browser), `profile` field on requests, GET /profiles.

## Evidence
- In-memory jar: crates/engine-http/src/lib.rs:30 (cookie_store(true), no persistence)
- Single profile: crates/core/src/config.rs:149 (user_data_dir), engine-browser/lib.rs:32-38
- No profile field: engine.rs:23-39 (HttpRequest), :75-87 (RenderRequest)
- Backlog #139 unshipped (INDEX.md:83)

## Acceptance criteria
- [ ] Profiles live under data/profiles/<name>/ (cookies.json for HTTP — persisted jar loaded per profile; browser/ per-profile user_data_dir).
- [ ] `profile: Option<String>` on FetchRequest/HttpRequest/RenderRequest, threaded through the tiered fetcher; default = today's behavior (shared jar / default profile).
- [ ] HTTP jar persisted across restarts (load on first use, save on write or shutdown — state the approach; reqwest cookie_provider with a serializable jar).
- [ ] GET /profiles lists existing profiles (OpenAPI + EXPECTED); profile names validated (path-safe).
- [ ] docs/features/fetching.md + http-api.md updated.

## Risks / non-goals
- Non-goals: credential storage/encryption, login automation, profile CRUD beyond list (mkdir-on-use).
- Browser concurrency: chromiumoxide = one user_data_dir per browser instance — switching profiles may require relaunch; coordinate with [[browser-resilience]]'s holder (per-profile browser instances or relaunch-on-switch; builder states the approach, DECISION NEEDED if unclear).

## Build record
(pending)
