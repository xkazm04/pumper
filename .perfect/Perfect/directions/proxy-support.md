---
slug: proxy-support
type: perfect/direction
context: "[[Fetch Engines (HTTP / Browser / Claude)]]"
lens: feature
status: shipped
size: M
proposed: 2026-07-13
accepted: 2026-07-13
shipped: 2026-07-13
commit: 9d2044f
---

## What & why
No proxy support anywhere — no .proxy() in the reqwest builder, no config, nothing for the browser. The single biggest reach limiter for a scraping service. Add [http] proxy config + per-request override, and --proxy-server for the browser from the same config.

## Evidence
- Builder without proxy: crates/engine-http/src/lib.rs:27-34; no config field: crates/core/src/config.rs:122-140
- Docs gap: docs/features/fetching.md:58 ("No proxy pool")

## Acceptance criteria
- [ ] `[http] proxy` (URL, supports http/https/socks5 per reqwest features) — #[serde(default)] + Default.
- [ ] `HttpRequest.proxy: Option<String>` per-request override (requires per-request client or pooled clients keyed by proxy — state the approach and its cost).
- [ ] Browser launches with --proxy-server when `[browser] proxy` (or inherits [http].proxy — choose, justify).
- [ ] Auth-in-URL proxies (user:pass@) work; unit tests for config parsing; live verification best-effort (no real proxy available — verify the reqwest builder wiring via a local forward proxy if trivially possible, else state honestly).
- [ ] docs/features/fetching.md updated (remove/adjust the gap note).

## Risks / non-goals
- Non-goal: proxy pools/rotation (backlog moonshot); single proxy per request/config is the slice.

## Build record
(pending)
