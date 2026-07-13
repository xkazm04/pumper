---
slug: http-request-controls
type: perfect/direction
context: "[[Fetch Engines (HTTP / Browser / Claude)]]"
lens: api-ux
status: shipped
size: M
proposed: 2026-07-13
accepted: 2026-07-13
shipped: 2026-07-13
commit: 709e84b
---

## What & why
The HTTP engine buffers entire bodies with no size cap (memory DoS), has no per-request timeout, retries with blind 500ms·2^n doubling that ignores the Retry-After it already parsed and adds no jitter, and hardcodes redirect limit + retryable statuses. Add the missing knobs.

## Evidence
- Unbounded buffering: crates/engine-http/src/lib.rs:104 (response.text())
- Retry sleep ignores Retry-After: lib.rs:66 vs :84; no jitter
- Hardcoded: redirects lib.rs:33 (10), retryable statuses lib.rs:16; no timeout field on HttpRequest (engine.rs:23-39)

## Acceptance criteria
- [ ] `[http] max_body_bytes` (default e.g. 10 MiB) + per-request override; body read streamed with limit; over-limit → typed error naming the cap.
- [ ] `HttpRequest.timeout_secs: Option<u64>` per-request (reqwest RequestBuilder::timeout).
- [ ] Retry sleep = max(backoff, Retry-After) with jitter; config for redirect limit + retryable statuses (#[serde(default)] + Default).
- [ ] Unit tests: size cap, retry sleep policy (deterministic, no real sleeps where possible).
- [ ] docs/features/fetching.md updated.

## Risks / non-goals
- Body cap default must not break existing big-page apps (SEDIA clean-text etc.) — pick generous default, verify against known apps' expectations.

## Build record
(pending)
