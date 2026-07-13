---
name: "Fetch Engines (HTTP / Browser / Claude)"
type: perfect/context
group: "Scraping Engines"
category: lib
opportunity: 7
last_proposed: never
cooldown_until: —
directions: []
---

## Current state (scout brief digest, 2026-07-13 — FRESH, reuse next round, do not re-scout)

- **HTTP**: shared reqwest client; UA hardcoded Chrome/126 default; global 30s timeout (no per-request field); redirects hardcoded 10; gzip+brotli (no zstd); retries 3 × blind 500ms·2^n (NO jitter; Retry-After parsed but NOT applied to retry sleep — only governor penalty, lib.rs:84 vs :66); retryable statuses hardcoded [429,502,503,504] lib.rs:16; **fully buffered bodies, no size cap** (lib.rs:104 — memory DoS); **NO proxy support anywhere** (biggest gap; docs confirm fetching.md:58).
- **Browser**: Chrome lazy-launched into OnceCell, lives forever; **dead-browser trap** — Chrome crash leaves populated OnceCell, no relaunch possible, all future renders fail (lib.rs:27-64); no page pool/concurrency cap (N renders = N tabs unbounded); no resource blocking (images/fonts all downloaded); stealth = one flag; all wait timeouts silent warn-only (partial DOM returned, caller can't tell — lib.rs:82,89-90); single persistent profile dir (data/browser-profile).
- **Claude**: CLI via stdin pipe (Windows-safe); model/effort/budget resolution request→role→config; --json-schema + parse_loose_json fallback; cost/turns silently None if envelope omits (budget accounting can go blind, lib.rs:179-182); no config default max-turns; skip_permissions defaults TRUE.
- **Session state today**: HTTP cookie jar in-memory only (dies with process); browser = ONE unnamed persistent profile; no per-request profile selection (no profile field on HttpRequest/RenderRequest). Backlog #139 "session vault: managed multi-profile login store" NOT shipped — the single profile dir is the primitive it generalizes.
- Backlog #114 fleet governor: mostly shipped (adaptive governor + /hosts, round 1); remaining tail = cross-instance sharing, RPS-ceiling model (low value).
- Three inconsistent identities: HTTP UA ≠ browser UA ≠ Claude.

## Direction seeds (Director, from brief)
- Browser resilience: relaunch-on-crash (replace OnceCell with reset-able holder), render concurrency cap/page pool, resource blocking, honest wait outcomes (timed_out flag).
- HTTP hardening: body size cap + optional streaming, Retry-After honored in retry sleep + jitter, per-request timeout, configurable redirects/retryable statuses.
- Proxy support (config + per-request).
- Session vault: named profiles — persistent cookie jars (HTTP) + per-profile user-data-dirs (browser), profile field on requests.
- Claude guards: default max-turns, cost-missing warning.

## Direction history
(never proposed)

## Shipped
(none)
