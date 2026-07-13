---
name: "Fetch Engines (HTTP / Browser / Claude)"
type: perfect/context
group: "Scraping Engines"
category: lib
opportunity: 7
last_proposed: 2026-07-13
cooldown_until: —
directions: ["[[browser-resilience]]", "[[browser-cheap-renders]]", "[[proxy-support]]", "[[http-request-controls]]", "[[session-vault]]"]
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
- 2026-07-13 (round 3): 5 proposed, **5 accepted** (browser resilience, cheap renders, proxy, HTTP request controls, session vault). Claude-engine guards (default max-turns, cost-missing warning) deliberately unslated — small, future ride-along.

## Shipped
- [[browser-resilience]] → a57ee1c — Mutex<Option<LiveBrowser>> holder, alive-flag crash detection (CDP handler-drain completion), relaunch on acquire, render semaphore (max_concurrent_renders default 4), RenderedPage.nav_timed_out + selector_found. Kill-recovery unit-tested (live Chrome-kill impractical — would kill user's Chrome).
- [[browser-cheap-renders]] → 8d3eda5 — CDP interception blocks image/font/media (NOT stylesheets), block_resources default true + per-request load_all_resources opt-out, blocked_resources count on RenderedPage (live-proven vs Wikipedia), --disable-dev-shm-usage + 512MB JS heap cap, recycle_after_renders (200). Caveat: interception disables Chrome's HTTP cache while on (chromiumoxide coupling).
- [[http-request-controls]] → 709e84b — [http] max_body_bytes (16 MiB) streamed cap + per-request override, HttpRequest.timeout_secs, retry sleep = max(backoff, Retry-After) + hash jitter (no rand), config redirect_limit + retryable_statuses. Caveat: capped path uses from_utf8_lossy (drops charset-from-header vs .text()).
- [[proxy-support]] → 9d2044f — [http]/[browser] proxy (http/https/socks5, user:pass@), Config::normalize browser→http fallback, HttpRequest.proxy override via bounded LRU client pool (≤8). Caveats: pooled clients have own cookie jars; browser proxy auth unsupported (Chrome prompts). Pool key generalized to (proxy, profile) by session-vault.
- [[session-vault]] → 50e03ba — data/profiles/<name>/{cookies.json, browser/}; profile on Http/Render/FetchRequest; ClientPool keyed (proxy, profile) — pool GENERALIZED not duplicated; jars owned by engine (eviction can't lose cookies), atomic tmp+rename write on ~1s trailing debounce; browser holders keyed by profile (≤4 live Chromes, LRU-evict). **Builder caught a real correctness trap unprompted: profiled requests now BYPASS the shared http_cache** (key is method+url+body only — a logged-in body would otherwise be served to anonymous callers). Live-verified: two profiles, no cross-bleed, cookies survive engine restart; interleaved Chrome profiles on real browser. Caveats: ~1s crash-loss window; plaintext on disk (no encryption/credential mgmt — phase 1); HTTP jar and Chrome profile are separate stores.
Context COMPLETE: 5/5 shipped.
