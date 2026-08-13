---
name: http-engine
type: perfect/context
group: Scraping Engines
category: lib
opportunity: 4
last_proposed: 2026-08-13
cooldown_until: r21
directions: ["[[http-fetch-has-a-deadline]]", "[[http-transport-errors-terminal]]", "[[profiled-fetch-is-honest]]"]
alias_of_old_map: "[[fetch-engines]] (round-3 pass covered this file)"
---

## Current state (scouted very thorough, r19, 2026-08-13)

`crates/engine-http/src/lib.rs` — 1075 lines, plus `tests/profiles.rs` and
`tests/fetch_bytes.rs`. The default fast-path fetch tier for **every** scraping app.

**The core is solid and the perimeter is soft.** Verified-good (do not re-litigate): the
body cap is checked *before* `extend_from_slice` (`:512-517`), so peak memory is
`cap + one chunk` and a gzip/brotli bomb is bounded at decoded bytes; the client-pool LRU
cannot kill an in-flight request (`get` clones at `:209`; `reqwest::Client` is internally
`Arc`); cross-profile cookie bleed is refuted and tested twice (`:1057`,
`tests/profiles.rs:104`); profiled bodies bypass the shared cache by construction
(`:382`, pinned at `:1004`); the cache key hashes method+url+body+**sorted** headers+proxy
(`cache.rs:63-91`); the `dirty`/`flushing` re-arm race at `:146-148` is correct (all four
interleavings traced); a broken configured proxy does **not** fail open to direct.

What is soft: **nothing bounds a `fetch()` end to end**, **nothing signals a degraded
identity**, and **nothing counts anything** (9 tracing events, 0 metrics, 0 `/metrics`
series — the only fetch-layer counters in the repo are `EgressCounters`, built r18 for the
remote fabric only).

**Both r18-banked anchors CONFIRMED, both sharper** (Director-verified in source, not
taken from the brief):
- *Retry-timeout amplification*: per-attempt timeout inside `for attempt in 0..=retries`
  (`:397`); `retry_delay` floor = `max(backoff, retry_after)` + 25% jitter (`:602-610`) with
  `MAX_SECS = 600` (`:624`). Black-holed host ≈ **127 s** against a `config.toml:36-40`
  promise of "~30s, not 300s"; `429 Retry-After: 600` ≈ **37.5 min**, exceeding
  `job_timeout_secs` 900 s. Zero `Instant`/`deadline` in the crate. `[remote] timeout_secs`
  (`config.rs:297-302`) documents this engine as the reason it needed its own deadline.
- *Profile jar silently empty*: `NotFound => CookieStore::default()` at `:89`, no signal at
  all; `create_dir_all` at `:81-83` runs first, so a typo **materialises** a profile that
  then appears in `GET /profiles`. Documented as a live gap at `docs/features/fetching.md:189`.
  `require_existing_profile` readers workspace-wide = **1** (`engine-browser:999`), zero here.
  **Two new seams the anchor did not have**: the flush loop clears `dirty` *before* saving
  (`:139-143`), so a transient save failure loses the login permanently with one WARN; and
  `save()` renames unconditionally over a cached-`Arc` jar (`:105-121`, `:307-309`), so an
  empty in-memory jar **clobbers** a restored `cookies.json`.

Biggest untested behavior: **the retry loop itself** (`:397-474`). `retry_delay` is tested
exhaustively as a pure function; no test anywhere executes the loop more than once. The
engine conformance battery runs with `retries: 0` (`engine_conformance.rs:134-138`) — the
ladder is switched off in the only cross-engine test that could see it.

## Direction history

- (as [[fetch-engines]], round 3): 5/5 shipped — body cap + timeout + Retry-After retries
  (`709e84b`), proxy support (`9d2044f`).
- **r19 (2026-08-13), 5 drafted → 3 accepted / 2 rejected**:
  - ACCEPTED [[http-fetch-has-a-deadline]] · [[http-transport-errors-terminal]] ·
    [[profiled-fetch-is-honest]]
  - REJECTED-deferred **http-engine-observable** (feature · M — `HttpCounters` on `/metrics`
    mirroring `EgressCounters`: retries, 429 rate, cap rejections, pool evictions,
    empty-jar fetches, pool thrash past the hardcoded `MAX_POOLED_CLIENTS = 8` at `:46`).
    Precedent exists (r8 `webhook-observability`, r14 `wasm-fuel-telemetry`), but two of its
    best series are produced by the three accepted directions, so it is worth more *after*
    them. **BANKED as this context's next anchor** — it is the only zero-metric engine left.
  - REJECTED outright **http-redirect-target-policy** (a public URL that `302`s to
    `169.254.169.254` escapes `/fetch-proxy`'s target policy; `Policy::limited` at `:255`).
    Bound by the r8 precedent: "egress-hardening — no privilege gain on an unauthenticated
    local API". `[remote] enabled` is default-off and a caller who can reach `/fetch-proxy`
    can already reach anything. Re-open only if auth ships or `[remote]` becomes default-on.
  - REJECTED-deferred **explicit `.no_proxy()`** (reqwest defaults `auto_sys_proxy = true`;
    zero hits for `no_proxy`/`HTTP_PROXY` repo-wide, so on a corporate box all scraping may
    egress through the system proxy while `[http] proxy` reads `None`). Rests on unverified
    dependency semantics — the scout flagged it SUSPECTED. **Banked**: verify against reqwest
    0.12 before proposing.
  - Folded as riders rather than slated: `[http] max_body_bytes = 0` = total outage with no
    `validate` clause (→ deadline direction, which already touches config validation);
    retry-loop test coverage (→ criteria on both loop directions).

## Banked (r19 — re-verify at proposal time, seeds decay)

1. **http-engine-observable** — the anchor. See above.
2. `HttpResponse.final_url` is set on every response but has **zero production consumers**
   of the redirect-resolved value (8 readers total: 3 test asserts, 2 cache round-trip, 2
   are a *different* `final_url` in transact, 1 doc comment). `FetchOutcome` has no such
   field and `outcome()` sets `url: req.url.clone()` (`fetcher.rs:1256`), so **after any
   redirect everything past the tiered fetcher records the requested URL, not the real one.**
3. The `jars` map (`:240`) and the profile directory tree grow unbounded from request input
   (names arrive from job params; `create_dir_all` per new name). Deliberate today — the
   comment at `:236-240` correctly identifies flush-on-eviction as the prerequisite.
4. Blocking IO under `std::sync::Mutex` on the async runtime: `client_for` holds the lock
   across rustls client construction (`:338-343`), `jar_for` across `create_dir_all` + open
   + JSON parse (`:306-313`). Zero `spawn_blocking` in the crate.
5. No flush-on-shutdown hook anywhere in `crates/server/` (measured: zero). The 1 s debounce
   is documented as a *crash*-loss window; it applies to every clean shutdown too.
6. Four `[http]` keys (`max_body_bytes`, `proxy`, `redirect_limit`, `retryable_statuses`)
   are absent from the shipped `config.toml`, which lists only three.
7. `docs/features/fetching.md:109` says the pool is "keyed by proxy URL"; it is
   `(proxy, profile)` since the session vault landed (`:186-188`). `:222` says it correctly.

## Shipped

- **r19 (2026-08-13), 3/3** — landed on master via `perfect/2026-08-13-r19`:
  - [[http-fetch-has-a-deadline]] → `967c409` — one `fetch()` is bounded end to end by
    `[http] total_budget_secs` (default 300, `0` disables). The 37.5-minute `Retry-After` fetch is
    now < 5 s and 1 attempt; a retry sleep the budget cannot hold **fails rather than truncating**,
    because retrying earlier than the server asked would trade a wall-clock bug for a politeness
    one. First tests anywhere to run the retry loop more than once. Rider:
    `[http] max_body_bytes = 0` refused at boot.
  - [[http-transport-errors-terminal]] → `2e46fd0` — an unparseable URL or `ftp://` off a crawl
    frontier fails **once** (`Error::BadRequest`, terminal, 400) instead of burning 4 engine
    attempts × N job attempts. Classified on reqwest's typed predicates via a testable
    `TransportPredicates` value, never on message text; `is_connect` deliberately left retryable
    with the DNS/TLS/captive-portal reasoning recorded. Conformance battery 5 → 7 and no longer
    runs with `retries: 0`.
  - [[profiled-fetch-is-honest]] → `2e46fd0` + `3c806b1` — a mistyped profile no longer scrapes the
    login wall in silence: WARN at load, `x-pumper-anonymous-profile` on the response, lifted into
    the escalation trail, the receipt and `TierTrace.detail` **including the winning entry**. A typo
    no longer materialises a profile in `GET /profiles`. A failed jar write is retried instead of
    losing the login forever, and an empty jar can no longer clobber a restored `cookies.json`.
- (inherited — see [[fetch-engines]])
