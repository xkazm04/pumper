---
name: "Tiered Fetcher & Politeness"
type: perfect/context
group: "Scraping Runtime Core"
category: lib
opportunity: 8
last_proposed: 2026-07-13
cooldown_until: after-round-3
directions: ["[[fetch-no-cache-ttl]]", "[[structured-fetch-trace]]", "[[governor-hot-path]]", "[[fetch-tier-verdicts]]", "[[host-profiles-api]]"]
---

## Current state (scout brief digest, 2026-07-13)

- Three tiers http→browser→claude; strategy enum Http/Browser/Auto/AutoWithResearch (fetcher.rs:19-31). Escalation heuristic = single scalar `min_content_chars` (compile-time const 250, fetcher.rs:17) measured on **markdown chars** — html_to_markdown runs on every tier even when `to_markdown=false`, output discarded (fetcher.rs:121,143,202).
- Escalation trail = free-text `Vec<String>` (fetcher.rs:126-133); tier-router loss detection string-matches `e.starts_with("http tier")` (app.rs:133) — fragile coupling.
- Governor: per-host token bucket + adaptive penalty (429/503 doubles, Retry-After seconds only, reward halves — even on 404, engine-http/lib.rs:83-86). PENALTY_BASE/CAP/FLOOR are compile-time consts (governor.rs:17-19). **Two global mutexes serialize all hosts** (governor.rs:67,78-85). Per-host maps grow unbounded.
- Cache: sha256-keyed SQLite, single global TTL (3600s); `put` always uses default_ttl (engine-http/lib.rs:148) though signature takes explicit ttl. Cache hits invisible in FetchOutcome (debug! only).
- **CONFIRMED gap**: `HttpRequest.no_cache` exists and is honored (engine.rs:31-43, engine-http/lib.rs:55) but `FetchRequest` never exposes it — monitors (watch, readable) see up-to-1h-stale bodies.
- Tier memory exists (tiers.rs, `tier_memory` table): 3 http strikes → pinned to browser; one http win resets. No strike decay, no learned claude tier, no visibility endpoint. Direct `ctx.engines.fetch` callers (plugin, extractor apps) bypass routing AND metering.
- Cost visible only for claude tier; no latency/bytes/cache-hit in FetchOutcome.

## Direction history
- 2026-07-13: 5 proposed, **5 accepted** (clean sweep — no rejections): no_cache+TTL (feature/S), structured trace (api-ux/M), governor hot path (opt/S), tier verdicts (robustness/M), host profiles (wildcard/M).

## Shipped
- [[fetch-no-cache-ttl]] → d6236d4 — FetchRequest.no_cache + ttl_override; watch app bypasses cache by default (cache_ttl_secs param caps staleness instead)
- [[governor-hot-path]] → 1deadf9 — DashMap per-host state (slot+penalty+last_seen unified), amortized idle eviction (4096 hosts / 1h TTL), markdown once per tier
- [[fetch-tier-verdicts]] → 11ca817 — 403/429/503 + challenge-marker escalation (4KB leading window), browser marker heuristic, 2xx-only governor reward, Retry-After HTTP-date, [fetcher] min_content_chars + [governor] penalty knobs
- [[structured-fetch-trace]] → a2bcee2 — TierTrace {tier, verdict enum, status, chars, cache_hit, latency_ms, cost_usd}; HttpResponse.cache_hit surfaced; escalations strings preserved
- [[host-profiles-api]] → 6fad704 — strike aging ([fetcher] host_memory_ttl_secs, default 7d), governor penalties persisted (write-behind, restored on boot), GET/DELETE /hosts endpoints, migration 0016

(build note: F1 initially wrote docs to main checkout — Director relocated + builder rebuilt commits, code trees byte-identical. Known accepted false-positive: "captcha" in a real article's first 4KB over-escalates. Penalty snapshot lags live value by persist interval; decayed-to-zero not re-zeroed in DB until DELETE — documented.)
