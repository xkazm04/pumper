---
slug: fetch-tier-verdicts
type: perfect/direction
context: "[[Tiered Fetcher & Politeness]]"
lens: robustness
status: shipped
size: M
proposed: 2026-07-13
accepted: 2026-07-13
shipped: 2026-07-13
commit: 11ca817
---

## What & why
The "is this tier's answer good enough" verdict is a single char-count: a 200 "Enable JavaScript" page or a 403 challenge page passes as content; the browser tier has no success signal at all; a host returning 404s gets REWARDED with faster spacing; HTTP-date `Retry-After` is dropped; thresholds/penalties are compile-time consts. Fix the verdict layer and make it configurable.

## Evidence
- Char-count-only verdict: crates/core/src/fetcher.rs:121-123 (http), :143-144 (browser)
- 404 rewards the governor: crates/engine-http/src/lib.rs:83-86
- Retry-After seconds-only: governor/engine-http retry handling (engine-http/src/lib.rs:119-131)
- Compile-time consts: fetcher.rs:17 (min chars), governor.rs:17-19 (penalty base/cap/floor)

## Acceptance criteria
- [ ] Bot-wall/challenge detection (status 403/429/503 + known challenge-page text patterns) escalates instead of passing.
- [ ] Browser tier gets an error-page heuristic (not just char count).
- [ ] 404 (and 4xx generally, except 429) no longer decays penalties; only genuinely healthy responses reward.
- [ ] Both Retry-After forms (seconds + HTTP-date) honored.
- [ ] `[fetcher]` / `[governor]` config keys for min_content_chars and penalty bounds — `#[serde(default)]` + manual `Default` (repo law).
- [ ] Unit tests per heuristic.

## Risks / non-goals
- Risk: over-eager challenge detection escalating good pages — patterns must be conservative and tested.

## Build record
(pending)
