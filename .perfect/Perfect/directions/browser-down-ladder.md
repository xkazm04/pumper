---
slug: browser-down-ladder
type: perfect/direction
context: "[[tiered-fetcher]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-10
accepted: 2026-08-10
shipped: 2026-08-11
commit: 65f893e
---

## What & why
When Chrome is down, every `Auto`-strategy fetch fails outright — and hosts the tier
router pinned to browser skip the WORKING http tier first, so the learned router
amplifies the outage exactly where it concentrates traffic. Separately, a Claude-tier
engine error is a fatal `?` instead of the trace-and-exhaust every other tier gets.
The escalation ladder should degrade honestly, not collapse: a dead browser must not
take down fetches that plain HTTP could serve.

## Evidence
- `crates/core/src/fetcher.rs:669–677` — browser engine error under Auto →
  trace_tier_error fall-through with nothing after it.
- `crates/core/src/fetcher.rs:717–721` — "all fetch tiers exhausted".
- `crates/core/src/app.rs:285–305` — tier-memory pin sets req.skip_http, so http was
  never attempted in the same fetch.
- `crates/core/src/fetcher.rs:693` — `self.claude.research(...).await?` fatal.
- `crates/core/src/fetcher.rs:669` — explicit Browser strategy fail-fast (keep).

## Acceptance criteria
- [ ] When the browser attempt fails with an ENGINE error (not Blocked/Thin) and the
      http tier was skipped (skip_http) under Auto/AutoWithResearch, the fetcher falls
      back to the http tier before declaring exhaustion — extracted decision fn +
      anti-pattern test (e.g. browser_down_does_not_kill_pinned_hosts).
- [ ] Claude-tier engine error becomes trace-and-exhaust (TierVerdict::Error entry,
      then the exhaustion error naming every attempted tier) — consistent with the
      other tiers; test. Explicit `Browser` strategy keeps its fail-fast.
- [ ] The fallback http attempt appears in the trace in real chronological order.
- [ ] Blocked/Thin browser verdicts unchanged (bot-wall semantics untouched); the
      fallback goes through the governor like any http attempt.
- [ ] Tier learning stays honest: an http win on the fallback records an http win
      (zeroes strikes) — that is true information, pin correctness follows.
- [ ] Doc-sync: fetching.md ladder section (failure semantics table/paragraph).

## Risks / non-goals
- Non-goal: a global browser-health circuit breaker or engine health endpoint.
- Do not fall back when the browser returned a rendered-but-thin/blocked page — only
  on engine-level errors (launch/connect/timeout).
- Whether caller-set skip_http (vs router-set) also gets the fallback is the builder's
  documented call — state the tradeoff; router-set MUST get it.

## Build record
Shipped `65f893e` (Lot F, opus, 2026-08-11). try_http_tier extracted so the fallback
IS the normal attempt (same acceptance bar, trace shape, governor spacing);
browser_failure_falls_back_to_http narrow: engine errors only, escalating strategies
only, only when http was skipped. Claude tier now traces Error and exhausts; the
exhaustion error names attempted tiers in trace order. Fallback http win records an
http WIN (asserted with app.rs's exact http_lost predicate) — an outage never deepens
the pin that caused it. skip_http provenance untracked → both caller- and router-set
fall back; documented with the tradeoff in fetching.md. 6 async + 2 unit tests.
Review: keep. Noted behavior change: auto_with_research fetch on Claude engine failure
now returns Error::App(exhausted) instead of Error::Claude — no caller matched on the
variant (builder grepped).
