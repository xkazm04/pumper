# Vision Scan Fix Wave 8 — Adaptive Fetching

> 2 commits, 2 ideas closed + 1 duplicate absorbed (the moonshot-triage runner-up: per-domain learning).
> Baseline preserved: build clean → build clean; tests 45 → 48 (+2 unit +1 integration, 0 failed).

## Commits

| # | Idea | Title |
|---|---|---|
| 1 | f50bc37e | Fleet rate governor learns host limits from 429s |
| 2 | 0e934fdb | Self-learning tier router that skips dead tiers (absorbs 988604cb escalation memory) |

## What was built

- **Adaptive governor**: 429/503 doubles a host's learned extra spacing (1s base, honors larger `Retry-After`, 5min cap) and pushes the host's next slot out so queued peers back off immediately; any healthy response halves it until it drops below the 100ms floor. HTTP engine feeds both signals after every response. In-memory (resets on restart — penalties are transient by nature).
- **Tier router** (`tier_memory`, migration 0015): each metered fetch teaches per-host memory — 3 consecutive HTTP-tier losses flip the host to start at the browser tier (`FetchRequest.skip_http`, noted in the escalation trail); one HTTP win resets. Explicit `Http` strategy always overrides. Persistent (SQLite) — the lesson survives restarts.

## Patterns established

19. **Penalty/reward symmetry for adaptive limits** — double-on-signal + halve-on-health with floor/cap gives recovery without config; keep the learned state next to the enforcement point (governor in-memory, tier memory persistent — matched to each signal's lifetime).
20. **Learn at the metered seam** — `AppContext::fetch` is where per-host learning belongs: it sees the request, the outcome trail, and has storage; engines stay stateless.

## What remains

T9 tail (census blended/YoY, salary-gap API, ARES, SEDIA clean-text, wage bands), remaining moonshots (answer engine/RAG, self-healing scrapers, source scout, session vault), deferred T4/T5/T7 tails. ~215 pending ideas in the backlog.
