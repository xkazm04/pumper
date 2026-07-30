# Moonshot Batch 7 — Platform Power (2026-07-31)

> 7 commits on `vibeman/moonshot-batch7-2026-07-30` (off merged master `9b626e3` / PR #26). One wave of 5 agents.
> Baseline preserved: tests 724/0 → **772/0** (+48). Migrations 0031 (watch sinks) + 0032 (trigger plugin hooks) consumed.

## Commits

| Commit | Item | Summary |
|---|---|---|
| `29e80de` | M08 | Link graph — crawl/edges (out-degree cap, dedup, top_linked); additive core hook (links never reached the sink) |
| `12401e0` | M15 | WASM trigger predicate + transform plugins on the extract_v2 envelope; host-re-stamped provenance keys; fail-open loud |
| `ce419c3` | M32 | Medicare oracle — fetch_bytes seam (engine-traits#2-LITE), RVU ZIP→PPRRVU CSV→cms/fee_schedule + release diffs; layout pinned, loud drift; zip dep app-local |
| `142ebe7` | M22 | Sink connectors — watch sink webhook|file|slack, all transports through the single deliver() funnel (same DLQ/backoff/log) |
| `8c5c40c` | M24 | VCR — cassette record of fetch+research, strict replay ($0, ReplayMiss typed, no governor/tier training); crawl bypass documented |
| `27dba84` | — | integration + lockfile + design doc |

## Operator notes

- cms-fee-schedule's ZIP/CSV layout is pinned-but-NOT-live-verified (no CI download) — watch the first fresh-release parse; watcher behavior stands if parsing fails.
- VCR flags are job params (`record:true`, `replay_of`); browser replay returns recorded final HTML.
- Trigger plugin `.wasm` artifacts must be compiled from plugins-src to be used (tests stub the host).

## Remaining

Batch 8: M35 taxonomy-as-data, M38 salary nowcast, M09 wrapper induction, M25+M26 DataHub pair. Batch 9: M01, M06, M17, M28, M30. Gated: M04 enforcement.
