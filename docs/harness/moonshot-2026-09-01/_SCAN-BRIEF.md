# Moonshot scout brief — Pumper, 2026-09-01 (scan-sweep, ad-hoc lens `moonshot-architect`, strategy: develop)

You are ONE of eight read-only scouts, each assigned one group of the context map. Your job is to
read your group's code for real and return the **next generation of moonshots**: L or XL ideas
(architecture-grade, multi-module, new capability or paradigm shift) that would materially raise what
this product can do. You are NOT hunting bugs, polish, or refactors — those are other lenses. A card
that is really an M-sized fix is out of scope; drop it.

## The repo in one paragraph
Pumper is a local-first scraping service: one Rust binary (axum HTTP API + durable SQLite/WAL job
queue + worker pool + cron scheduler + triggers + webhooks + SSE + DataHub emitter) that scrapes
through pluggable engines (archive/Wayback tier, reqwest HTTP, chromiumoxide Chrome, `claude -p`
research subprocess, wasmtime WASM UDF host, tantivy search) and ships ~30 "apps" (one crate per
use case: grants, Czech labor, US census, trades pricing, crawler, extractor, page monitor, research,
source provisioner, browser transact, dataset peering...). A TypeScript consumer SDK (`@pumper/sync`)
mirrors canonical datasets. Cargo workspace; apps depend only on `core`. Read `CLAUDE.md`,
`README.md` (esp. the API-by-example and Roadmap sections), `ONBOARDING.md` §7–10, and
`docs/features/README.md` for orientation, then your group's files (listed in your group file).

## HARD "never re-propose" list
All 44 moonshots of the 2026-07-30 scan (M01–M44) are SHIPPED. Do not re-propose them or
rephrasings; you may build ON them (a v2 that a shipped v1 makes possible is fair game, but the card
must say which shipped seam it extends and why the v1 is insufficient). The list:
`docs/harness/moonshot-2026-07-30/INDEX.md` (table of M01–M44) — read it. Also excluded: README
"Roadmap ideas — Still open" items (TLS/JA4 impersonation, streaming crawler download, screenshots,
job cancellation, proxy/UA rotation), the pending items in `.perfect/Architect/backlog.md`, and the
190 /perfect direction slugs in `dirs.txt` next to this brief (mostly shipped hardening; skim it).
Also gated by operator choice, do not propose flipping: M04 economics enforce mode.

## Lenses you carry
- `moonshot-architect` (ad-hoc, primary): what would a founder build here in 2027 that the current
  substrate makes cheap and the market would pay for or a fleet of agents would depend on?
- `feature-scout`, `innovation-catalyst`, `integration-planner`, `business-strategist` (develop
  deep tier): missing capabilities, AI/agent-native shifts, integrations, business value.
- Registry: `.ai/registry-map.json` maps each context to governing subjects in
  `../ai-registry/knowledge/software-engineering/` and `llm-observability/`. If a subject's golden
  path or technique inspires or constrains a card, name its slug in the card. Optional — do not
  spend more than a few minutes there.

## Method
1. Read every file in your group's contexts (they are listed with paths in your group file). Form no
   verdicts while reading; collect evidence — what the substrate already CAN do, what it stops just
   short of, which shipped seams are default-OFF or half-wired (e.g. `docs/features/*.md` "Known gaps"
   sections are gold: read the feature docs for your group's contexts).
2. Only then propose. **3 to 6 cards**, ranked by your own conviction. Every card must be L or XL.
   Every claim cites `file:line`. Ground each card in something you actually read — a seam, a
   default-off flag, a data shape that is one join away from a product.
3. Prefer cards where several of your contexts converge on the same missing capability; say which
   contexts each card touches (the coordinator ranks by cross-scout convergence).
4. Be honest about the gate: contract (schema/API/persisted format), policy (spend/security/privacy),
   irreversible, or none.

## Output — STRICT
Write your result as JSON to the path given in your task (one object per card, in a JSON array),
AND return the same JSON as your final message. Do not modify ANY file under the repository. The
card shape:

```json
{
  "title": "<= 80 chars; noun phrase for a capability>",
  "lens": "moonshot-architect | feature-scout | innovation-catalyst | integration-planner | business-strategist",
  "contexts": ["<context-name>", "..."],
  "group": "<your group>",
  "size": "L | XL",
  "effort": 1-10, "impact": 1-10, "risk": 1-10,
  "gate": "none | contract | policy | irreversible",
  "extends": "<shipped seam or M-number it builds on, or 'new'>",
  "body": "## Summary\n...\n\n## Description\n...(file:line for every claim; name the technique/subject when one applies)\n\n## Flow\n- build step 1\n- build step 2\n...\n\n## Expected impact\n...(who notices, what changes, how measured; one sentence on what could break)\n\n## Evaluation\nClaim: user|performance|resilience|quality|other - <what it promises>\nBefore: <the figure today — a count, a reproduced behaviour, the sample you looked at>\nAfter: <the same figure under the change, or the missing instrument named>\nMethod: probe|simulation|gate - <what you did>\nResult: unmeasurable (moonshots are, say what instrument would measure it) | better\nGate: none|contract|policy|irreversible",
  "evidence": "<code block or file:line list — the proof, not prose>"
}
```

Write the body sections exactly in that order with those `## ` headings. Keep the JSON valid
(escape newlines as \n). If, after reading everything, you honestly find fewer than 3 L+ ideas, return
what you have and say in a final `"note"` field what you read in full and which hypotheses you
traced and discarded.
