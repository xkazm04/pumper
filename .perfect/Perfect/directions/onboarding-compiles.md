---
slug: onboarding-compiles
type: perfect/direction
context: "[[engine-contracts]]"
lens: robustness
status: shipped
size: S
proposed: 2026-08-13
accepted: 2026-08-13
shipped: 2026-08-13
commit: f3e62cd
---
## What & why
`ONBOARDING.md` is described by `CLAUDE.md` as *"the agent-facing contract"* — the primary
extension path, and in practice the first thing a CLI agent reads before adding an app. Its
engine section teaches **three consecutive snippets that are wrong**, and the most prominent one
teaches the exact bypass the repo has both a compiler guard and a test guard against.

- `ONBOARDING.md:345-353` is headed `### ctx.engines.claude — Researcher::research(...)` and
  ends `let out = ctx.engines.claude.research(req).await?;`. `EngineSet::claude` is
  **`pub(crate)`** (`engine.rs:1044`), so that line cannot compile from an app crate — and
  `crates/core/tests/llm_chokepoint.rs:169-199` asserts no app crate may contain the string
  `engines.claude` **at all**. `ONBOARDING.md:261` repeats it. The real idiom is
  `ctx.research(request)`, which is what the doc's own named template
  (`crates/apps/research/src/lib.rs:417`) actually does.
- `ONBOARDING.md:266` gives `ctx.plugins.run(name, doc)`. Real signature takes three arguments
  (`plugin.rs:81`).
- `ONBOARDING.md:224-231` presents `ScrapeApp` as five methods. It has seven (`app.rs:896-934`);
  the two missing ones include `manifest()`, which is load-bearing — a declared schema makes
  enqueue enforce 422, and `registry.rs:395` asserts at least five apps ship rich manifests.

Why this is worth a slot rather than a five-minute doc edit: **the reason all three survived is
mechanical.** `scripts/docs/feature-doc-map.json` maps source globs only to `docs/features/*`
pages; `ONBOARDING.md` is the target of **no map entry at all**, so no doc-sync signal has ever
pointed at it. Fixing the text without fixing that leaves the next drift equally free.

The user moment: *"I followed the onboarding doc to add an app and the first engine call I copied
didn't compile — and when I worked around it, I was reaching past the metering seam."*

## Evidence (Director-verified)
- `crates/core/src/engine.rs:1044` — `pub(crate) claude: Arc<dyn Researcher>`; `:1069`
  `pub(crate) fn researcher()`.
- `crates/core/tests/llm_chokepoint.rs:169-199` — bans `engines.claude` in app crates.
- `ONBOARDING.md:345-353`, `:261`, `:266`, `:224-231`.
- `crates/core/src/plugin.rs:81` — `run(&self, name: &str, input: &str, params: &Value)`.
- `crates/core/src/app.rs:896-934` — seven methods; `:914` `requires()`, `:928` `manifest()`.
- `crates/apps/research/src/lib.rs:417` — the idiom that actually compiles.
- `scripts/docs/feature-doc-map.json` — no entry whose target is `ONBOARDING.md`.

## Acceptance criteria
1. Every engine/extension snippet in `ONBOARDING.md` §5–§7 is **correct against today's code** —
   verified symbol by symbol, not eyeballed. The three named above are the known set; sweep the
   section for others rather than fixing only what this note lists.
2. The `ScrapeApp` surface in the doc matches the trait, including `manifest()` and what it buys
   (the 422 enforcement is the reason an app author cares).
3. A **guard** so this cannot silently rot again. The repo's idiom is an inventory/EXPECTED-diff
   test (`routes/mod.rs`, and now `crawl`'s `OUTPUT_SHAPE`). Extract the symbols/signatures the
   doc claims and assert they exist — a test that fails when the doc and the code disagree. Keep
   it cheap and non-brittle: pin the things that were actually wrong (method names, arity,
   privacy of `engines.claude`), not prose.
4. `ONBOARDING.md` gains a `feature-doc-map.json` entry so edits to the engine/app-trait sources
   nudge it. Coordinate with [[doc-sync-hook-fires]] — that direction owns the hook script
   itself; you own the map entry. If the two collide, the map entry is yours.
5. Do not "fix" the doc by relaxing `pub(crate)` on `EngineSet::claude`. That privacy plus the
   chokepoint test is a deliberate metering guard (`engine.rs:1036-1039`); the doc is what is
   wrong.

## Risks / non-goals
- Doc + guard only. No behavior change to any engine or trait.
- Do not rewrite `ONBOARDING.md` wholesale — it is long and mostly correct; fix what is false.
- Keep the guard from becoming a maintenance tax: a test that breaks on every prose edit is worse
  than the drift it prevents.

## Build record
(to fill during build)
