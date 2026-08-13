# CLAUDE.md

This file provides guidance to Claude Code when working with code in this repository.

<!-- vibeman:context-map:start -->
## Context Map

This project has a Vibeman-generated context map at `context-map.json` (repo root). It maps every file to a feature ("context"), grouped by business domain. **Before editing code, read `context-map.json` to find the relevant context and scope your changes to its `filePaths`.** The `index` field is a quick one-line-per-context overview. If you change which files a context owns, update `context-map.json` to match (or run Vibeman's refresh) so it stays accurate.
<!-- vibeman:context-map:end -->

## Documentation Sync — one surface, same-session enforcement

`docs/features/` is the implemented-product reference for users, developers, and CLI agents ([index](../docs/features/README.md)). Development happens through Claude CLI sessions with no second human reviewer to catch drift, so enforcement is **per-session gap-prevention, not periodic catch-up**: drift compounds across sessions much faster than a batch pass can clear it.

### The rule

When a turn edits **feature source** with **user/API-visible** effect — a new or changed endpoint/param, dataset shape, app, trigger/webhook contract, config key, CLI-observable behavior — update the coupled feature doc **in the same session**.

If the change is internal-only (refactor, bugfix without behavior shift, test-only), no doc update is needed. Dismiss the hook with one short sentence naming why.

### Source → doc map (single source of truth)

[`scripts/docs/feature-doc-map.json`](../scripts/docs/feature-doc-map.json) maps source globs to feature docs. A Stop hook (`.claude/settings.json` → `node scripts/docs/check-doc-sync.mjs`) walks each turn's edits and exits 2 with a reminder when mapped source changed without a `docs/features/*` edit.

**What "this turn's edits" means.** The hook scans the transcript backwards from the end to the most recent *genuine* user prompt and collects the `Edit`/`Write`/`MultiEdit`/`NotebookEdit` calls in between. Tool results are recorded as `type:"user"` + `message.role:"user"` entries too, and treating one as the turn boundary is what kept this hook silent for its entire life — so the boundary predicate rejects anything that is a `tool_result` by content shape, carries `toolUseResult`/`sourceToolAssistantUUID`, or is `isMeta`. It then stays silent when: the turn touched any `docs/features/*`; every edit matched `SKIP_PATTERNS` (tests, `docs/`, `catalog/`, `plugins-src/`, `target/`, `Cargo.lock`, `.claude/`, `.perfect/`, `scripts/`); an edit landed outside this checkout (sessions routinely edit sibling repos); or nothing matched the map. It never blocks — exit 2 is a reminder you may dismiss in one sentence.

**Verify it, don't infer it.** `just doc-sync` runs `scripts/docs/check-doc-sync.test.mjs` (node:test, no deps) against redacted transcript fixtures in `scripts/docs/fixtures/`. A quiet turn and a broken hook look identical from the outside, so a silent session is not evidence that the hook works. You can also replay any recorded transcript through it directly: `node scripts/docs/check-doc-sync.mjs <transcript>.jsonl`. Calibrated over all 31 recorded transcripts of this project it fires on 2.6% of turns (8/308; 10.3% of turns that edit this repo), never more than once per session.

**When you add a new feature area (new crate, new app, new server module), add a map entry and its feature doc in the same change.** Feature docs should name: what it does, the API/params surface, the data model (tables/datasets), and known gaps. Keep future-looking ideas out — the Vibeman backlog holds those; docs describe what IS.

Other durable references: `docs/harness/harness-learnings.md` (structural facts + conventions + pattern catalogue — read before large changes), `docs/harness/vision-scan-2026-07-10/` (scan INDEX, wave summaries, trigger design doc).

## Bug fixes ship as extracted, tested functions

A bug fix buried inline in a `run()` body is an unguarded fix — this repo's guard coverage correlates almost perfectly with whether the fix was extracted into a named pure function (measured across 15 bug classes, /architect 2026-07-26). When fixing a bug: extract the predicate/transform into a named function, add a test named after the anti-pattern it defends (`x_not_y` style), and only then wire it into the call site. Conventions ("all sites use helper X") are enforced with an inventory test (EXPECTED-diff idiom in `crates/server/src/routes/mod.rs`), not with a sentence in a doc.
