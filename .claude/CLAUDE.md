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

**When you add a new feature area (new crate, new app, new server module), add a map entry and its feature doc in the same change.** Feature docs should name: what it does, the API/params surface, the data model (tables/datasets), and known gaps. Keep future-looking ideas out — the Vibeman backlog holds those; docs describe what IS.

Other durable references: `docs/harness/harness-learnings.md` (structural facts + conventions + pattern catalogue — read before large changes), `docs/harness/vision-scan-2026-07-10/` (scan INDEX, wave summaries, trigger design doc).
