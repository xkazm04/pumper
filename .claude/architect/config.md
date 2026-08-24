# architect overlay - pumper

Project specifics for the `/architect` lane skill (`ai-registry/skills/architect`, linked at
`.claude/skills/architect`). The lane body is the method; everything below is what pumper is.
Extracted 2026-08-24 from the project-owned copy the lane replaced.

pumper is a Rust workspace (axum job server, tiered fetch engines, declarative extraction,
dataset store, `ScrapeApp` crates). Taxonomy comes from `context-map.json`, structural facts
from `docs/harness/harness-learnings.md`, validation is cargo-based. It shares the repo vault
with `/perfect` but owns its own `Architect/` subtree.

## Vault

```bash
if [ -d "C:/Users/mkdol/Documents/Obsidian/pumper" ]; then
  VAULT="C:/Users/mkdol/Documents/Obsidian/pumper"
else
  VAULT="<repo>/.perfect"   # create if missing; Obsidian-openable, committed by default
fi
```

`Architect/` subtree: `scans/`, `decisions/`, `backlog.md`, `strong-patterns.md`,
`weak-patterns.md`, `coverage.md`, `architect-preferences.md`. `Lessons/{date}-architect.md`
lives in the **shared** `Lessons/` dir - create the dir if missing, never recreate an existing
file.

## Reference files

- `context-map.json` (repo root) - feature/context map. Resolves area scope and target file
  lists (`groups[].contexts[].filePaths`, `index` for the overview). Missing -> stop; it is
  Vibeman-generated and the user must refresh it.
- `docs/harness/harness-learnings.md` - structural facts, conventions, pattern catalogue.
  **The most important input for architect; read in full, first.**
- `.claude/CLAUDE.md` - project rules (context-map discipline, docs-sync enforcement).
- `docs/features/README.md` + relevant `docs/features/*.md` - the implemented-product surface.
- `$VAULT/Perfect/Perfect.md` (if present) - skim so structural work does not collide with an
  in-flight `/perfect` direction.

Staleness: if `context-map.json`'s `generatedAt` is >30 days old or
`git log --oneline <generatedAt>.. | wc -l` > 200, warn that area scoping may be stale.

## Themes (scan mode)

```
  2. error-handling      - Result/anyhow/thiserror discipline, error surfacing to API + logs
  3. async-patterns      - tokio usage, spawn/join discipline, cancellation, blocking-in-async
  4. trait-boundaries    - engine capability traits, ScrapeApp contract, framework-vs-app split
  5. data-modeling       - dataset shapes, store schema, migrations, change-detection contracts
  6. config-surface      - config.toml keys, defaults, validation, drift between docs and code
  7. api-surface         - route/handler consistency, status codes, params, response envelopes
  8. testing-strategy    - what's tested at which layer, fixture duplication, e2e gaps
  9. observability       - tracing/log consistency, health reporting, event/webhook telemetry
```

Theme -> angle swaps (angle library: 1 usage map, 2 type/contract, 3 failure mode,
4 performance surface, 5 test coverage):

- `error-handling` -> 1, 2, 3, 5
- `async-patterns` -> 1, 2, 3, 4
- `trait-boundaries` -> 1, 2 + "framework-vs-app leakage" + "capability trait completeness"
- `data-modeling` -> 1, 2 + "migration history" + "schema-vs-struct drift"
- `config-surface` -> 1, 2 + "config-vs-docs drift" + "default/validation consistency"
- `api-surface` -> 1, 2, 3 + "docs/features parity" (the docs-sync rule makes drift a
  first-class smell)
- `testing-strategy` -> 5 deeply + "fixture duplication" + "harness reach"
- `observability` -> 1, 3 + "tracing span/field consistency" + "silent-failure audit"

## Areas (area mode)

```
  2. runtime      - Scraping Runtime Core (fetcher, politeness, app/job model, capability traits)
  3. extraction   - Data Extraction & Storage (crawler, extraction engine, dataset store)
  4. engines      - Scraping Engines (search index, WASM sandbox, http/browser/claude engines)
  5. server       - Job Server & API (config/catalog, events/webhooks, worker/cron, datahub, registry, routes)
  6. apps         - the ScrapeApp fleet (funding, labor-market, content/research crates - the three app groups combined)
  7. clients      - clients/ (TypeScript SDK) and its contract with the API
```

Options 2-6 map to groups in `context-map.json`; resolve the file list from the matching
groups' contexts' `filePaths`.

## Risk scale

1 (low, isolated) ... 5 (production-critical surface: **worker loop, dataset store, fetch
tiering**).

## Sub-agent prompt preamble

```
You are scanning the pumper codebase (Rust workspace at <repo>) for {angle name}.
Background: {1 paragraph from docs/harness/harness-learnings.md relevant to the theme}
```

## Gates / validation baselines

Capture at pre-flight and record in the ADR; later commits are judged as *delta vs baseline*,
never absolute.

```bash
cargo check --workspace
cargo clippy --workspace 2>&1 | tail -5    # baseline warning count
cargo test --workspace                      # baseline pass/fail
```

Per-commit bar: check errors must not increase, clippy warnings <= baseline + 5, tests at the
baseline rate. Final sweep re-runs all three fully (`--workspace`) and exercises real code
paths where possible (run the server, hit the route, run the app). Any unverified checklist
item -> the ADR stays `in-progress` with a "needs verification" note.

## Git discipline - shared checkout

Multiple CLI sessions commit into this tree concurrently. Default is **commit on the current
branch**; a `architect/{slug}` branch only when the user explicitly asks.

**Never bare `git commit` in this tree.** Concurrent sessions pre-stage their work in the
shared index; a bare commit (even after `git add <your paths>`) sweeps their staged changes
into yours. Use the pathspec form, which commits only the named paths and leaves the rest of
the index untouched (learned the hard way, first run, 2026-07-26):

```bash
git commit -m "architect: <step title>" -- <exact paths>
```

Forbidden at every phase: `git stash`, `git reset --hard/--merge`, `git restore` /
`git checkout --` on any path, `git clean`, `git add -A` / `.` / `-u`, `--amend`,
`--no-verify`. If a conflicting path cannot be resolved, abort and queue the decision back
with `blocked: working-tree-conflict`.

Do NOT require a clean working tree. Classify each dirty path: **in-flight by someone else**
(leave strictly alone), **pre-existing in your touch zone** (surface: commit theirs first /
commit on top <- default / abort), **yours from this session** (normal).

## Docs-sync - non-negotiable

If a commit changes a user/API-visible surface (endpoint, param, dataset shape, config key,
trigger/webhook contract, CLI-observable behavior), update the coupled `docs/features/*.md`
**in the same session** per `.claude/CLAUDE.md` - the Stop hook enforces it. A new feature
area needs its `scripts/docs/feature-doc-map.json` entry + feature doc in the same change.
Internal-only refactors: dismiss the hook with one sentence.

## Codification vehicles (Phase 7B)

```
  1. lint-config    - workspace lints in Cargo.toml ([workspace.lints.clippy/rust]) or clippy.toml,
                      when a clippy/rustc lint (or a level bump) mechanically catches the anti-shape
  2. docs-harness   - append a section to docs/harness/harness-learnings.md (read before large changes)
  3. docs-claude    - append a convention to .claude/CLAUDE.md (loaded into every session)
  4. test-guard     - a structural test: a Rust #[test] that walks the tree / asserts the invariant,
                      or a scripts/ check wired like scripts/docs/check-doc-sync.mjs (Stop hook)
  5. multiple       - combination (each vehicle = a separate atomic commit)
```

`lint-config` execution: add the lint, run `cargo clippy --workspace`, count new warnings.
More than 200 new warnings -> too noisy; pause for guidance.

## Self-reflection hook

If a run discovers a structural fact future sessions need, add it to
`docs/harness/harness-learnings.md` tagged with the run date. Architect runs are especially
prone to surfacing unmapped boundaries.

## Division of labour

`/perfect` proposes product directions (features, API elevations); `/architect` proposes
structural changes (conventions, bug classes, boundaries). A finding that is really a feature
goes to the `/perfect` vault, not the architect backlog.
