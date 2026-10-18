<!--
This template exists because most changes here are authored in a Claude CLI
session with no second human in the loop (.claude/CLAUDE.md). The checklist is
therefore not a courtesy to a reviewer — it is the list of things that are
enforced SOMEWHERE ELSE than in `cargo test`, and so are the things a green
board does not tell you about.

Delete any section that does not apply. Do not delete a box to make it pass.
-->

## What changed, and why

<!-- One paragraph. The "why" matters more than the "what" — the diff is the what. -->

## How it was verified

<!--
Name the command and its verdict, not the intention. `just ci` runs every rung
CI blocks on (fmt-check, lint, test, audit, plugins-verify, sdk, inventory,
flake-check, harness-test, disk-check). If you ran something narrower, say so.
-->

- [ ] `just ci` is green locally, or the checks on this PR are.

## The things CI cannot see

- [ ] **Docs.** If this changed a user- or API-visible surface — an endpoint or
      param, a dataset shape, an app, a trigger/webhook contract, a config key,
      CLI-observable behavior — the coupled `docs/features/*` page is updated in
      this same change. If it is internal-only (refactor, bugfix with no
      behavior shift, test-only), say which and move on.
- [ ] **The catalog.** A new or renamed app has its `[[source]]` entry in
      `catalog/data-sources.toml` (ONBOARDING.md §10).
- [ ] **The dependency rule.** No app depends on another app or on an engine
      crate; engines depend only on `core` (README.md §Architecture).
- [ ] **The contract.** If a route or response shape moved, `just openapi` and
      `just clients` were re-run and their output is committed. `just
      clients-check` is what turns the omission red.
- [ ] **The bug-fix shape.** If this fixes a bug, the predicate or transform is
      an extracted, named function with a test named after the anti-pattern it
      defends — not a condition buried in a `run()` body (.claude/CLAUDE.md).
- [ ] **New gates.** If this adds a check, there is a fixture that proves it can
      go RED. A gate never observed failing is indistinguishable from one that
      cannot fail.

## Supply chain

<!-- Delete unless this PR touches .github/, Cargo.lock, deny.toml, or scripts/ci/. -->

- [ ] Any new `uses:` in a workflow is pinned to a 40-hex commit SHA, or is
      registered in `.github/unpinned-actions.json` with a reason and an owner.
      `just pin-check` is the gate.
- [ ] Any new workflow declares a top-level `permissions:` block, and any job
      that widens it says why in a comment.
- [ ] New or bumped dependencies pass `just audit` (advisories, bans, sources).

## Risk

<!--
What breaks if this is wrong, and how it would be noticed. "Nothing, it is
test-only" is a complete answer.
-->
