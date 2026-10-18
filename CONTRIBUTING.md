# Contributing to pumper

This file answers one question: **how does a change get from your working tree
into `master`?** Everything else — what the code does, how it is laid out, why
the engines are separate crates — lives in the docs [CLAUDE.md](CLAUDE.md) points
at, and is not repeated here.

Most changes here are authored in a Claude CLI session with no second human in
the loop. So the process below is written for two readers at once, a person and
an agent, and it leans on gates rather than on habits: anything that only holds
because someone remembered it is called out as such.

---

## 1. Set up

```bash
cargo install just          # the canonical task runner (CLAUDE.md §Commands)
just check                  # first run: installs the git hooks, then type-checks
```

- **Run everything from the repo root.** The `.env` loader and the default
  `config.toml` path are both CWD-relative.
- The toolchain is **pinned** in `rust-toolchain.toml`; do not override it. If a
  recipe complains about a missing target (`wasm32-unknown-unknown`), add it to
  the pinned toolchain — `rustup target add wasm32-unknown-unknown` inside the
  checkout — not to `stable`.
- **The hooks install themselves.** The first `just` recipe you run points
  `core.hooksPath` at the versioned hooks in [`.githooks/`](.githooks). Confirm
  with `just hooks-status`; opt out with `PUMPER_NO_HOOKS=1`, uninstall with
  `just hooks-uninstall`. What they do:

  | hook | what it runs | bypass |
  | --- | --- | --- |
  | `pre-commit` | `cargo fmt --check` on staged `.rs`, `just pin-check` on staged workflows | `PUMPER_SKIP_HOOKS=1`, `--no-verify` |
  | `commit-msg` | the conventional-commit shape (`scripts/ci/commit-lint.mjs`) | as above |
  | `pre-push` | `cargo clippy -D warnings`; `PUMPER_HOOKS_FULL=1` runs `just ci` | as above |

  Git hooks are per-clone and bypassable, so **none of them is the rung that
  binds** — each has a server-side counterpart in CI. They exist to move the
  verdict earlier, not to be trusted.

## 2. Before you write code

- Read [CLAUDE.md](CLAUDE.md) whole (commands, architecture, the dependency
  rule), then [`.claude/CLAUDE.md`](.claude/CLAUDE.md) (binding policy:
  doc-sync, the context-map protocol, the shape a bug fix ships in).
- Read `context-map.json` and scope your edits to the relevant context's files.
- **The dependency rule is not negotiable:** apps depend only on `core` plus
  parsing libs; engines depend only on `core`; the server wires everything
  together. An app that depends on another app or on an engine crate is a
  rejected change, not a style note.
- Adding a scraping use case is a four-step contract (new crate → `impl
  ScrapeApp` → register in `crates/server/src/registry.rs` → `[[source]]` entry
  in `catalog/data-sources.toml`). See README.md and ONBOARDING.md §5–7, §10.

## 3. While you write it

Two rules here are enforced somewhere other than `cargo test`, which is exactly
why they are worth stating:

- **A bug fix ships as an extracted, tested function.** The predicate or
  transform gets a name and a test named after the anti-pattern it defends
  (`x_not_y`); only then is it wired into the call site. A fix buried inline in a
  `run()` body is an unguarded fix (`.claude/CLAUDE.md`).
- **A user- or API-visible change updates its `docs/features/*` page in the same
  change.** New/changed endpoint or param, dataset shape, app, trigger or webhook
  contract, config key, CLI-observable behavior. Internal-only changes need no
  doc edit — say which, in one sentence, in the PR.
- **A new gate ships with a fixture that proves it can go RED.** A gate never
  observed failing is indistinguishable from a gate that cannot fail; every
  instrument under `scripts/ci/` has a `*.test.mjs` beside it for this reason.

## 4. Verify it

```bash
just ci     # every rung CI blocks on
```

`just ci` is deliberately the same list of rungs the CI workflow runs, so a green
local run predicts the remote one. Run the narrower recipes while iterating
(`just check`, `just test`, `just lint`, `just fmt`), but push on `just ci`.

The **long lanes** (`just lanes`) are *not* in `just ci` and are not a merge gate:
they are certifications on the nightly clock, and blocking a merge on a
minutes-long certification is how the certification stops happening.

**Exit codes.** Every gate in `scripts/ci/` uses the same three-outcome shape,
and the third one is the one people get wrong:

| code | meaning |
| --- | --- |
| 0 | it checked, and it is clean |
| 2 | it checked and found problems |
| **3** | it **could not check** — unreadable input, missing register, a range that would not resolve |

**A 3 is not a pass.** If a gate exits 3, say so rather than reporting silence as
green.

## 5. Commit

Conventional commits, enforced by the `commit-msg` hook locally *and* by the
`Conventional commits` CI step over every commit a PR contains — so `--no-verify`
buys you a red PR, not a shortcut.

```
<type>(<optional scope>): <subject in the imperative, ≤72 chars>
```

Types: `feat fix docs style refactor perf test build ci chore revert deps`
(`scripts/ci/commit-lint.mjs` is the single definition). Check a message with
`just commit-lint`, or print the rule with `just commit-lint --report`.

## 6. Open a pull request

1. Branch off `master`. Never push to `master` directly.
2. Fill in [the PR template](.github/pull_request_template.md). Its checklist is
   not courtesy — it is the list of things enforced somewhere other than `cargo
   test`, so it is precisely what a green board does not tell you.
3. CI runs on every push to the PR. These are the checks the branch-protection
   rule requires, named exactly as GitHub reports them
   ([`.github/branch-protection.json`](.github/branch-protection.json) is the
   source of truth; `just protection-report` prints the current list):

   - `Format`
   - `test (ubuntu-latest)` · `test (windows-latest)`
   - `Consumer clients (TS SDK, CLI, Python)`
   - `Ship inventory + doc-sync hook + supply chain`
   - `Test harness gates`
   - `Dependency audit`

4. [`.github/CODEOWNERS`](.github/CODEOWNERS) routes the review request. Every
   line resolves to `@xkazm04` today; the file's job is to make the ownership
   *boundaries* explicit so that adding a maintainer is one handle edited per
   area.
5. Merge with a **squash or rebase** — `required_linear_history` is on in the
   declared rule, so a merge commit is refused.

### If you touch the supply chain

Any new `uses:` in a workflow must be pinned to a 40-hex commit SHA, or
registered in [`.github/unpinned-actions.json`](.github/unpinned-actions.json)
with a reason, an owner, and under the ceiling — which only ever moves **down**.
`just pin-check` is the gate; `just pin-report` prints the burn-down. Every
workflow must also declare a top-level `permissions:` block; a job that widens it
says why in a comment.

## 7. Branch protection — the rung everything else hangs off

The rule for `master` is **declared in the repo**, at
[`.github/branch-protection.json`](.github/branch-protection.json), and applied
from it:

```bash
just protection-report        # what it requires, and the apply command
just protection-apply         # needs `gh` authenticated with ADMIN on the repo
```

`just protection-check` runs on every PR (inside the `Ship inventory` job) and
reconciles that declaration against the workflows **in both directions**: a
required check whose job was renamed or whose matrix legs changed is a finding,
and so is a new job that the declaration neither requires nor explicitly excuses.
It cannot verify that GitHub has the rule switched on — a checkout has no token —
so that remains a maintainer action, and the honest statement of the state today
is: *declared and reconciled, applied by whoever holds admin*.

## 8. Adding a second maintainer

This is the one place the repo currently depends on a single person, so the edits
are written down rather than remembered. All four in one PR:

1. **`.github/CODEOWNERS`** — add the handle to the areas they own. The catch-all
   `*` line and the load-bearing seams (`/crates/core/`, `/crates/server/`,
   `/.github/`) are the ones worth splitting first.
2. **`.github/branch-protection.json`** — set `require_code_owner_reviews: true`
   and `required_approving_review_count: 1`. Both are deliberately off today
   because GitHub does not let an author approve their own pull request: with one
   maintainer, requiring an approval means nothing merges, and a rule that blocks
   everything gets switched off. `scripts/ci/branch-protection.test.mjs` covers
   both settings, so the payload change is already tested.
3. **`just protection-apply`** — re-apply, so the change reaches GitHub.
4. **`SECURITY.md`** — add them to the disclosure contact.

## 9. Releases

Push a `v*` tag. [`.github/workflows/release.yml`](.github/workflows/release.yml)
builds the binary per target with `--locked`, generates a CycloneDX SBOM from
`Cargo.lock`, publishes SHA-256 sums, and signs two Sigstore attestations (build
provenance and an SBOM attestation bound to that exact binary). Verify a download
with `gh attestation verify <file> --repo xkazm04/pumper`.

There is **no generated changelog** today: the release notes are what the tag and
the GitHub Release say. Conventional commits are in place, so adding one is a
tooling decision rather than a history problem — it is a known gap, not an
oversight.

## 10. Security

Do not open a public issue for a vulnerability. [`SECURITY.md`](SECURITY.md) has
the disclosure process and the supported-versions statement.
