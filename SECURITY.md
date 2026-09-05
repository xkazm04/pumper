# Security policy: pumper

This file describes what IS enforced, and names the gate that enforces it. A
checkbox with no command behind it is worse than an empty list — it is read as a
control. Anything still open is under [Not covered](#not-covered), with the
reason.

## Reporting a vulnerability

Report privately through **GitHub's private vulnerability reporting** on this
repository: *Security → Advisories → Report a vulnerability*. That opens a
channel visible only to the maintainers, so a working exploit never has to sit
in a public issue while it is triaged.

If that form is unavailable to you, open a public issue containing **no
reproduction detail** — just "security report, please open a private channel" —
and wait to be contacted.

This is a single-maintainer, local-first project. Expect a first response within
about a week, and no formal SLA beyond that.

### What is in scope

Anything that lets a **scraped page**, a **configured feed**, or an **operator-
supplied WASM plugin** escape the boundary it is supposed to sit inside:
sandbox escapes from `engine-wasm`, SSRF through the fetcher or `/fetch-proxy`,
SQL injection into the job/dataset store, path traversal out of `data/`,
argument injection into the `claude` subprocess or the Chrome launch.

### What is NOT a vulnerability here

**The absence of inbound authentication is a documented default, not a bug.**
The server binds `127.0.0.1:8088` and, with no `[auth]` section, every route is
unauthenticated and fully mutating — see
[docs/deployment.md § Auth posture](docs/deployment.md). Binding it to a public
interface without adding `[auth] mode = "keys"` or an authenticating reverse
proxy is an operator decision that the docs already refuse to recommend. Reports
that consist of "the API is unauthenticated when exposed to the internet" are
describing that documented posture.

## What actually runs

| Control | Command | When | On failure |
| --- | --- | --- | --- |
| Advisories, bans, and source pinning over every crate | `cargo deny check advisories bans sources` (`just audit`) | every push, every PR, and weekly on cron `17 6 * * 1` — the RUSTSEC database moves without a commit of ours | the `Dependency audit` job fails, blocking the merge |
| Dependency updates | Dependabot, cargo + github-actions ecosystems | weekly, Monday 06:00 | n/a — it opens PRs |
| Workflow token scope | top-level `permissions: contents: read` in every workflow; a fixture asserts each workflow declares one | every push, every PR | `Supply-chain gate fixtures` fails |
| Action pinning register | `just pin-check` — every `uses:` must be a 40-hex SHA or be registered with a reason, an owner, and a ceiling that only moves down | every push, every PR | `Action pinning register` fails |
| SBOM (CycloneDX 1.6, from `Cargo.lock`) | `just sbom`; fixtures in `just supply-chain` | generated and fixture-tested on every push and PR; published per release | `Supply-chain gate fixtures` fails |
| Build provenance + SBOM attestation (Sigstore, keyless) | `actions/attest-build-provenance` + `actions/attest-sbom` | on a `v*` tag | the release job fails and nothing is uploaded |
| Zero-warning lint | `cargo clippy --workspace --all-targets -- -D warnings` | every push, every PR | the `test` job fails |

Waivers live where the tool reads them, not in prose: crate policy and every
exemption in [`deny.toml`](deny.toml), the action-pinning burn-down in
[`.github/unpinned-actions.json`](.github/unpinned-actions.json).

## Verifying a release

Every binary published by [`.github/workflows/release.yml`](.github/workflows/release.yml)
carries two Sigstore attestations signed with the workflow's own OIDC identity —
there is no signing key to leak or rotate. Both are checkable with the GitHub
CLI and no local trust setup:

```bash
# Did this binary come out of this repo's pipeline, from the commit it claims?
gh attestation verify ./pumper-x86_64-unknown-linux-gnu --repo xkazm04/pumper

# Is the SBOM next to it the SBOM for THAT binary, not for some other build?
gh attestation verify ./pumper-x86_64-unknown-linux-gnu --repo xkazm04/pumper \
    --predicate-type https://cyclonedx.org/bom
```

The SBOM is generated from the committed `Cargo.lock` by
[`scripts/ci/sbom.mjs`](scripts/ci/sbom.mjs) — offline, dependency-free, and
byte-for-byte deterministic, so two SBOMs of the same lockfile are identical and
a dependency-set change is visible as a diff. It covers the **cargo dependency
graph only**. It does not describe the host Chrome that `engine-browser` drives
or the `claude` CLI that `engine-claude` shells out to; those are host software
the operator supplies, and `docs/deployment.md` explains why that is deliberate.

Reproduce the same document locally:

```bash
just sbom --out /tmp/pumper.cdx.json
just sbom-summary
```

## Local guardrails

Versioned in [`.githooks/`](.githooks), inert until installed — git hooks are
per-clone and cannot be shipped by a checkout:

```bash
just hooks-install     # points core.hooksPath at .githooks
just hooks-status      # is this clone actually running them?
```

`pre-commit` runs `cargo fmt --check` and, when a workflow is staged, the pinning
gate. `pre-push` runs clippy (`PUMPER_HOOKS_FULL=1` runs all of `just ci`).
Bypass with `PUMPER_SKIP_HOOKS=1` or git's `--no-verify`.

## Not covered

Stated rather than checkboxed, because an unticked box reads like an oversight
and each of these is a decision:

- **SAST (CodeQL/Semgrep).** Not wired up. CodeQL's Rust support is newer than
  this repo's clippy-with-`-D warnings` gate, which is what currently carries
  the load. Adding it is a real gap, not a rejected idea.
- **Secret scanning (gitleaks and friends).** Not wired up. Secrets are read from
  a gitignored `.env` at the repo root (`docs/deployment.md` § Environment), so
  there is no committed-secret surface by design — which is an argument for the
  posture, not a substitute for the scan.
- **Container image scanning.** Not applicable. Nothing is containerized, on
  purpose: `docs/deployment.md` § "Not containerized — on purpose".
- **License policy.** `cargo deny check licenses` is deliberately absent from the
  audit job; `deny.toml` documents the one-step follow-up that turns it on.
- **Actions pinned to commit SHAs.** Every `uses:` is still on a floating tag.
  The register in `.github/unpinned-actions.json` names all of them with an
  owner and blocks any *new* one; draining it needs a network round trip per
  action (`gh api repos/<owner>/<repo>/git/ref/tags/<tag> --jq .object.sha`) and
  is the next thing to do here.
- **Inbound auth by default.** See [What is NOT a
  vulnerability](#what-is-not-a-vulnerability-here) and
  [docs/features/auth.md](docs/features/auth.md).
