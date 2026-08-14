# Pumper task runner — `just <recipe>`.
#
# Why `just` and not `make`: neither is part of the toolchain today, so this
# picks the one that costs a Rust developer nothing extra — `cargo install just`
# uses the toolchain the repo already requires, and recipes are literal commands
# with no tab-significance, `.PHONY`, or implicit-rule baggage. `make` would need
# a POSIX toolchain that this Windows box doesn't ship.
#
# Recipes run under `sh` (just's default shell): Git Bash on Windows, the system
# shell on Linux/CI. Every command below is copied from README.md, ONBOARDING.md
# §8, or .github/workflows/ci.yml — nothing here is invented.

# List the recipes.
default:
    @just --list

# --- run ---------------------------------------------------------------------

# `--bin pumper` is required: the package also ships the `reindex` and
# `search-backfill` maintenance binaries, so a bare `cargo run -p pumper-server`
# is ambiguous.
#
# Boot the server (http://127.0.0.1:8088 by default; see [server] in config.toml).
run:
    cargo run -p pumper-server --bin pumper

# Boot the server with verbose logs (ONBOARDING.md §8).
dev:
    RUST_LOG=debug cargo run -p pumper-server --bin pumper

# --- build / verify ----------------------------------------------------------

# Fast type-check of the whole workspace.
check:
    cargo check --workspace

# Build the binaries (debug); append `--release` yourself for an optimized build.
build:
    cargo build -p pumper-server

# Unit + integration tests, exactly as CI runs them.
test:
    cargo test --workspace

# The #[ignore]d environment-dependent tests (real Chrome, built wasm, timing).
test-ignored:
    cargo test --workspace -- --ignored

# Lint exactly as CI does.
lint:
    cargo clippy --workspace --all-targets

# Format in place.
fmt:
    cargo fmt

# Format check exactly as CI does (fails on drift).
fmt-check:
    cargo fmt --check

# The full CI job (.github/workflows/ci.yml) in one command.
ci: fmt-check lint test

# The doc-sync Stop hook (.claude/settings.json -> check-doc-sync.mjs) is the
# repo's only same-session doc-drift defense, and it is invisible when it works:
# a turn that needs no reminder looks exactly like a broken hook. This is how you
# tell them apart without waiting for a nag. Node only, no dependencies.
#
# Replay one recorded transcript through it instead:
#   node scripts/docs/check-doc-sync.mjs ~/.claude/projects/<project>/<id>.jsonl
# — exit 2 means it would have nagged, exit 0 means it would have stayed quiet.
#
# Prove the doc-sync hook still fires: runs its fixture suite.
doc-sync:
    node --test scripts/docs/check-doc-sync.test.mjs

# --- maintenance -------------------------------------------------------------

# Recompute every record's SimHash. Run with the server STOPPED.
reindex:
    cargo run -p pumper-server --bin reindex

# Repairs NOTHING, and needs the server RUNNING (`just run`) — unlike reindex and
# search-backfill, which need it stopped. Performs full scans, so run it on demand
# rather than on a timer. `just doctor 8088` for a non-default port.
#
# Read-only store integrity report; empty `findings` means the store is healthy.
doctor port='8088':
    curl -s "http://127.0.0.1:{{port}}/datasets/doctor"

# Deletes nothing, and needs the server RUNNING. `just retention-preview 90`
# models a 90-day window the config has not enabled.
#
# Retention dry run: reclaimable artifact bytes per app (pinned bytes broken out).
retention-preview days='':
    #!/usr/bin/env sh
    # Omit `?days=` entirely when no argument is given, so the server applies the
    # configured [storage] artifact_retention_days rather than parsing an empty
    # value.
    if [ -n "{{days}}" ]; then q="?days={{days}}"; else q=""; fi
    curl -s "http://127.0.0.1:8088/retention/preview$q"

# Gates nothing and writes nothing, and needs the server RUNNING. Replays the
# STORED verdicts — it does not re-judge history against today's rules.
# `just enforcement-preview extractor` scopes it to one app.
#
# What `[resilience] enforce = true` would have done: per-source would-be state
# timeline, the counts it would have gated, and `ready` + `not_ready`.
enforcement-preview app='':
    #!/usr/bin/env sh
    # Omit `?app=` entirely when no argument is given, so the server replays the
    # whole fleet rather than filtering on an empty app name.
    if [ -n "{{app}}" ]; then q="?app={{app}}"; else q=""; fi
    curl -s "http://127.0.0.1:8088/enforcement/preview$q"

# What the DataHub governance actuator would do RIGHT NOW: which schedules it
# would disable, which apps it would pause, which syncs it would enqueue.
# Writes nothing, and deliberately works with `[datahub] govern = false` —
# it is the answer to the question that gates turning governance on.
# Needs the server RUNNING and `[datahub] enabled = true`.
datahub-preview port='8088':
    curl -s "http://127.0.0.1:{{port}}/datahub/governance/preview"

# A scope is required, e.g.
#   just search-backfill "--app grants --dataset unified"
#   just search-backfill --all
#
# Rebuild the full-text index from stored records. Run with the server STOPPED.
search-backfill scope:
    cargo run -p pumper-server --bin search-backfill -- {{scope}}

# Install the resulting .wasm into data/plugins/ and `POST /plugins/reload` —
# see README.md § WASM plugins, or use `just plugins-install`, which builds AND
# installs EVERY plugin under plugins-src/ with the right installed name.
#
# Build ONE example WASM plugin from plugins-src/<crate> (detached workspace).
plugin crate:
    cd plugins-src/{{crate}} && cargo build --release --target wasm32-unknown-unknown

# Builds AND installs every plugins-src crate, which `just plugin` does for one.
#
# Nothing else in this repo compiles them: each plugins-src crate carries its own
# `[workspace]` (they target wasm32, not the host), so `cargo test --workspace`
# never sees them. This recipe and the `plugins` steps in
# .github/workflows/ci.yml are the ONLY things that build them — which is why
# both go through here rather than each spelling the loop out.
#
# Without this step every configured `plugins.predicate` / `plugins.transform`
# hook hits the unknown-plugin path: predicates silently pass and transforms
# silently no-op (fail-open by design — see docs/features/trigger-plugins.md),
# and the four artifact-dependent #[ignore]d tests have nothing to run against.
# Re-run after editing a plugin, then
# `curl -X POST localhost:8088/plugins/reload` to hot-swap without a restart.
plugins-install:
    #!/usr/bin/env sh
    set -e
    if ! rustup target list --installed 2>/dev/null | grep -q '^wasm32-unknown-unknown$'; then
        echo "error: the wasm32-unknown-unknown target is not installed." >&2
        echo "       run: rustup target add wasm32-unknown-unknown" >&2
        exit 1
    fi
    mkdir -p data/plugins
    for dir in plugins-src/*/; do
        crate=$(basename "$dir")
        # cargo underscores the artifact name; the INSTALLED file stem is the
        # plugin name the host loads — what a job's `params.plugin` and a
        # trigger's `plugins.predicate.plugin` name — so it must be the
        # hyphenated crate name. `title-extractor` is the one exception: README
        # and the `plugin` app's examples have always called it `title`, and
        # that rename used to exist only as a line of README prose, which is why
        # `just test-ignored` could not pass on a clean machine.
        artifact=$(echo "$crate" | tr '-' '_')
        case "$crate" in
            title-extractor) name=title ;;
            *) name="$crate" ;;
        esac
        ( cd "$dir" && cargo build --release --target wasm32-unknown-unknown )
        cp "$dir/target/wasm32-unknown-unknown/release/$artifact.wasm" \
           "data/plugins/$name.wasm"
        echo "installed data/plugins/$name.wasm"
    done

# The plugin crates' OWN unit tests, on the HOST target (a detached workspace
# gets no coverage from `just test`). Crates with no `#[cfg(test)]` module pass
# with zero tests, which is the point: adding one is enough to get it run.
plugins-test:
    #!/usr/bin/env sh
    set -e
    for dir in plugins-src/*/; do
        echo "== $dir"
        ( cd "$dir" && cargo test )
    done

# Everything CI's plugin steps run, in one command — so "does my plugin change
# break the host ABI?" is answerable locally with the same commands.
#
# A shipped hook plugin that stops exporting `extract_v2` still COMPILES; what
# catches it is loading the built artifact and asking the host whether it is
# executable, which is exactly what the #[ignore]d artifact tests do. Without
# this recipe such a break reaches production as a fail-open hop (an ungated
# gate), never as a red build.
plugins-verify: plugins-install plugins-test
    cargo test -p pumper-engine-wasm --test plugins -- --ignored
    cargo test -p pumper-server e2e::trigger_plugins -- --ignored

# --- live verification --------------------------------------------------------

# Reuses an existing debug build (set CARGO_TARGET_DIR to point at one) rather
# than rebuilding; add -SkipBuild to fail instead of building if it's missing,
# -KeepScratch to leave the scratch dir behind on exit for debugging.
#
# Boots the real binary against an isolated scratch config (port 18099, its
# own DB/artifacts/search-index dir under the OS temp folder), drives one real
# job end-to-end, curls the doctor/retention/enforcement-preview/openapi/
# receipt surfaces, and tears down — see docs/features/http-api.md
# "Smoke verification". PowerShell 7 (`pwsh`) required.
smoke *args:
    pwsh -NoProfile -File scripts/smoke.ps1 {{args}}
