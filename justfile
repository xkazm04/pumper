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

# A scope is required, e.g.
#   just search-backfill "--app grants --dataset unified"
#   just search-backfill --all
#
# Rebuild the full-text index from stored records. Run with the server STOPPED.
search-backfill scope:
    cargo run -p pumper-server --bin search-backfill -- {{scope}}

# Install the resulting .wasm into data/plugins/ and `POST /plugins/reload` —
# see README.md § WASM plugins for the copy step.
#
# Build an example WASM plugin from plugins-src/<crate> (detached workspace).
plugin crate:
    cd plugins-src/{{crate}} && cargo build --release --target wasm32-unknown-unknown
