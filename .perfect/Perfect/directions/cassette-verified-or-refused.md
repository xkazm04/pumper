---
slug: cassette-verified-or-refused
type: perfect/direction
context: "[[vcr-testing]]"
lens: robustness
status: accepted
size: M
proposed: 2026-08-13
accepted: 2026-08-13
shipped: —
commit: —
---

## What & why

Replay sells exactly one property — *the bytes you get back are the bytes that run recorded* —
and the loader never checks it.

1. **Identity is trusted, never recomputed.** `Cassette::load` keys its map on the *deserialized*
   `entry.req_hash` field; `resolve` looks up the *computed* hash of the incoming request.
   Nothing ever asserts `entry.req_hash == req_hash(&entry.method, &entry.url)`. An entry whose
   `url` says one thing and whose `req_hash` says another is served for the request the hash
   names, and the replayed `FetchOutcome.url` reports the URL the entry names. Cassettes are
   plain NDJSON files under `data/artifacts/`.
2. **Partial corruption loads clean and lies by omission.** Every unparseable line is
   `warn!`-skipped; the load errors only if **zero** entries survive. A crash mid-`write_all`
   leaves a truncated final line that is silently dropped — with no `recorded_truncated` marker,
   because that flag only covers the byte-cap path. The worker then logs `entries = N` at `info!`
   and proceeds. A cassette with 1 readable line out of 5,000 is a successful load followed by a
   storm of misses that are indistinguishable from "the job never fetched that".
3. **No format version.** `CassetteEntry` has no `v`/`schema` field and neither does the file.
   Adding a field degrades gracefully (`serde(default)`); *renaming or retyping* one turns every
   existing cassette into case 2 — silent per-line skips, then an all-miss replay. And cassettes
   are deliberately retention-exempt (`config.rs:1086-1090`, `artifact_retention_include_cassettes`
   defaults `false`), i.e. designed to outlive many releases.

With `replay-miss-terminal` landed, a miss becomes loud and permanent — which makes it *more*
important, not less, that the operator can tell "the cassette lost this" from "the run never
fetched this". Today those two produce byte-identical output.

## Evidence

- `crates/core/src/vcr.rs:510` — map keyed on `entry.req_hash` as deserialized;
  `crates/core/src/vcr.rs:541-555` — lookup by computed hash. No verification branch anywhere.
- `crates/core/src/vcr.rs:512` — `warn!`-skip per unparseable line;
  `crates/core/src/vcr.rs:515-520` — error only when the result is fully empty.
- `crates/core/src/vcr.rs:434-462` — `recorded_truncated` is set on the cap path only;
  `crates/core/src/vcr.rs:472` — the `write_all` whose interruption produces the torn line.
- `crates/core/src/vcr.rs:110-141` — `CassetteEntry`, no version field.
- `crates/core/src/config.rs:1086-1090` — cassettes are retention-exempt by default.
- Zero tests: `grep -rni "corrupt\|unreadable\|malformed"` over `crates/core/tests/vcr.rs`,
  `crates/server/src/e2e/vcr_attempts.rs`, `crates/core/src/vcr.rs` → one hit, the `warn!` itself.

## Acceptance criteria

1. A cassette entry whose stored `req_hash` disagrees with the hash of its own `method`+`url` is
   **refused**, not served. Decide and state whether that refusal is per-entry (skip + count) or
   whole-file (fail the load) — argue the choice; a silent skip that reproduces case 2 is not
   acceptable.
2. A partially-unreadable cassette is **distinguishable from a complete one at load time**. The
   number of dropped/torn lines reaches the operator through the same surface that already reports
   `entries = N` — a `warn!` nobody aggregates does not count. Say in the diff which surface you
   chose.
3. The file carries a format version, and a version the build does not understand is a typed,
   named refusal rather than a silent all-miss. Choose the cheapest shape that survives a rename
   (a header line, a per-entry field, or a sidecar) and justify it against the NDJSON append model
   — `Recorder::append` opens and appends per entry, so a header has an ordering cost; weigh it.
4. Tests for all three, each named after the anti-pattern it defends, each failing against
   today's loader. At minimum: a torn final line, a hash/url disagreement, an unknown version.
5. **Riders** (both are one-line-scale; you are already in these files):
   - `crates/core/src/retention.rs:39-42` re-declares `CASSETTE_FILE` with the comment "mirrors
     `vcr::CASSETTE_FILE`, which is private to that module" — it is `pub const` at `vcr.rs:70`,
     re-exported through `lib.rs:24`. Two constants kept in sync by a comment that is false.
   - `Cassette::from_entries` (`vcr.rs:558`) is doc'd "Test/tooling constructor" and has zero
     external callers, while `crates/core/tests/vcr.rs:277-283` and
     `crates/server/src/e2e/vcr_attempts.rs:60-69` are two hand-rolled copies of the same
     "resolve → unwrap body → index `["html"]` → contains" inspection. Either give it the caller
     that justifies it or say in its doc that it is test-local.

## Risks / non-goals

- **Non-goal**: redacting secrets from cassettes (separately assessed and rejected this round —
  the same bytes sit unredacted in the revisions store one directory over).
- **Non-goal**: changing first-wins duplicate semantics (`vcr.rs:479-481`, `:510`) — that is
  deliberate, documented and tested (`vcr.rs:690-708`).
- Hazard: any format-version scheme must keep **existing** cassettes readable or explicitly and
  loudly reject them. A change that silently invalidates every cassette on disk is the bug this
  direction exists to prevent, committed by the fix.

## Build record

(filled during build)
