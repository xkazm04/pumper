---
name: vcr-testing
type: perfect/context
group: Core Platform
category: lib
opportunity: 4
last_proposed: 2026-08-13
cooldown_until: round 22
directions: ["[[replay-miss-terminal]]", "[[cassette-verified-or-refused]]", "[[replay-means-replay]]"]
---

## Current state
Scouted end to end, round 20 (`crates/core/src/vcr.rs` 750 lines, `crates/core/src/testing.rs`
366 lines, both read completely; every consumer traced with per-symbol call-site counts).

**The model.** Three modes on one enum — `Vcr::{Off, Record(Arc<Recorder>), Replay(Arc<Cassette>)}`
(`vcr.rs:87-95`), carried as a public `AppContext` field (`app.rs:106`). Key = `sha256(method \0
key)` (`vcr.rs:101-107`), no URL normalization. Entries are NDJSON at
`<artifacts>/<app>/<job_id>/cassette.ndjson`. **The record seam is exactly two call sites**
(`app.rs:411-413` fetch, `app.rs:483-488` research) and replay mirrors them (`app.rs:288-294`,
`:430-437`), both returning before any engine. Attempt policy is `CassetteStart::{Fresh, Resume}`
(r9's work), chosen at `worker.rs:593-609` off `restored.is_some()`. Caps 4 MiB/entry,
128 MiB/cassette. **There is no `[vcr]` config section and no env var**; the only adjacent key is
`storage.artifact_retention_include_cassettes` (default `false`, i.e. cassettes are retention-exempt
and outlive releases).

**Health of what exists is good.** Confirmed non-findings: replay never falls through to a live
fetch (proven against panicking engines); `record` + `replay_of` together is rejected and tested;
a malformed `replay_of` is rejected; a missing cassette is loud and permanent; an unknown engine
string is a typed error; the total cap really binds across attempts; the empty-attempt hole r9
closed is still closed; `cargo test -p pumper-core --test vcr` → 7 passed. Cassettes are **not**
committed (`git ls-files | grep cassette` → empty; `/data` gitignored).

**Where it is weak** (the three accepted directions): a resolve-time `ReplayMiss` is not terminal
while the load-time one is; the loader trusts `req_hash` from the file and warn-skips corrupt lines,
erroring only when *zero* entries survive, with no format version; and the module doc's "Replay runs
touch no engine" is false — `transact` drives a live Chrome session under a `vcr_replay_of` stamp.

**Banked, with the sharpest slice named** — `TestContext` has **8 setters against `AppContext`'s 18
fields**. Six sites reach around it by mutating `ctx.plugins` directly, with a code comment
admitting the missing seam (`crates/apps/plugin/tests/common/mod.rs:126-128`); 14 tests hand-roll a
full `AppContext { … }` literal, which already falsifies `testing.rs:14-15`'s claim that a new field
is "a one-site edit in test code"; and **50 hand-rolled `impl HttpClient for`** exist because `Dead`
(panics) is the only double shipped. The single sharpest gap: `NoCheckpoints::save` returns `true`
unconditionally and `grep -rn "impl CheckpointSink for" crates/` returns exactly one hit — the no-op
itself — so **"the app handled a lost checkpoint" is unassertable anywhere in the workspace**, even
though `app.rs:42-44` explicitly tells apps to handle it.

Five public seams have zero external consumers (`req_hash`, `TOTAL_CAP_BYTES`,
`Cassette::is_empty`, `Recorder::cassette_path`, `CassetteEntry`'s re-export); two of them become
live as riders inside [[cassette-verified-or-refused]], which is the right home for them.

## Direction history

**Round 20 — 3 accepted, 3 rejected** (director-self-gated, autonomous):

- ACCEPTED [[replay-miss-terminal]] — `is_terminal_for_job` (`error.rs:337-342`) omits `ReplayMiss`
  while the load-time miss calls `fail_permanently`. Fourth instance of the class r17/r18/r19 each
  killed once, and `BadRequest`'s own doc states the rationale verbatim.
- ACCEPTED [[cassette-verified-or-refused]] — identity trusted not recomputed, partial corruption
  loading clean, no format version on a deliberately long-lived file.
- ACCEPTED [[replay-means-replay]] — a replay of `transact` drives a live browser and is still
  stamped `vcr_replay_of`. **Director-verified and partially refuted before gating**: the scout
  framed it as "meters real work"; both raw meters pass `0.0`, so the "spend $0" clause holds. The
  defect is engine contact plus a false provenance stamp, not cost. Slated against the corrected
  claim.
- REJECTED-DEFERRED **harness-expresses-the-run** (the `TestContext` seam work, and this context's
  r19-handed anchor) — real, and banked as the context's anchor. Deferred for three reasons, in
  order: (1) each accepted direction is a live correctness/honesty bug with a user-visible failure
  mode, while this is developer ergonomics whose payoff is indirect; (2) its write set is
  `testing.rs`, which ripples into 33 files across **both** lots' crates — the one edit in this
  slate that could straddle an otherwise zero-Class-B partition; (3) it needs a scoped design
  (which seams; whether a canonical `CannedHttp` earns its keep against 50 bespoke doubles) rather
  than a line. **Next round's brief must require it to ship a test that is structurally impossible
  today** — otherwise it is exactly the consistency polish the taste filter rejects.
  **Cross-brief note: this direction is the blocker for [[us-federal-grants]]'s deferred
  `grants-detail-delta-survives-restart`** — that fix's regression test needs a checkpoint sink
  that can fail, which does not exist. Two independent scouts converged on the same missing seam;
  build them as a pair.
- REJECTED **cassette-redaction** — record writes URLs verbatim with zero redaction in `vcr.rs`
  (20 redact hits in core, all in `engine.rs`), and `FetchRequest.profile` threads a logged-in
  cookie jar so a cassette can hold an authenticated page body. Rejected because the exposure is
  one step removed: cassettes are gitignored and never committed, so the leak is to local disk —
  which already holds the same bytes in the revisions store and the artifacts dir. Redacting the
  cassette alone is theatre while the same secrets sit unredacted one directory over, and r10's
  `transact-secret-redaction` already covers everything that leaves the box. Revisit only if
  cassettes become shareable artifacts.
- REJECTED **dead-public-seams** (`is_empty`, `TOTAL_CAP_BYTES`, `cassette_path`, the
  `CassetteEntry` re-export) — cosmetic churn with no user moment; precisely what the taste filter
  exists to reject. The two that have a real use (`req_hash`, `from_entries`) ride as named riders
  in [[cassette-verified-or-refused]] instead of being banked.
- REJECTED-DEFERRED **concurrent-attempt cassette race** — two attempts of one job can race on one
  cassette path and `Fresh` will `remove_file` under a live writer, because `reap_stale` cannot
  stop the running task and `heartbeat_secs = 0` + `stale_after_secs = 120` is a config the
  validator permits. Real, but it requires a configuration nobody runs, and the Windows failure
  mode (unlink fails → fail-open append) is bounded by first-wins load semantics. Banked; becomes
  live the day anyone ships `heartbeat_secs = 0`.

## Shipped
- (via app-runtime r9): vcr-attempt-integrity `4e3647a`.
- Round 20: pending — see the three accepted directions above.
