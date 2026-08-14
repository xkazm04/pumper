---
name: wasm-plugin-examples
type: perfect/context
group: Event Pipeline
category: lib
opportunity: 6
last_proposed: 2026-08-14
cooldown_until: —
directions: ["[[shipped-plugins-are-verified]]", "[[shipped-plugins-are-verified]]", "[[trigger-gate-honest-across-source-kinds]]"]
---

## Current state
**Scouted THOROUGHLY 2026-08-14 (round 22) — COVERED.** Opportunity revised **3 → 6**: the
"mostly verdict-shaped" read below was wrong. Two of these four crates are *production*
artifacts, not examples, and nothing verifies them.

Files: plugins-src/{title-extractor,delta-slim,trigger-gate,busyloop}/src/lib.rs.

- **ABI conformance: all four match the host today.** Host contract is `memory` + `alloc(u32)->u32`
  + (`extract_v2` preferred | `extract` fallback) `(u32,u32)->u64`, output packed `(ptr<<32)|len`,
  bounds-checked (`crates/engine-wasm/src/lib.rs:150-155, 799-846, 607-635`). No executable
  divergence. `busyloop` conforms to the *legacy* path only (`extract`, no `describe`).
- **Nothing compiles them.** The four crates are workspace-detached; CI has no wasm32 target; all
  four artifact tests are `#[ignore]`d — and `just test-ignored` cannot pass on a clean machine
  because `plugins-install` covers 2 of 4 and the `title.wasm` rename lives only in the README.
  → [[shipped-plugins-are-verified]]
- **`trigger-gate` is fail-CLOSED on job/external triggers** and the ledger calls it a healthy
  veto. → [[trigger-gate-honest-across-source-kinds]]
- **`delta-slim`'s shaping silently rescopes the target's WORK**, not just its payload:
  `_trigger.keys` is the authoritative key set the target reads
  (`crates/apps/plugin/src/lib.rs:1063`, `crates/apps/extractor/src/lib.rs:1522-1532` → falls back
  to *all live records*, capped 10,000). The docs' own copy-paste example
  `{"keep":["dataset","count"]}` (`trigger-plugins.md:63`) drops `keys` → a scoped N-key hop
  becomes a whole-dataset sweep; `max_keys` truncates work while `count` still reports the full
  number, **and `trigger_plugins.rs:702-709` asserts that state as correct**. Banked.
- **The fail-open path is SOUND and well covered** — error log per evaluation + one ledger row per
  (trigger, plugin, outcome), re-armed by `POST /plugins/reload`; tested at
  `trigger_plugins.rs:288-352, 544-628`. Not a defect. But note: `data/plugins/` on this machine
  holds only `busyloop.wasm` and `title.wasm`, so **this checkout is currently on that path**.
- **`busyloop` still exhausts fuel** (budget 200M vs a `u64::MAX` loop — ~11 orders of magnitude of
  headroom), so the hostile premise is intact; it is simply never run, and `BURN_WAT`
  (`trigger_plugins.rs:55-58, 211-240`) already proves fuel exhaustion in CI **without** a wasm32
  toolchain. Verdict: retire it or give it a recipe + an `#[ignore]`d trap assertion; as it stands
  it is a loaded, executable, invocable artifact no test consumes.
- Riders: `title-extractor`'s `describe()` omits `kind`, so `GET /plugins?kind=extractor` returns
  `[]` on a host that has the reference extractor loaded (`routes/runtime.rs:326-328`) — and
  `docs/features/extraction.md:190` lists the manifest without `kind` while
  `trigger-plugins.md:43` includes it, so the two docs disagree with each other. `README.md:247-249`
  teaches only the dead legacy half of the ABI.

## Direction history
- **2026-08-14 (round 22): PROPOSED — 2 directions drafted, both REJECTED on the 6-direction cap.**
  Not a nothing-clears verdict: both are real and evidenced. [[shipped-plugins-are-verified]] and
  [[trigger-gate-honest-across-source-kinds]] are a **pair** (the second is unassertable without the
  first's harness — the same enabler-blocks-fix relationship r22 finally paid off for the checkpoint
  seam). **Top r23 candidate for this context.** The r11 anchors below are superseded by them.
- (round 6, via trigger-pipeline): activation covered trigger-gate/delta-slim.
- 2026-08-12 (round 11): scouted (medium); candidates exist — banked, not slated (cap). NOT
  covered yet. Anchors:
  1. **shipped-plugins-verified** (S/M): the two PRODUCTION plugins (trigger-gate, delta-slim)
     are never compiled or exercised by CI — plugins-src crates are workspace-detached, CI has
     no wasm32 target, and all 4 tests that touch a real artifact are #[ignore]d. A build break
     surfaces as a silent production behavior change (fail-open unknown-plugin path). Fix: CI
     wasm32 + plugins build + un-ignore against built artifacts.
  2. **trigger-gate-honest-across-source-kinds** (S): reads delta.count unwrap_or(0) vs
     min_count default 1 — on job/external triggers (no count field) it silently vetoes EVERY
     hop with a well-formed pass:false, worse than a crash (fail-open never engages); docs say
     "attachable to any source_kind". Needs (1)'s harness to be provable.
  3. (riders) title-extractor's describe() omits kind → invisible to GET /plugins?kind=
     filters and teaches the omission; README documents only the legacy ABI (no extract_v2/
     describe); busyloop is a doc prop no test consumes.
  ABI itself verified compatible (extract_v2 preferred, fallback works).

## Shipped
- (via trigger-pipeline r6)
- 2026-08-14 (r23): [[shipped-plugins-are-verified]] -> `9256ddd` — the twice-banked
  enabler. CI now builds all four detached `plugins-src` crates for wasm32 and runs the
  artifact tests; `just plugins-install` globs all four and owns the `title_extractor.wasm`
  -> `title.wasm` rename that lived only as README prose (the reason `just test-ignored`
  could not pass on a clean machine). **The builder refuted the naive design**: deleting
  `#[no_mangle]` from an export still compiles clean for wasm32, so a build-only CI step
  would NOT have caught an ABI break. Hence two guards — a source-level EXPECTED-diff ABI
  test that runs in the ordinary suite, plus the artifact tests asking the host whether each
  installed module is executable.
- 2026-08-14 (r23): [[trigger-gate-honest-across-source-kinds]] -> `ce9652c` — the shipped
  example predicate returned `{"pass": false}` forever on job- and external-kind hops
  (neither envelope carries `count`), and the ledger recorded it as `predicate_veto` — "a
  predicate that ran and answered". A rule whose field is **absent** is now *inapplicable*
  rather than failed. Pair shipped in the order the bank specified: enabler first, and the
  artifact tests it unlocked FAILED against the previously-installed `.wasm`.
