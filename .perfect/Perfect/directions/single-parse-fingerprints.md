---
slug: single-parse-fingerprints
type: perfect/direction
context: "[[source-resilience]]"
lens: optimization
status: shipped
size: M
proposed: 2026-08-04
accepted: 2026-08-04
shipped: 2026-08-04
commit: 42e8b37
---
## What & why
Every document is HTML-parsed twice per run. Extraction parses it, then `observe` hands the raw
bodies to `signals_batch`, which calls `scraper::Html::parse_document` again per document purely to
fingerprint it. The code says so itself: *"Fingerprinting parses each body once more."* On the
extractor — the platform's highest-volume path — that is a full second DOM build per document, and
the parse is the expensive part of the pass.

## Evidence
- The admission: `crates/apps/extractor/src/lib.rs:387` (comment) and the `spawn_blocking` at `:388`.
- Second parse: `crates/core/src/resilience/mod.rs:273` inside `doc_signals`.
- Batch path: `mod.rs:287-293` (`signals_batch`, rayon).
- Extraction's own parse: the compiled-RuleSet execution path in `crates/core/src/extract.rs`.

## Acceptance criteria
- Each document is parsed once per run and the parsed DOM is shared with fingerprinting.
- **Fingerprints byte-identical before and after** — equivalence test over a fixture corpus. These
  values are persisted in `doc_fingerprints` and compared across runs, so any drift silently
  corrupts every future divergence verdict.
- Measured wall-clock AND peak memory on a realistic batch, reported (sharing DOMs may trade memory
  for time — if it does, say so with numbers).
- The 200k visible-text cap and the rayon parallelism preserved.
- The async runtime is still never blocked (the `spawn_blocking` boundary stays).

## Risks / non-goals
Correctness over speed: a changed fingerprint is worse than a slow one. Do not change the SimHash
functions or the text/DOM/value asymmetry.

## Build record
(pending)
