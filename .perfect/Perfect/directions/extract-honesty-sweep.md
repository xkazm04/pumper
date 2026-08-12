---
slug: extract-honesty-sweep
type: perfect/direction
context: "[[extraction-core]]"
lens: robustness
status: shipped
size: S
proposed: 2026-08-12
accepted: 2026-08-12
shipped: 2026-08-12
commit: 26fb0cc
---

## What & why
Four small ways an extraction result lies, all in extract.rs, one sweep: (1) XPath non-node
results (count(), string(), number()) render as Rust Debug strings — garbage data stored as
if extracted; (2) an XPath that fails at runtime returns Null and classifies Empty, hiding a
broken rule as "site had nothing"; (3) the `default` transform only fires on Null while the
status system defines blank as null/""/[] — a matched-but-empty string keeps "" and the
declared default never applies; (4) to_number/to_int disagree on overflow/NaN, and
uppercase + regex_replace are the two transforms with zero test coverage.

## Evidence
- crates/core/src/extract.rs:772-773 — `other => Value::String(format!("{other:?}"))`.
- :747-749 — `let Ok(items) = xpath.apply(tree) else { return Value::Null; }` → Empty, not Error.
- :310-312 — `(Self::Default { value: d }, Value::Null) => d.clone()` vs is_blank :524-531.
- Verified live 2026-08-12.

## Acceptance criteria
- XPath atomics map to their JSON types (number→number, string→string, boolean→bool); Debug
  formatting is unreachable for atomics; test pins count()/string() results.
- XPath runtime failure produces FieldStatus::Error with a detail string, not Empty; test
  named `xpath_error_not_empty`.
- `default` fires exactly on is_blank (shared predicate — the two definitions cannot
  disagree), with a test `default_fires_on_blank_not_just_null`; existing default-on-null
  behavior preserved as a subset.
- to_number/to_int agree on non-finite input (documented choice, tested); uppercase and
  regex_replace get coverage.

## Risks / non-goals
- `default`-on-blank is a behavior change for rules relying on "" passthrough — the builder
  should check in-repo RuleSet usages (apps, catalog) and report any that change meaning.
- Non-goal: new transforms (url_absolute is its own direction).

## Build record
Continuation builder (E2), commit `26fb0cc`. All four lies closed as extracted named
functions: `xpath_atomic_value` (atomics typed, Debug unreachable), `xpath_extract` →
Result with Cow detail (runtime failure = Error not Empty; healthy fields allocate
nothing), `default` guarded by the SHARED `is_blank` (drift-proof test walks every value
shape; falsey 0/false stay data), `number_value` (to_int saturation → null; non-finite
null at both precisions; the FINITE-but-out-of-i64 divergence kept deliberately — an f64
holds 1e20, an i64 cannot, forcing agreement would be its own lie). uppercase +
regex_replace covered. Blast radius verified: NO in-repo rule set uses `default`, so the
blank widening changes no shipped pipeline. Builder refutations (load-bearing): xpath
predicate errors only on non-empty node sets (first test passed for the wrong reason,
caught and fixed); to_number/to_int "disagreement" was two distinct cases, only one a bug.
Gates: check + workspace lib + 30 extract tests green; fmt clean; no new clippy.
