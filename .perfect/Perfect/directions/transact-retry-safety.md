---
slug: transact-retry-safety      type: perfect/direction
context: "[[browser-transact]]"  lens: robustness
status: shipped                  size: M
proposed: 2026-08-11  accepted: 2026-08-11  shipped: 2026-08-11  commit: 8e17ca7
---
## What & why
For an app that ACTS on live pages, the retry ladder is a hazard, and the front door lets garbage
through. A `submit: true` or empty-key refusal is maximally deterministic yet rides the whole
backoff ladder (the exact anti-pattern core/error.rs:72-76 documents); a typo'd steps key
("step") passes the schema and runs a zero-step flow that produces a plausible landing-page
bundle; `submit: true` is only rejected at run time (failed job) instead of enqueue time (422);
a typo'd profile silently creates an empty logged-OUT Chrome profile and runs the flow against a
login wall. Make refusals terminal and the door honest.

## Evidence
- core/src/error.rs:77-79 — is_terminal_for_job: only BudgetExhausted; Error::Transact (produced
  solely by deterministic validate() refusals, engine.rs:286-308) is retried.
- crates/server/src/worker.rs:697 — the worker branch that consumes the classification.
- apps/transact/src/lib.rs:73-77 — schema: submit is {"type":"boolean","default":false} (true
  passes); idempotency_key minLength 1 accepts "   ".
- core/src/engine.rs:244 — TransactRequest lacks unknown-field rejection; typo'd fields silently
  dropped.
- engine-browser/src/lib.rs:207-212,226 — Some(profile) → create_dir_all: nonexistent profile
  silently born empty (logged out), and nothing records which profile ran (evidence-honesty
  records it; this direction makes it an error up front).

## Acceptance criteria
1. Deterministic transact refusals no longer burn attempts: Error::Transact joins the terminal
   classification (extending the is_terminal_for_job doc + its anti-pattern test in the same
   style), OR the builder finds and documents a sharper boundary if some Transact variant is
   genuinely transient — intent is "a refusal fails ONCE".
2. `submit: true`, blank idempotency_key, and unknown top-level fields are rejected at the DOOR
   (422 at enqueue) — schema tightening (const/enum false, pattern, additionalProperties) and/or
   deny_unknown_fields, **but FIRST verify how trigger-injected params (`_trigger`) and any other
   injected keys interact with enqueue-time schema validation and app-side deserialization — do
   not break triggered jobs.** If injection conflicts, reject unknown fields app-side with an
   allowlist for underscore-prefixed keys, and say so.
3. A flow under a nonexistent profile FAILS with an actionable error naming the profile and the
   /profiles surface, instead of silently running logged out (existence check where the profile
   dir mapping happens; "no profile" stays valid).
4. Tests named after each anti-pattern: refusal_not_retried, unknown_field_not_silently_dropped,
   missing_profile_not_silently_created (or equivalent), all green alongside the existing
   manifest_tests worked-example validation.

## Risks / non-goals
- Verify the manifest worked-example still validates after schema tightening. The comment at
  apps/transact/src/lib.rs:243-245 about Dead engines is inaccurate (Dead doesn't override
  transact — trait default fires) — fix in passing if touched. Non-goals: idempotency-key dedup
  table (live-submit slice), URL/SSRF policy (banked), max_attempts caps in the job model.

## Build record
- Shipped `8e17ca7` (Lot T, opus). All 4 criteria met. `Error::Transact` terminal
  (boundary tested both ways: pre-flight refusal = Transact/terminal, mid-flow failure =
  Browser/retryable). Schema: submit `const:false`, idempotency_key `pattern:"\S"`,
  `additionalProperties:false` + `patternProperties {"^_":{}}` — builder verified against the
  real jsonschema 0.30 in a throwaway crate (7 cases). KEY REFUTATION: trigger-fired jobs
  enqueue via `enqueue_dedup` directly (triggers.rs:938,1027) and NEVER hit the schema
  validator — so the app-side `unknown_transact_fields` twin (with `_`-prefix allowlist) is the
  ONLY guard for triggered transacts, not defense in depth. `require_existing_profile` refuses
  before any Chrome launch/mkdir. `Dead::transact` now panics (comment's premise was hollow).
  Review: keep. Live-verified by smoke: three 422s at the door + declared schema (21/21).
- Note: error.rs doc attributes all three refusals to validate() — the profile check actually
  lives at the engine seam and unknown-fields app-side; the CLASS (deterministic pre-flight)
  is what matters and holds. Minor imprecision, not worth a follow-up alone.
