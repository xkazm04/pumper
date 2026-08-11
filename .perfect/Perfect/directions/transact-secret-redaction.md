---
slug: transact-secret-redaction  type: perfect/direction
context: "[[browser-transact]]"  lens: robustness
status: shipped                  size: S
proposed: 2026-08-11  accepted: 2026-08-11  shipped: 2026-08-11  commit: f0ddec5
---
## What & why
A transact flow that types a password captures that password's live value into
`filled_fields[].value` — which lands in evidence.json on disk, the persisted job result, every
SSE subscriber, and every webhook/callback payload. The evidence summary must prove a field was
filled without republishing the secret. In-repo precedent: census-common's `redact_key` +
enforcement test was called out as a keeper pattern.

## Evidence
- core/src/engine.rs:378-388 — filled_fields_js reads `el.value` with no input-type check, no
  masking, no length cap.
- apps/transact/src/lib.rs:142,160 — values flow into evidence.json and jobs.result (thence SSE
  events, webhook payloads, HMAC callbacks).
- crates/apps/census-common/src/lib.rs:52 — redact_key precedent.

## Acceptance criteria
1. Sensitive inputs are masked at CAPTURE time in the browser-side JS (at minimum
   input[type=password]; the builder decides whether autocomplete hints like cc-* join v1) —
   the plaintext value never leaves the page: evidence shows found + redacted + a non-reversible
   hint (e.g. length or a fixed mask), never the value.
2. Non-sensitive captured values get a sane length cap so a textarea can't balloon the result
   payload; the cap is visible (truncated flag or documented constant).
3. Extracted, named, tested: the masking predicate/transform is a pure function with tests named
   after the anti-pattern (password_value_not_republished style), per repo law.
4. parse_filled_fields round-trips the new shape without breaking existing consumers' reads
   (FilledField stays serde-compatible or the change is deliberate + documented in the transact
   doc section).

## Risks / non-goals
- Job PARAMS still contain whatever the caller typed into the request — that is the job-model's
  storage posture, out of scope here (note it in the doc). dom.html redaction is out of scope
  (password inputs don't echo values into the DOM attribute; a test may assert the captured DOM
  isn't value-populated for password fields if cheap). Non-goal: encrypting artifacts at rest.

## Build record
- Shipped `f0ddec5` (Lot T, opus). All 4 criteria met. Masked at capture in-page:
  input[type=password] + credential/card autocomplete tokens (`SENSITIVE_AUTOCOMPLETE_TOKENS`
  compiled into the JS from the Rust const so predicates can't drift) → `{value:null, redacted:true,
  value_len}`. 512-char cap (`FILLED_VALUE_MAX_CHARS`) + truncated flag for non-secrets.
  `redact_field` re-enforces both on decode (defense in depth — a drifting page can't republish);
  `parse_filled_fields` routes every row through it. FilledField grew 3 serde(default) fields —
  old payloads decode unchanged (pinned). End-to-end sentinel test asserts neither jobs.result nor
  evidence.json serialization contains the secret. Review: keep.
- Documented caveat: a redacted field reads as value:null+found:true to old consumers; job PARAMS
  still hold the caller's typed text (job-model posture, noted in apps.md).
