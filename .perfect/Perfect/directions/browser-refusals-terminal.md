---
slug: browser-refusals-terminal
type: perfect/direction
context: "[[browser-engine]]"
lens: robustness
status: shipped
size: S
proposed: 2026-08-13
accepted: 2026-08-13
shipped: 2026-08-13
commit: f145ad2
---

## What & why

`docs/features/apps.md:59` states that the transact pre-flight refusals are "all typed
`Error::Transact`, which is now terminal for the job". **One of them is not** — an unsafe
profile name returns `Error::Profile`, which `is_terminal_for_job` does not match, so a
typo'd profile in a transact job burns the whole retry ladder: four Chrome launches, four
backoff sleeps, the same sentence four times. The transact params schema has no `pattern`
on `profile` either, so it is not a 422 at the door — it becomes a retried job on the most
expensive tier.

This is exactly the failure r17's `engine-conformance-suite` killed for `Browser::transact`'s
default refusal, one contract line away and still live. The over-cap-HTML error is the same
class: a pure function of (page bytes, cap) that cannot change across attempts, classified
retryable.

## Evidence

- `crates/core/src/error.rs:314-316` — `is_terminal_for_job` matches only
  `BudgetExhausted | Transact`. **Director-verified.**
- `crates/core/src/engine.rs:483-485` — `TransactRequest::validate` returns
  `validate_profile_name(profile)?`, i.e. `Error::Profile` (`engine.rs:41,44,52`), while its
  siblings at `:467` (`submit: true`) and `:477` (blank idempotency key) return `Error::Transact`.
- `crates/engine-browser/src/lib.rs:212` — the same `Error::Profile` on the `render` path.
- `crates/engine-browser/src/lib.rs:650-656` — over-cap HTML returns `Error::Browser` (retryable).
- `crates/apps/transact/src/lib.rs:82-85` — `profile` has no `pattern`, so the door admits it.
- `crates/server/src/worker.rs:818` (terminal branch) vs `:844` ("Not terminal — retry pending").
- The false doc line: `docs/features/apps.md:59`.

## Acceptance criteria

1. A deterministic pre-flight refusal fails the job **once**. Decide and justify which lever
   you use — widen `is_terminal_for_job`, or make the validation return the already-terminal
   variant — and apply it consistently to both the `render` and `transact` profile-name paths.
   Do not fix one and leave the other.
2. The over-cap-HTML failure is classified honestly: either terminal (it is deterministic) or
   explicitly argued as retryable in a comment. State which and why.
3. The transact params schema rejects an unsafe profile name at the **door** (422), so the job
   is never created. Reuse the existing validation rather than duplicating a regex — grep first.
4. Extend r17's cross-engine conformance battery (`crates/server/src/e2e/engine_conformance.rs`)
   so the "a deterministic refusal fails once" property is policed for this class too, not just
   for `transact`'s unsupported-engine default. A convention is enforced by a test, not a sentence.
5. `docs/features/apps.md:59` becomes true — either by the code matching it or by the sentence
   naming the exception. No third option.
6. A test named after the anti-pattern proves a typo'd profile does not burn the ladder.

## Risks / non-goals

- **Non-goal:** a general error-taxonomy rework. Touch only the deterministic-refusal class.
- Risk: widening `is_terminal_for_job` to all of `Error::Profile` may capture a *transient*
  profile error (e.g. a jar unreadable because of a concurrent write). Check every construction
  site of `Error::Profile` before widening; if any is transient, take the other lever.

## Build record

**Shipped `f145ad2` · verdict KEEP — and the best refutation of the round.**

The brief flagged a risk to *check before acting*: widening `is_terminal_for_job` to all of
`Error::Profile` might capture a transient error. **The builder checked and it does** —
`engine-http`'s `ProfileJar::load` types an unreadable cookie jar as `Error::Profile`, covering a
sharing violation against its own flusher's `fs::rename`. Reclassifying the variant would silently
have taken the retries away from that IO. So the refusal is retyped at its seams instead:
`engine::require_safe_profile_name` → `Error::BadRequest`, applied to **both** `render` and
`transact`, never one. The *why* now lives in the `is_terminal_for_job` doc comment, so a future
round cannot undo it by accident.

`Error::BadRequest` joins the terminal set on its own merits (every producer is a pure function of
input frozen at enqueue) — and it had been sitting in
`transient_failures_stay_retryable_not_terminal` as if transient, which its own doc never claimed.

**Over-cap HTML: argued retryable**, refuting my framing that it is a pure function of (bytes, cap) —
the cap is fixed but the size is a live page re-rendered next attempt, and a JS-built DOM is not
deterministic between two renders of the same URL.

**Door:** `"pattern": profile_name_pattern()` generated from `PROFILE_NAME_MAX_LEN` + the validator's
own alphabet, pinned by `the_schema_pattern_and_the_validator_agree` over a corpus. No second regex.
Live-verified end to end by `just smoke` (36/36): an unsafe name is a 422 at the door.

**Conformance:** `a_deterministic_profile_refusal_fails_once_on_every_seam`, an EXPECTED map over
every seam that checks a name — and it recorded `engine-http::fetch` as **false**, a known gap kept
*visible* rather than untested. **The Director closed that gap in `eefdd3b` the same round**, flipping
the row to `true`; all three seams now agree.

**Refuted:** `docs/features/apps.md:59` was **not** the false sentence — "all of these" scopes to its
four bullets and all four *are* `Error::Transact`. The genuinely false claims were code comments at
`crates/apps/transact/src/lib.rs:157-161` and `crates/core/src/error.rs:294-300`; both corrected.
Also: the cost was four job attempts + backoff with **zero** Chrome launches — validation runs before
`acquire` — so my "four Chrome launches" was wrong. The ladder waste is real; the launch cost was not.
