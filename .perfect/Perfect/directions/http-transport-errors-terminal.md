---
slug: http-transport-errors-terminal
type: perfect/direction
context: "[[http-engine]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-13
accepted: 2026-08-13
shipped: 2026-08-13
commit: 2e46fd0
---

## What & why

Statuses are classified — `retryable_statuses` (`lib.rs:430`, default `[429,502,503,504]`)
correctly returns a 404 on attempt 1. The **transport** arm is not classified at all:

```rust
Err(e) => { last_error = e.to_string(); warn!(...); }   // lib.rs:469-472
```

So an invalid URL, an unsupported scheme (`ftp://`), DNS NXDOMAIN, a TLS certificate
mismatch and a redirect-limit overflow are each retried four times with full backoff and
three governor slots. Then the terminal `Error::Http` (`:475`) is **retryable at the job
level** — `is_terminal_for_job` admits only `BudgetExhausted | Transact | BadRequest`
(`error.rs:337-342`) — so the worker re-queues and the whole ladder runs again per job
attempt. A crawl frontier that yields one dead hostname pays `4 × N` attempts to learn
what reqwest knew deterministically the first time.

This is the **third instance of a class this loop has already killed twice**: r17 caught
`Browser::transact`'s default returning retryable `Error::Browser` for an unsupported
flow, and r18 caught the profile-name refusal doing the same on three seams. Both were
closed by classifying at the source and pinning the seam in the engine conformance
battery. The transport arm is the seam that battery never reached — and the battery
currently sets `retries: 0` (`engine_conformance.rs:134-138`), so the retry ladder is
switched off in the only cross-engine test that could have seen this.

## Evidence

- `crates/engine-http/src/lib.rs:469-472` — the unclassified transport arm
- `crates/engine-http/src/lib.rs:430` — the status arm, which *is* classified (the contrast)
- `crates/engine-http/src/lib.rs:475` — terminal `Error::Http`
- `crates/core/src/error.rs:337-342` — `is_terminal_for_job` does not admit `Error::Http`
- `crates/server/src/e2e/engine_conformance.rs:134-138` — the battery runs with `retries: 0`
- `crates/server/src/e2e/engine_conformance.rs:301-334, 354-417` — the two prior kills of this
  class and the EXPECTED-map idiom that pinned them

## Acceptance criteria

1. Deterministic transport failures fail **once**, not `retries + 1` times. At minimum:
   builder/URL-construction errors and unsupported schemes. Connect failures, timeouts and
   read errors stay retryable — those are genuinely transient.
2. **TLS and DNS are the judgment calls — check before you classify.** A cert mismatch is
   usually permanent but can be a captive portal; NXDOMAIN can be a transient resolver
   failure. Decide each with the reasoning in a doc comment; "left retryable, here is why"
   is an acceptable answer for either. Do not classify on error *message* substrings if
   reqwest exposes a typed predicate — and say which predicates you found.
3. The terminal set maps to an error variant `is_terminal_for_job` already admits, so the
   job stops after one attempt instead of burning its ladder. If you widen
   `is_terminal_for_job` instead, **audit every construction site of the variant you widen
   first** and say what you found (this is exactly how r18's profile fix chose its lever).
4. The engine conformance battery gains a row that pins the seam — and the battery's
   `retries: 0` fixture no longer hides the retry ladder from every engine it tests
   (either a second fixture with retries on, or a targeted test; your call).
5. A test asserts the attempt **count** for one deterministic failure and one transient
   failure — the numbers, not just the outcome.

## Risks / non-goals

- Over-classifying is the failure mode: making a transient class terminal turns a
  recoverable blip into a failed job. When unsure, leave it retryable and say so.
- Non-goal: changing `retryable_statuses` or its defaults.
- Non-goal: the body-cap error, which already `?`-propagates out of the loop and fails once
  (`:459`) — verify that before assuming otherwise.

## Build record

**Shipped `2e46fd0`. Director verdict: KEEP.** All five criteria met.

*Process note*: this commit carries D2 **and** the engine half of
[[profiled-fetch-is-honest]] — both live in `engine-http/src/lib.rs` and the builder had started
the jar work before this direction's conformance run finished. Flagged in the report rather than
papered over; the commit message names both directions and states what it carries, and both
commits build and pass standalone. Accepted: the skill's own guidance is to commit by file
boundaries and document the shared commit when directions interleave in one file.

**The best engineering in the wave.** `TransportPredicates` — a struct-of-bools lifted off
`reqwest::Error` — exists for a reason the direction never anticipated: **`reqwest::Error` has no
public constructor**, so a classifier taking one could only ever be exercised through a live
socket, and the cases that matter most here (TLS mismatch, NXDOMAIN) are precisely the ones a
loopback test cannot produce. Splitting the rule out makes every combination testable and puts
the decision in one reviewable place.

Criterion 1–3: exactly one class is deterministic — `is_builder` → `Error::BadRequest`, a variant
`is_terminal_for_job` **already admits, so nothing was widened and no construction-site audit was
needed**. The exclusions carry more reasoning than the inclusion: `is_connect` stays retryable
because it bundles NXDOMAIN-from-a-down-resolver, a captive portal failing the TLS handshake, and
a restarting service, with no reqwest predicate separating them; a redirect-limit overflow stays
retryable because it is usually a *session* fact an expired cookie causes. Predicates verified
against reqwest 0.12.28 **source** (`url_bad_scheme` and a URL parse error are both
`Kind::Builder`) *and* by a live test asserting `is_builder()` for `ftp://` and `::not a url::`.

Criterion 4–5: the conformance battery gained `http_engine_with_retries` (`retries: 2`), so the
ladder is no longer switched off, plus an EXPECTED map over three seams —
`fetch (unsupported scheme)=true`, `fetch_bytes (unsupported scheme)=true`,
**`fetch (connection refused)=false`**, the transient row pinned false on purpose because
over-classification is this fix's failure mode. Attempt counts asserted as numbers: deterministic
→ 0 origin hits and < 500 ms (vs 3.5 s of backoff if the ladder ran); transient → the engine's own
`"failed after 3 attempts"` string plus elapsed ≥ 1.4 s. Battery 5 → 7 tests.

**Builder refutation worth keeping**: `is_connect`/`is_timeout` walk the error's `source()` chain
rather than matching a `Kind`, so they are **not** mutually exclusive with `is_builder`. A
classifier written as a `match` on "which predicate is true" would be order-dependent and fragile;
this one checks `builder` positively and has a test for the `builder + connect` combination.

**Not verified (builder-disclosed)**: no live-network TLS mismatch or real NXDOMAIN was exercised.
Both judgment calls rest on reading reqwest's source. Both leave the class *retryable*, which is
the safe direction — a wrong reading costs wasted attempts, not failed jobs.
