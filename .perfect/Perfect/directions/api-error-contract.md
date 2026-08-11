---
slug: api-error-contract         type: perfect/direction
context: "[[api-surface]]"       lens: robustness
status: shipped                  size: M
proposed: 2026-08-11  accepted: 2026-08-11  shipped: 2026-08-11  commit: 0cfc366
---
## What & why
The error envelope's `code` field exists so clients can branch without string-matching — and today
it lies for exactly the cases that matter: a rate-limited ingress push, a disabled ingress source,
and the five resilience routes' documented detection-off 503 all report `"internal"`. Meanwhile
every core error except BadRequest collapses to a 500 whose body echoes raw SQLite/sqlx text,
filesystem paths, and upstream URLs to the client. Complete the code map, widen the core-error
mapping where the variant is definitionally client-fault or upstream-fault, and stop leaking
internals in bodies.

## Evidence
- crates/server/src/routes/error.rs:25-36 — error_code: no 403/429/503 arms ("kept in lockstep"
  doc claim is false).
- ingress.rs:274 (403), ingress.rs:282 (429), health.rs:337-344 (503 ×5 routes) — all → "internal".
- error.rs:45-56 — From<core::Error>: only BadRequest→400; Storage (incl. RowNotFound + raw
  Database text), Profile, BudgetExhausted, ReplayMiss, Transact, Http/Browser/Claude, Config/Io/
  Json/Other → 500 with other.to_string() verbatim in the body.
- e2e/ingress_gates.rs:115,144 — asserts status only, so nothing pins codes today.

## Acceptance criteria
1. error_code covers every status any handler emits (add forbidden / rate_limited / unavailable;
   sweep handlers for others) with a test that pins the map against the statuses in use — the
   inventory-test idiom, not a comment.
2. From<core::Error> maps the client-fault and upstream-fault variants honestly. Builder decides
   the exact table with rationale recorded; intent: Profile→4xx naming the bad input;
   BudgetExhausted→a distinct, documented client-visible refusal (not 500); Http/Browser/Claude→
   502 (code bad_gateway exists); ReplayMiss/Transact→the 4xx that fits their semantics. Storage
   stays 500 EXCEPT the builder evaluates whether RowNotFound at this boundary is safe to map
   (handlers already raise explicit 404s — do not double-map if it would mask bugs; a reasoned
   "leave it" is acceptable).
3. 500-class bodies stop leaking: raw sqlx/SQLite/anyhow/path/upstream-URL text is logged
   server-side (with enough context to debug) and replaced client-side by a generic message +
   code. Redaction is an extracted, tested function per repo law.
4. e2e assertions upgraded: ingress 403/429 and resilience 503 pin their new codes; http-api.md's
   error-contract section updated in the same commits (doc-sync).

## Risks / non-goals
- @pumper/sync and other consumers read these envelopes — changing STATUSES for existing flows
  needs a compatibility check (grep the TS client + conformance tests); adding/complet­ing codes
  is safe. BudgetExhausted already has terminal-classification semantics in the worker — this
  direction touches only the HTTP boundary, not job retry logic (round-9 work; don't disturb).
  Coordinate note: a sibling lot makes Error::Transact terminal for JOBS — unrelated to its HTTP
  status here. Non-goals: auth (rejected), problem+json migration, i18n of messages.

## Build record
- Shipped `0cfc366` (Lot A, opus). All 4 criteria met. error_code completed:
  402→budget_exhausted (one producer; more actionable than payment_required), 403→forbidden,
  429→rate_limited, 503→unavailable. `client_facing` mapping table (exhaustive match, NO
  wildcard — new core variants must be homed on purpose): Transact→422 verbatim,
  BudgetExhausted→402, Profile→400 redacted (msg embeds dirs), ReplayMiss→409 redacted,
  Http/Browser/Claude→502 redacted, Storage/Parse/Config/App/Io/Json/Other→500 generic.
  RowNotFound: reasoned LEAVE at 500 pinned by `row_not_found_stays_500_not_a_fabricated_404`
  (a tidy 404 would hide fetch_one-vs-fetch_optional bugs forever). Redaction logged
  server-side (error! for 5xx → Sentry). Inventory test scans routes+mcp sources for
  StatusCode:: uses, EXPECTED-diffs, closes name↔number via canonical_reason. TS client
  checked: passes code through, never branches — no breakage. Review: keep. Live-verified:
  422+"unprocessable" envelope on a real socket (smoke).
- REFUTED: the From leak was narrower than briefed — engine-touching routes already map
  explicitly (remote.rs 502, runtime.rs 400/413), so the new 4xx/502 arms are largely LATENT;
  the real reachable fix was Storage/Io/Json/Other redaction. Honest, kept.
- Banked: clients/typescript/src/http.ts:16 doc comment lists the old code vocabulary (outside
  write set — next TS touch).
