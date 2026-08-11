---
name: browser-transact           type: perfect/context
group: Content & Research Apps   category: lib
opportunity: 6                   # single app, but the evidence bundle IS the product and it currently lies
last_proposed: 2026-08-11        cooldown_until: round-12
directions: ["[[transact-evidence-honesty]]", "[[transact-secret-redaction]]", "[[transact-retry-safety]]"]
---

## Current state (scout brief digest, 2026-08-11 — full pipeline traced)

App `transact` (crates/apps/transact/src/lib.rs, 317 LOC, one substantive commit `33180ec`
2026-07-31, untouched since). Registered (registry.rs:38), enqueueable via POST /apps/transact/jobs
and MCP; deliberately CATALOG_EXEMPT (routes/mod.rs:352). Path: app validate → raw
`ctx.engines.browser.transact` (pinned chokepoint exemption, fetch_chokepoint.rs:86-88) →
BrowserEngine::transact maps onto RenderRequest (engine-browser:678-695) → evidence_from_render →
two artifacts (dom.html, evidence.json) + result JSON. Emits ZERO dataset records; no user-facing doc.

**Stop-before-submit is genuinely structural** (dataflow: submit_action never reaches
execute_actions — engine-browser:689-691) with defense-in-depth validation. But:
- **Evidence lies**: `steps_completed` counts ATTEMPTS not successes (`completed += 1` outside the
  match arms, engine-browser:831 — a flow whose every selector 404'd reports the same number as a
  perfect run); `selector_found` is computed then DISCARDED by evidence_from_render (:706-719);
  the submit target is never assessed on the final page; profile (identity!) not recorded.
- **Secrets leak 4 ways**: filled_fields_js reads `.value` with no input[type=password] mask and no
  cap (engine.rs:378-388) → evidence.json + job result + SSE + webhooks; dom.html unredacted;
  jobs.params plaintext; goto errors echo full URL incl. query tokens (engine-browser:530).
- **Retry semantics wrong for an acting app**: Error::Transact not terminal (core error.rs:77-79)
  → deterministic refusals ride the backoff ladder; any retryable failure re-runs the whole acting
  flow (re-type, re-click, re-trigger OTP); idempotency_key recorded but consumed by NOTHING.
- **Door validation weak**: schema allows `submit: true` (run-time reject only), no
  unknown-field rejection (typo'd `"step"` runs a 0-step flow that looks successful), whitespace
  key passes minLength, typo'd profile silently creates an empty logged-OUT Chrome profile
  (engine-browser:207-212,226).
- DOM-over-cap after the flow ran → total evidence loss (engine-browser:645-652); whole action
  list shares one nav_timeout (:565) with silent truncation; iframes/shadow DOM silently unmatched.
- Tests: 8 pure/mocked; execute_actions has ZERO tests; no e2e; no Chrome #[ignore] test.

## Direction history
- 2026-08-11 (round 10, director-self-gated autonomous): 5 drafted, 3 accepted —
  [[transact-evidence-honesty]] · [[transact-secret-redaction]] · [[transact-retry-safety]].
  **REJECTED-deferred: evidence-access endpoint** (GET /jobs/{id}/artifacts/{name} — reviewer
  currently needs shell access to read the bundle; real value, but write set collides with the
  round-10 api-surface lot (routes/mod.rs EXPECTED) and pool cap 6 was reached. BANKED as this
  context's next anchor — generalizes to crawl/extractor/provisioner artifacts too.)
  **REJECTED: flow-budget param** (dedicated action-list timeout — honesty about the shared 30s
  budget ships inside evidence-honesty (`deadline_hit` + requested-vs-completed); a new param has
  no consumer asking and nav_timeout_secs exists.)

## Banked seeds (re-verify at proposal time — seeds decay)
- Evidence-access endpoint (above) — the context's next anchor.
- URL policy for acting flows: no scheme/private-IP guard; a transact job can point a logged-in
  Chrome at http://127.0.0.1/… (engine-browser:522, no robots/no allowlist). Design-heavy (SSRF
  taxonomy, opt-outs) — pair it with the same guard for fetch-proxy if taken.
- iframe/shadow-DOM field support; screenshot capture (screenshot_path honest-None since v1);
  live-submit human-approval slice (pending-approval + approve endpoint) — the documented v2,
  needs the OWNER's product call, do not self-gate it in an autonomous round.

## Shipped
- 2026-08-11 (round 10): [[transact-evidence-honesty]] → `428d2e9` (per-step outcomes,
  successes-only steps_completed, selector_found wired, submit-target probe, profile +
  dom_bytes/dom_truncated, truncate-don't-destroy on the transact path) ·
  [[transact-secret-redaction]] → `f0ddec5` (in-page masking for password/credential inputs,
  512-char cap, decode-side re-enforcement, end-to-end no-sentinel proof) ·
  [[transact-retry-safety]] → `8e17ca7` (Error::Transact terminal, door tightened
  schema+app-side with the trigger-bypass discovery, missing profile refuses pre-Chrome,
  Dead::transact panics). Full gate 1314/0 + live smoke 21/21 (3 door 422s + declared schema
  driven on a real socket). Observed effect: the bundle a human approves off now distinguishes
  a total miss from a clean run, can't republish secrets, and a refusal costs one attempt.
- Still open after round 10 (verified during build): no live-Chrome exercise of the new probe
  JS (needs a fixture page in engine-browser/tests/render.rs); execute_actions loop has no mock
  seam (accounting extracted+tested); dom.html itself and goto-error URLs still unredacted;
  submit_target visibility is a heuristic signal.
