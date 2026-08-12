---
slug: watch-honesty
type: perfect/direction
context: "[[automation-api]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-12
accepted: 2026-08-12
shipped: 2026-08-12
commit: 5ee2462
---

## What & why
Watches — the notify-on-change surface — can be dead on arrival in both directions with
zero signal. POST /watches {app:"grants"} 404s because "grants" is a virtual namespace not
in the registry, yet the fan-out (notify_watches) matches on exactly that entry app — the
place every grant revision lands is unwatchable. Inversely, {app:"ca-grants",
dataset:"unified"} is accepted, sits enabled forever, and can never fire (ca-grants writes
under "grants"). ?app= filters on /watches and /triggers accept any string and return
200 with an empty list — the exact unvalidated-filter anti-pattern the same file's
delivery-status validator was built to kill. And there is no path from a watch to its
deliveries (status is the only delivery filter), so "did watch X ever deliver?" is
unanswerable over the API.

## Evidence
- watches.rs:86-91 — registry contains_key then 404; registry.rs has no "grants" (verified
  live 2026-08-12); worker.rs:1324-1378 fan-out matches entry app incl. virtual namespaces;
  grants-common UNIFIED_APP = "grants".
- triggers.rs:135-136 — triggers explicitly allow virtual source_app; opposite rule.
- watches.rs:40 + triggers.rs:44 — ?app= passed raw to SQL; precedent triggers.rs:477-494.
- storage.rs list_deliveries filters status only; deliveries carry ref_id/kind unqueried.

## Acceptance criteria
- A watch can be created on any namespace the fan-out can actually deliver for: registered
  apps PLUS known virtual entry namespaces. Builder verifies how the honest set is
  derivable (registry? grants-common const? worker fan-out logic?) and gates creation on
  it — a watch that structurally cannot fire is refused with a message naming the
  namespace the data actually lands under (the ca-grants/unified trap gets that hint).
- ?app= on GET /watches and GET /triggers validates against the same honest set → 400
  with the known-values message, mirroring validate_delivery_status; test pins it.
- Deliveries are reachable from a watch: a ref_id-scoped filter on GET /webhooks/deliveries
  or GET /watches/{id}/deliveries — builder picks the shape consistent with existing cursor
  conventions; e2e proves watch to delivery-row traceability.
- GET /watches carries minimal honesty enrichment: at least last delivery status/time (or
  explicit null so never-fired is distinguishable from firing-and-dead).
- New/changed routes appear in the OpenAPI spec + EXPECTED inventory.

## Risks / non-goals
- Non-goal: resilience/contract suppression visibility (recorded gap, different seam);
  watch update verbs and dedup constraints (banked).
- Risk: the virtual-namespace set must not drift from the fan-out's actual behavior —
  prefer deriving both from one function/const over a second hand-kept list.

## Build record
Continuation builder (A2), commit `5ee2462`. `NamespaceIndex` unions FOUR sources:
registry + `registry::VIRTUAL_NAMESPACES` bootstrap seed (grants, publisher-pinned to
the registry by test) + the STORE (`list_all_datasets` — the running authority, since
index_datasets namespaces are declared in job RESULTS at runtime and app-peer writes
under caller-supplied params.namespace; no compile-time list can enumerate them) +
saved-search materialize_app (a second notify_watches call site the evidence missed).
`watch_target_refusal`: unknown namespace → 404 with publishes_into hint; the
ca-grants/unified trap → 400 naming grants (store-derived, so e2e-proven; a dataset
nothing has written is ACCEPTED — a new app's first dataset is indistinguishable from a
typo, and `last_delivery: null` is the surface that reveals a dead watch).
`validate_app_filter` on /watches; SEPARATE `trigger_filter_values` on /triggers —
builder REFUTED the "same honest set" criterion: triggers.source_app for external
triggers holds ingress source ids/`*`, not apps; the watch set alone would 400 filters
that return rows. `GET /watches/{id}/deliveries` (OpenAPI + EXPECTED), backed by
`list_deliveries_for_ref_page`; `GET /watches` carries explicit-null `last_delivery`.
7 e2e (watch_honesty.rs). Known gaps recorded, not papered over: `trades` virtual
namespace is accepted but trades-common emits no index_datasets so it never enters the
fan-out (fix = index_datasets in trades-common — BANKED); VIRTUAL_NAMESPACES pins to
the registry, not grants_common::UNIFIED_APP (rename there wouldn't fail a test);
namespace_index adds 2 queries to list/create endpoints (not hot paths, unbenchmarked).
Gates: full workspace 1372/0; smoke 25/25 (grants watchable 201 + bogus-filter 400 +
last_delivery checked live).
