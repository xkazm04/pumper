---
slug: watch-honesty
type: perfect/direction
context: "[[automation-api]]"
lens: robustness
status: accepted
size: M
proposed: 2026-08-12
accepted: 2026-08-12
shipped: —
commit: —
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
(pending)
