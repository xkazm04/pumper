---
slug: webhook-observability
type: perfect/direction
context: "[[webhook-delivery]]"
lens: feature
status: shipped
size: M
proposed: 2026-08-10
accepted: 2026-08-10
shipped: 2026-08-10
commit: c3d1f52
---

## What & why
The only way to notice that every webhook has been failing for six hours is to hand-poll
`GET /webhooks/deliveries?status=dead` — and the docs tell you the wrong status to ask
for. `/metrics` exports jobs, costs, schedules… and nothing about deliveries. This
direction gives the operator a delivery-health surface: gauges on `/metrics`, a
validated+honest `?status` filter, and docs that match the state machine that actually
ships. User moment: "my dashboard showed oldest-undelivered climbing, so I fixed my
endpoint before anything hit dead."

## Evidence
- `crates/server/src/routes/meta.rs:53-149` — `/metrics` has zero webhook series; the
  raw rows exist but no surface aggregates them (contrast: triggers got a decision
  ledger for exactly this, 5d99cc6).
- `crates/server/src/routes/triggers.rs:468,481` + `docs/features/events-webhooks.md:31`
  — all three call `failed` "the dead-letter view"; since ebf0954 `failed` means "still
  in the retry ladder" and the terminal state is `dead`. `?status=typo` returns an empty
  200, not a 400 (`storage.rs:1471-1473` binds whatever it gets).
- `docs/features/events-webhooks.md:37` still lists "no retention/purge job yet" —
  retention shipped (`config.rs:1039-1045`, `main.rs:353`). Sinks (`webhook`/`slack`/
  `file`, a first-class `POST /watches` param, `routes/watches.rs:68`) appear nowhere in
  docs/features. Retention keys `webhook_delivery_retention_days`/
  `webhook_dead_letter_retention_days` absent from shipped `config.toml`.

## Acceptance criteria
- [ ] `/metrics` exports webhook delivery health: counts by status (pending/failed/
      dead/delivered), oldest-undelivered age (seconds, over pending+failed), and
      deliveries attempted/succeeded (windowed or cumulative — follow the existing
      metrics idiom in meta.rs). One aggregate SQL query, not N.
- [ ] `?status=` on `GET /webhooks/deliveries` validates against the real state set
      (`pending|delivered|failed|dead`) → 400 with the allowed values on anything else;
      param doc + OpenAPI describe `failed` = retrying, `dead` = dead-letter.
- [ ] `docs/features/events-webhooks.md` rewritten to match code: the 5-state lifecycle
      with the ladder timings, `dead` as the DLQ view, retention keys (+ added as
      comments to shipped `config.toml`), and a Sinks section documenting
      `webhook|slack|file` semantics incl. the file-sink secret/url quirks
      (`routes/watches.rs:95,114-123`, `webhook.rs:96`).
- [ ] Smoke checklist (`scripts/smoke.ps1`) extended: `/metrics` contains the webhook
      series after the driven job; deliveries list rejects a bogus status with 400.
- [ ] Repo law: status validation lands as a named predicate with tests
      (`bogus_status_rejected_not_empty_list` style); metrics assembly tested at
      storage level.

## Risks / non-goals
- Non-goal: per-endpoint success-rate breakdown (needs URL cardinality decisions) —
  aggregate first; per-endpoint is a future direction if wanted.
- Non-goal: auth on the delivery log (PARKED decision, do not touch).
- Risk: `?status` validation is a behavior change (typo used to return 200 []) — it is
  the honest change; note it in the doc's changelog line.
- Overlap note: shares `routes/triggers.rs` and `storage.rs` with
  [[webhook-dlq-recoverability]] — same lot, sequenced after it.

## Build record
delivery_health one-pass aggregate; 4 /metrics series (attempted/delivered/failed/dead); ?status 400-validation via DELIVERY_STATUSES; events-webhooks.md rewritten (lifecycle table, Sinks section, secret-resolution table — the 'failed is the DLQ' doc bug dead); retention keys in config.toml; smoke 15->17 checks. Review: keep. Builder refutation: '5-state' claim was 4 persisted states + a queued phase — docs now say so.
