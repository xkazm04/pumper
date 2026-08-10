---
slug: close-date-timezone-honesty
type: perfect/direction
context: "[[grants-unified-layer]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-04
accepted: 2026-08-04
shipped: 2026-08-04
commit: 86716d3
---
## What & why
`parse_date` keeps only the date component, dropping time-of-day and timezone entirely, and
`sweep_closed` compares against `Utc::now().date_naive()`. A grant closing `23:59
America/Los_Angeles` is already "yesterday" in UTC — so the sweep can mark it **closed while it is
still open**, and it vanishes from `GET /grants?status=open` and from `closing-soon`. Silently hiding
money that is still available is the worst failure this product has.

## Evidence
- Truncation: `crates/apps/grants-common/src/lib.rs:801-817` (`2026-11-02T23:59:00Z` → `2026-11-02`).
- UTC "today": `lib.rs:527`; predicate `is_past_due_open` `lib.rs:578-585`.
- Downstream visibility: `crates/server/src/routes/query.rs:194-197` (`closing-soon` hardcodes
  `status = "open"`), so a wrongly-swept grant disappears from that view entirely.
- SEDIA already picks a deadline out of a multi-stage array (`lib.rs:284-304`) — the richer value is
  available at parse time and is being discarded.

## Acceptance criteria
- The sweep cannot retire a grant that is still open in its own source's timezone.
- A named predicate with boundary tests: a deadline hours either side of midnight, in a timezone
  behind and ahead of UTC.
- Records with unambiguous deadlines behave exactly as today (existing sweep tests stay green).
- Where a source publishes no timezone, the conservative direction is chosen — keep it open the
  extra day — and that choice is documented.
- Time-of-day is preserved where the source gives it, rather than discarded at parse.

## Risks / non-goals
Not a full timezone database per grant program. Do not change the canonical `close_date` field's
shape for consumers without saying so in the feature doc.

## Build record
(pending)
