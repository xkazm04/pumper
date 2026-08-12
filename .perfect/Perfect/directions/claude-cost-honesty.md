---
slug: claude-cost-honesty
type: perfect/direction
context: "[[claude-engine]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-12
accepted: 2026-08-12
shipped: 2026-08-12
commit: c7f5c37
---

## What & why
Money the CLI reports as spent is thrown away on every failure path: an
`is_error: true` envelope carries `total_cost_usd` in the same object the engine
discards; a non-zero exit discards stdout entirely (which can hold a valid envelope
with cost); the chokepoint meters only on `Ok`. So a run that burns to its budget
and then errors records **$0** in `cost_events`, `SpentTotal` never advances, and
the job's budget ceiling is structurally unenforceable for exactly the runs most
likely to be expensive. This defect was documented 2026-07-14 (bughunt,
fetch-engines.md:30-31) and is still live. Also: a schema-constrained call whose
`result` is a non-string silently produces `text: ""`, which the research cache
refuses to store (`cache.rs:528`) — the call re-pays the model on every repeat with
no signal. The user moment: "my budget said $5, the ledger says $1.20, my bill
says $9 — and my scheduled job re-paid for the same answer every night."

## Evidence
- `crates/engine-claude/src/lib.rs:160-165` — `is_error` → `Err`, `total_cost_usd`
  discarded from the same envelope; `:154-159` unparseable stdout likewise.
- `crates/engine-claude/src/lib.rs:145-152` — non-zero exit: stdout (which may hold
  the envelope) discarded, only stderr reported.
- `crates/core/src/app.rs:436-438` — chokepoint meters `out.cost_usd` only on `Ok`;
  the `?` on `:436` skips metering entirely on `Err`.
- `crates/core/src/fetcher.rs:625` — tier-3 research call; error cost likewise lost
  (the ladder traces the error but no cost reaches the meter).
- `crates/engine-claude/src/lib.rs:167` — `result.as_str().unwrap_or_default()`:
  non-string result → `text: ""` → `crates/core/src/cache.rs:528` refuses to cache.
- `crates/core/src/error.rs:8` — `Error::Claude(String)` cannot carry cost.
- `docs/harness/refactor-bughunt-2026-07-14/fetch-engines.md:30-31` — known,
  un-actioned.

## Acceptance criteria
- [ ] On EVERY failure path where the CLI produced a parseable envelope (is_error,
      non-zero exit with envelope on stdout), the engine extracts `total_cost_usd`
      and the error carries it in structured form. Design options (builder picks
      with reasoning): restructure `Error::Claude` into a struct variant
      `{ message, cost_usd: Option<f64> }` (mechanical ripple, grep-bounded), or a
      typed wrapper the chokepoint downcasts. String-embedding the cost is NOT
      acceptable — that's the string-matching anti-pattern.
- [ ] `AppContext::research` meters the carried cost before propagating the error,
      with a detail naming the failure class (e.g. `failed_spend (is_error)`), so
      `SpentTotal`/budget clamps see it. Same for the tier-3 path if its plumbing
      differs (`fetcher.rs:625` — verify where its cost lands and close the same
      hole; if it needs no change, prove it with a test).
- [ ] Timeouts cannot report cost (no envelope). Record a $0 cost_event with detail
      `unmetered_timeout` at the chokepoint so the ledger at least shows THAT a
      paid call vanished unmetered — the receipt/economics surfaces must be able to
      say "spend on this job is a floor, not a total".
- [ ] Non-string `result` under `json_schema`: `text` falls back to the serialized
      `structured_output` (or the raw result value) so the answer is cacheable and
      non-empty. Pin with a test named for the anti-pattern
      (e.g. `schema_result_is_not_silently_empty_and_uncacheable`).
- [ ] A successful envelope MISSING `total_cost_usd` meters with a detail marking
      it unreported (`cost_unreported`), not a silent indistinguishable $0.
- [ ] Tests at both layers: engine tests (fake envelopes through the fake-CLI
      harness from [[claude-kill-tree]] or direct parsing seams) + core chokepoint
      tests (ScriptedResearcher erroring with cost → meter row asserted).

## Risks / non-goals
- Non-goal: token-level telemetry (banked separately).
- Non-goal: VCR-recording failed calls.
- Risk: `Error::Claude` variant reshape ripples — bounded; grep call sites first and
  report the count in the build record. Coordinate with [[claude-kill-tree]] (same
  lot, sequential — kill-tree first, then this).

## Build record
Original Lot C builder died pre-commit with the direction COMPLETE (all 6 criteria,
tests at 3 layers). Session -4 snapshotted the dirty tree via `git commit --only`
and landed it as `c7f5c37` after one Director fix: the catalog.rs test initializer
was missing the new `budget_usd` field — a Lot S ripple that `cargo check` misses
because it skips test cfg. Review verdict KEEP: `Error::Claude` struct variant with
`ClaudeSpend`; `ledger_event` is a pure decision (reported / timeout / spawn /
unreadable all distinct); both metered seams meter-before-propagate; tier-3 rides
`ladder_exhausted`; `cost_unreported` on unpriced success; `envelope_text` kills the
empty-text uncacheable class. Checked: cost travels in a FIELD, not the message.
Wave gate: `just ci` exit 0 on the wave tip (session -5).
