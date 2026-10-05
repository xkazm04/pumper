-- 0045: say whether a failed extraction was billed, unmetered, or genuinely free.
--
-- `cost_events.cost_usd = 0.0` meant four different things and the only thing
-- telling them apart was a prefix of the free-text `detail` column, which no
-- endpoint selects, groups or filters on. A job that burned to its ceiling and
-- was killed was byte-identical, on every surface, to one free cache hit.
--
-- The vocabulary is closed and owned by `pumper_core::costs::SpendCertainty`:
--   billed    — cost_usd IS the bill, as the engine reported it
--   unmetered — a paid call happened and its amount is unreadable; 0.0 is a floor
--   unknown   — a child existed and may have spent; we cannot say whether it was
--               billed at all (a failed wait(), a broken pipe, a signal)
--   free      — no money moved: cache hit, free engine tier, VCR replay
--
-- DEFAULT 'free' keeps the column additive: every pre-existing reader
-- (`job_total`, `summary`, the retention pruner) ignores it, and the back-fill
-- below immediately replaces the default for every historical row.
ALTER TABLE cost_events ADD COLUMN spend_certainty TEXT NOT NULL DEFAULT 'free';

-- BACKFILL --
-- Derived purely from `detail` (which is never modified) and `engine`/`cost_usd`,
-- so it is deterministic and replaying it is a no-op. The `detail` spellings are
-- the ones `ClaudeSpend::ledger_event()` and `AppContext::success_spend_event`
-- actually write; the three `failed_spend_unreported` classes listed by name are
-- the endings that imply the model ANSWERED (exit 0, or a complete is_error
-- envelope), which is what separates `unmetered` from `unknown`.
UPDATE cost_events
SET spend_certainty = CASE
    WHEN detail LIKE 'failed_spend (%' THEN 'billed'
    WHEN detail = 'unmetered_timeout' THEN 'unmetered'
    WHEN detail = 'cost_unreported' THEN 'unmetered'
    WHEN detail IN (
        'failed_spend_unreported (unparseable)',
        'failed_spend_unreported (is_error)',
        'failed_spend_unreported (prompt_undelivered)'
    ) THEN 'unmetered'
    WHEN detail LIKE 'failed_spend_unreported (%' THEN 'unknown'
    WHEN engine = 'claude' AND detail IS NULL THEN 'billed'
    WHEN cost_usd > 0 THEN 'billed'
    ELSE 'free'
END;
