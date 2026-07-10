-- Per-job spend ceiling: metered engine calls (Claude tier) refuse to run once
-- the job's cumulative cost_events total reaches this. NULL = unlimited.
ALTER TABLE jobs ADD COLUMN budget_usd REAL;
