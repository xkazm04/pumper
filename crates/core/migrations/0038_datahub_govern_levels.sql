-- Last-acted remote state per governance target: the memory that turns the
-- DataHub actuator from a level-follower into a transition-follower.
--
-- Before this table, governance acted on the LEVEL of the remote signal: a
-- deprecated dataset re-disabled the app's catalog-managed schedules on EVERY
-- poll, so an operator re-enabling one had it switched off again within the
-- poll interval, forever, with no override. Acting on the CHANGE instead means
-- a manual re-enable is respected until DataHub itself changes its mind.
--
-- It has to be durable, not in-memory: after a restart, a schedule the remote
-- still wants disabled must not be disabled a second time (that is exactly the
-- flap this replaces). Separate from `datahub_govern_actions` on purpose — the
-- audit trail is age-pruned, and pruning the memory would resurrect the flap.
-- Bounded by (signals × apps), so it needs no retention at all.
CREATE TABLE IF NOT EXISTS datahub_govern_levels (
    -- Which remote signal: currently only 'deprecation' (see
    -- `crate::storage::DATAHUB_GOVERN_EVIDENCE`).
    signal     TEXT NOT NULL,
    -- The Pumper app the level is about.
    target     TEXT NOT NULL,
    -- The level governance last ACTED on: 1 = signal was on and was acted upon,
    -- 0 = signal has since cleared. An absent row means "never seen on".
    level      INTEGER NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (signal, target)
);
