-- 0046: the api-recipe lifecycle made explicit.
--
-- Before this migration `api_recipes` carried a boolean (`validated`) and an
-- integer nobody read below the validated rung (`consecutive_failures`, 0028).
-- `record_failure` incremented the counter and its ONLY consumer was guarded on
-- `validated != 0`, so once a recipe was unvalidated that branch was unreachable
-- for that row forever: the counter was write-only, and `best_for_host` — which
-- reads neither — kept returning the same dead candidate first for its host on
-- the pre-archive path of every fetch, as a live outbound replay.
--
-- Two additive, nullable columns give the table its missing states:
--   `retired_at`       NULL = live (candidate or validated); stamped = retired,
--                      and a retired recipe is invisible to `best_for_host`.
--                      Reversible: `POST /recipes/{id}/revive` clears it.
--   `last_failure_at`  when the most recent strike landed, so `GET /recipes`
--                      can show an operator WHEN a recipe started failing, not
--                      only how often. Cleared by a successful replay.
--
-- Nothing in this repo deletes a recipe row, and nothing here starts to: the
-- irreversible door is deliberately absent. Retirement is the terminal state
-- and it is reversible.
ALTER TABLE api_recipes ADD COLUMN retired_at TEXT;
ALTER TABLE api_recipes ADD COLUMN last_failure_at TEXT;

-- Back-fill: a candidate already past the shipping `[recipes] retire_after`
-- (10) was being replayed on every fetch of its host before this migration
-- existed, so it retires on arrival rather than needing ten more live strikes.
--
-- Three guards, each load-bearing and each pinned by
-- `crates/core/tests/recipes_lifecycle.rs`:
--   `retired_at IS NULL`  — idempotent, and a second pass never re-stamps.
--   `validated = 0`       — a validated recipe takes the OTHER rung
--                           (`max_failures` demotes it to candidate first);
--                           retiring one here would skip its demotion.
--   no write to `consecutive_failures` — the strike count is the evidence an
--                           operator reads to decide whether to revive. A
--                           back-fill that reset it would retire the row and
--                           erase the reason in the same statement.
UPDATE api_recipes
   SET retired_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
 WHERE retired_at IS NULL
   AND validated = 0
   AND consecutive_failures >= 10;
