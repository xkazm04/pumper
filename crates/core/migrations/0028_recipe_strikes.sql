-- 0028: strike machinery for the api-recipe fetch branch (M05 step 4).
-- Counts consecutive failed/thin replays of a recipe; reset to 0 on a
-- successful overlapping replay, and a validated recipe that accumulates
-- `[recipes] max_failures` consecutive failures is un-validated (drops back
-- to opportunistic-only until a fresh successful replay re-proves it).
ALTER TABLE api_recipes ADD COLUMN consecutive_failures INTEGER NOT NULL DEFAULT 0;
