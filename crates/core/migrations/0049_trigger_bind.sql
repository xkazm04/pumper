-- N04 param binding and per-record fan-out: a trigger may LIFT values out of
-- the resolved `_trigger` envelope into the target's own top-level params, and
-- may fan one source event out into one hop per array element.
--
-- Two nullable columns (the `filters`/`plugin_hooks` pattern from 0016/0032):
-- NULL on both means "behave exactly as before", so every pre-existing trigger
-- keeps its behaviour byte for byte and no backfill runs.
--
--   bind  — JSON object {"<target param name>": "<JSON pointer>"} resolved
--           against the {template, _trigger} view BEFORE the target-schema
--           door. A pointer that resolves to nothing is the ledger outcome
--           `bind_miss` and the hop is NOT enqueued.
--   each  — JSON pointer to an ARRAY in that same view. One hop per element,
--           carrying `_trigger.item` + `_trigger.item_index`, capped by
--           `[triggers] fan_out_cap` with `_trigger.fan_out_truncated` stated.
ALTER TABLE triggers ADD COLUMN bind TEXT;
ALTER TABLE triggers ADD COLUMN each_path TEXT;
