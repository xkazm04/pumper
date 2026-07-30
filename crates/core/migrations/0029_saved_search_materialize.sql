-- Saved-search materialization (M13 "queries as datasets"): when both columns
-- are set, each saved-search run also executes the query and upserts the result
-- set into <materialize_app>/<materialize_dataset> as ordinary dataset records
-- (key = the search doc id). The dataset change feed then emits new/changed/
-- removed deltas for the view, so watches, dataset triggers, `?filter=` and
-- export all compose over full-text semantics with no new machinery.
ALTER TABLE saved_searches ADD COLUMN materialize_app TEXT;
ALTER TABLE saved_searches ADD COLUMN materialize_dataset TEXT;
