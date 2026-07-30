-- Derived datasets (M11 v1): a dataset declared as a filter/project(/lookup)
-- transformation of another dataset in the SAME app namespace. Specs are data,
-- not code: on every upsert_many the store feeds the batch's fresh keys through
-- the enabled specs whose (source_app, source_dataset) matches and upserts the
-- shaped rows into (source_app, target_dataset) in the same flow. Chains are
-- bounded by `[derived] max_depth`; cycles are rejected at spec-create time.
CREATE TABLE IF NOT EXISTS derived (
    id             TEXT PRIMARY KEY,
    source_app     TEXT NOT NULL,
    source_dataset TEXT NOT NULL,
    target_dataset TEXT NOT NULL,
    filters        TEXT NOT NULL DEFAULT '[]',  -- JSON array of `$.path:op:value` specs (ANDed)
    project        TEXT NOT NULL DEFAULT '{}',  -- JSON object {out_field: "$.path"}; {} = passthrough
    lookup         TEXT,                        -- JSON {dataset, key_expr, merge_as} or NULL
    enabled        INTEGER NOT NULL DEFAULT 1,  -- per-spec kill-switch
    created_at     TEXT NOT NULL,
    CHECK (source_dataset <> target_dataset)
);
CREATE INDEX IF NOT EXISTS idx_derived_source
    ON derived (source_app, source_dataset, enabled);
