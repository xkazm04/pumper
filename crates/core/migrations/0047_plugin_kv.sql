-- N10: the per-plugin key/value namespace behind `pumper_kv_get` /
-- `pumper_kv_put` (docs/features/trigger-plugins.md §capabilities).
--
-- The namespace is the PRIMARY KEY's first column, not a convention: a plugin
-- names a key, the host names the plugin, so one connector cannot read or
-- overwrite another's cursor even if it guesses the key. This is the whole
-- isolation story of the capability — there is no cross-plugin read at all.
--
-- Nothing is written here unless a plugin declares `capabilities.kv = true`,
-- which no shipped plugin does. The table exists so the seam has a home before
-- it has a caller.
CREATE TABLE IF NOT EXISTS plugin_kv (
    -- The loaded plugin's name (its `.wasm` file stem), supplied by the HOST.
    plugin     TEXT NOT NULL,
    key        TEXT NOT NULL,
    value      TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (plugin, key)
);

-- Answers "how much has this plugin stored", which is what the per-plugin key
-- ceiling is checked against on every put.
CREATE INDEX IF NOT EXISTS idx_plugin_kv_plugin ON plugin_kv (plugin);
