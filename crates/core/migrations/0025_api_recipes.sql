-- API X-ray (M05): discovered JSON API endpoints behind rendered pages.
-- Written by the discovery pass over `capture_network` renders; `validated`
-- stays 0 until a successful replay of the template proves the recipe.
-- Keyed by (host, url_template): re-discovery refreshes shape + last_seen_at.
CREATE TABLE IF NOT EXISTS api_recipes (
    id            TEXT PRIMARY KEY,
    host          TEXT NOT NULL,
    url_template  TEXT NOT NULL,
    params        TEXT NOT NULL DEFAULT '{}',   -- observed example values (JSON object)
    json_paths    TEXT NOT NULL DEFAULT '[]',   -- generalized data paths (JSON array)
    score         REAL NOT NULL DEFAULT 0,      -- extracted-value overlap share
    validated     INTEGER NOT NULL DEFAULT 0,
    discovered_at TEXT NOT NULL,
    last_seen_at  TEXT NOT NULL,
    UNIQUE (host, url_template)
);
