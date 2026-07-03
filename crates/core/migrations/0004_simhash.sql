-- Near-duplicate detection: 64-bit SimHash per record (stored as signed i64).
ALTER TABLE records ADD COLUMN simhash INTEGER NOT NULL DEFAULT 0;
