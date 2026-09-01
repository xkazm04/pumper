-- 0041 (N14): the API X-ray's validation verdict, stored instead of inferred.
-- `validated` alone cannot distinguish "never tried" from "tried and refused",
-- which is exactly what an operator reading `GET /recipes` needs to know before
-- trusting (or deleting) a discovered recipe. `validation_reason` is the short
-- verdict of the last replay ("replay matched the expected field paths", "non-
-- JSON payload", ...) and `validated_at` is when that verdict was reached.
ALTER TABLE api_recipes ADD COLUMN validation_reason TEXT;
ALTER TABLE api_recipes ADD COLUMN validated_at TEXT;
