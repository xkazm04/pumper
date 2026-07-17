# Perf-Feature Scan — Wave G: Query & API surface (Theme G)

> 1 commit, **1 High finding** closed — the theme's only open High.
> Baseline preserved: build clean, tests **237 → 242** (+5 tests, 0 regressions).
> Branch `vibeman/wave-g-jsonfilter-2026-07-17` (off master after PR #8).

## Commit

| Commit | Finding | What |
|--------|---------|------|
| `51b276b` | http-api-routes #2 | expose the generic `JsonFilter` surface on `GET /datasets/{app}/{dataset}` and `.../export` |

## What was fixed

`Datasets::list_filtered(app, dataset, &[JsonFilter], after, limit)` was already
generic and shipped — it takes app/dataset as plain parameters and pushes
`Eq`/`Contains`/`Gte`/`Lte`/`NumGteAny` into SQL with proper keyset paging — but
the only route that reached it hardcoded `grants/unified`. Every other app could
only page the whole dataset (`?cursor=`) or stream the entire corpus (`/export`)
and filter client-side.

Added a repeatable **`filter`** query param with a compact grammar:

```
?filter=$.state:eq:CA&filter=$.employees:numgte:50
```

`<path>:<op>:<value>` where `op` ∈ `eq | contains | gte | lte | numgte` (`numgte`
ORs comma-separated paths). A shared `parse_filters(&[String]) -> Result<Vec<JsonFilter>, ApiError>`
parses them; malformed specs (missing op/value, non-`$.` path, unknown op,
non-numeric `numgte`) map to the existing `400 bad_request` path. The value keeps
any `:` after the op, so timestamps/URLs pass through.

- **`GET /datasets/{app}/{dataset}`** uses `list_filtered` when filters are
  present (live rows only), else the legacy `list`/`list_page` (unchanged).
- **`GET .../export`** threads the filters through its keyset batch loop, so a
  filtered export streams only matching rows — the single biggest payload win on
  this surface (whole-corpus stream → targeted one). Filters are validated up
  front so a bad spec is a clean 400, not a mid-stream abort.

### Implementation notes

- Repeated `?filter=` keys are read via `Query<Vec<(String, String)>>` because
  axum's typed `Query<Struct>` (serde_urlencoded) collapses duplicate keys. No
  new dependency.
- The typed **`/grants`** route is left as a documented convenience layer over the
  same `JsonFilter` engine — it already produces `Vec<JsonFilter>` for
  `list_filtered` (not a parallel SQL implementation), and its per-field typed
  400s + named OpenAPI params are worth keeping. Forcing it through the string
  grammar would regress that, so it stays as-is.
- `parse_filters` / `filter_specs` unit-tested (per-op parse, colon-in-value,
  multi-path numgte, all malformed cases, empty). OpenAPI route inventory
  unchanged (params only, no new routes) — the spec-coverage test still passes.

## Gate

```
cargo build --workspace   # clean
cargo test --workspace    # 242 passed / 0 failed  (was 237)
```

## Open Highs after this wave

18 of the original 36 remain (themes F caching, H introspection, I domain model,
J extraction power, plus E grants-gov #1 deferred). Themes A/B/D/G/K now have zero
open Highs.
