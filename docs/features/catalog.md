# Data-source catalog

`catalog/data-sources.toml` is the machine-readable registry of **every data pipeline on this machine** — one `[[source]]` entry per source. It answers, without reading any app: which markets are covered, by what mechanism, how often, how fresh, and how trustworthy. It is not documentation-by-convention: it is parsed by `crates/core/src/catalog.rs`, served over the API, and cross-checked against the live app registry by tests that fail on drift.

Schema reference and a rendered overview table live in [`catalog/README.md`](../../catalog/README.md); the field list is repeated in the TOML header. The Path B contract (ONBOARDING.md §10) requires that adding or changing a scraping app updates its `[[source]]` entry **in the same change**.

## Source entry

Fields (all optional except `id`, `name`, `status`; missing values default rather than failing the parse):

| field | meaning |
| --- | --- |
| `id` | stable kebab-case slug; equals the app's `name()` when 1:1 |
| `app` | serving crate under `crates/apps/<app>`; `""` when not built yet |
| `market` | jurisdiction in the app's scheme — `us`, `us-ca` (California), `eu`, `au`, `gb`, `cz`, `ca` (Canada). **`us-ca` = California, `ca` = Canada** |
| `name` / `url` | human name and primary endpoint |
| `category` | `open-calls` · `awarded-history` · `registry` · `labor-market` |
| `engine` | `http` · `browser` · `claude` · `bulk` (import mechanism, not the Pumper engine trait) |
| `access` | `key-free` · `api-key` · `bulk` · `scrape` |
| `cadence` | `one-time` · `on-demand` · `daily` · `weekly` · `monthly` · `quarterly` · `annual` |
| `cron` | exact 6-field expression when on the scheduler; `""` otherwise |
| `status` | `live` (registered and running) · `planned` · `blocked` |
| `confidence` | 1–5, how much this source makes downstream output trustworthy |
| `dataset` | the dataset it writes via `ctx.upsert`; `""` if n/a |
| `notes` | freeform flags / gotchas |

A source that has only been researched still gets an entry, with `status = "planned"` and `app = ""` — so the catalog doubles as the roadmap and "live vs planned" stays honest.

`Catalog::load()` reads `$PUMPER_CATALOG` or `./catalog/data-sources.toml` (**CWD-relative**). A **missing** file is an empty catalog plus a warn log, so a deployment without it still boots; a **malformed** file is a hard error.

## Declared data contracts — and when each clause can actually fire

A source may carry a `[source.contract]` block (field reference in [`catalog/README.md`](../../catalog/README.md)): `required_fields`, `types`, `ranges`, `max_row_delta_pct`, `max_staleness_hours`. It is evaluated by `enforce_contracts` in the worker, at the same choke point where `suppress_unhealthy` gates pushes, and the latest verdict per `<app>/<dataset>` surfaces on `/catalog/health` and `/sources`. Verdicts are `pass` / `warn` / `block`; `[contracts] enforce` is `false` by default, so violations are recorded and surfaced but gate nothing.

Two lookups decide whether a declaration does anything at all, and **both key on the `(app, dataset)` pair the data actually lands under** — the pair carried by each stored revision, not the app whose job wrote it:

- `Catalog::contract_for(app, dataset)` finds the contract, so a row filed under an app that never writes that pair is never evaluated.
- `/catalog/health` reads `datasets.list(source.app, source.dataset)`, so the same mismatch reports the source **permanently stale** rather than reporting nothing.

A row that names a **virtual namespace** (`grants`, `census`) therefore cannot be `live` today: `live_catalog_entries_map_to_registered_apps_with_matching_cron` panics unless `app` is a registered app, and a virtual namespace is by definition not one. `grants/opportunity_details` and `census/market_blend` are the two datasets this blocks; both are documented in place in the TOML. Filing them under a producing app instead is the failure mode above, not a workaround.

**`max_row_delta_pct` is a mass-delete tripwire, and it only fires on a tombstoning write.** `Contract::evaluate` computes the delta only when `removed > 0`, and `removed` is populated only by `Datasets::sync_many` — the full-snapshot variant. On an **upsert-only** source (`upsert_many`, `upsert_many_with_provenance`, `upsert_many_derived`) removals never occur, so the declaration can never fire and reads as coverage it does not provide. Declare it where the write is `sync_many` (`cordis-topic-stats`), and leave it off where the write is upsert-only — `grants-gov`'s was removed for exactly this reason on 2026-08-13, with the reasoning recorded above the block. **Known inert:** `ca-grants` and `eu-sedia` still declare one on upsert-only writes.

## API

- `GET /catalog/sources?market=&status=&category=` → `{count, sources: [Source]}`. Filters are exact-match on the trimmed field; an absent or empty filter matches everything.
- `GET /catalog/health` → `{checked, stale, sources: [{id, app, dataset, cadence, expected_max_age_secs, last_write_at, age_secs, stale, monitored, reason?}]}`. The freshness monitor: for every **live** source that names a `dataset` and a cadence with a freshness expectation, it reports when that dataset was last written and whether the age exceeds the cadence window × a grace multiplier of **2** (so a daily source is flagged only past ~2 days). `monitored: false` when the source names no dataset/app or its cadence carries no expectation (`on-demand`, `one-time`, unknown). Cadence windows: daily 1d, weekly 7d, monthly 31d, quarterly 93d, annual 366d.

`/catalog/health` answers "did this source run recently?"; `GET /sources` (see [resilient-extraction.md](resilient-extraction.md)) answers "was what it produced any good?" — the two are complementary and cross-link in their responses.

## Drift enforcement

Two tests in `crates/server/src/routes/mod.rs` embed the TOML at compile time (so they don't depend on the test's working directory) and fail on drift:

- **`live_catalog_entries_map_to_registered_apps_with_matching_cron`** — every `live` source must name a non-empty `app`, that app must be registered, and `cron` must agree with the app's `schedule()` **in both directions** (an empty `cron` means the app has no schedule).
- **`every_registered_data_source_app_is_in_the_catalog`** — every registered app must be cataloged or listed in the explicit `CATALOG_EXEMPT` array. Exemptions are generic tooling (`crawl`, `extractor`, `plugin`, `readable`, `research`, `watch`), the `hackernews` template, and sibling-product consumers outside this catalog's grant/labor scope. Adding a new in-scope app without an entry fails the build.

## Connector watch manifest

`catalog/connector-docs.json` is a separate, **generated** artifact: the watch list for the [`connector-api-watch`](apps.md) app (connector slug, label, `docs_url`, icon, colour). It is produced outside this repo (`scripts/events/generate-connector-events.mjs` in the Personas repo) and copied in — do not hand-edit. The app reads it from `catalog/connector-docs.json` by default, overridable per job via the `manifest` param.

## Known gaps

- The rendered overview table in `catalog/README.md` is maintained by hand from the TOML; nothing regenerates or verifies it, so it can lag the TOML it summarizes.
- `catalog/` is in the doc-sync hook's `SKIP_PATTERNS` (`scripts/docs/check-doc-sync.mjs`), so editing the catalog does **not** trigger the reminder for this doc even though a map entry exists.
- `confidence` and `notes` are advisory: nothing reads them programmatically.
