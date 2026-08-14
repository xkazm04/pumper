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

**A row may name a virtual namespace** (`grants`, `census`) rather than a registered app, because that is the pair those datasets land under. `live_catalog_entries_map_to_registered_apps_with_matching_cron` accepts a namespace declared in `registry::VIRTUAL_NAMESPACES` provided **every one of its publishers is a registered app**, so this widens what may be named, not whether it is checked. Filing such a dataset under a producing app instead is the failure mode above, not a workaround.

Two rules come with it:

- **`cron` must be empty.** A virtual namespace has no `schedule()` of its own — the write rides a publisher's job — and `Catalog::reconcile` derives desired schedules from `Source::is_scheduled()`, so a cron here would ask the scheduler to create a schedule for an app that does not exist. The guard asserts this rather than trusting it. Hold the cadence with `max_staleness_hours` instead.
- **The cron-equality check is skipped** for these rows, and only for them; every app-named row still has to match its app's `schedule()` exactly in both directions.

`grants/opportunity_details` is live under this shape as of 2026-08-13. `census/market_blend` is the remaining candidate and is documented in place in the TOML.

**`max_row_delta_pct` is a mass-delete tripwire, and it only fires on a tombstoning write.** `Contract::evaluate` computes the delta only when `removed > 0`, and `removed` is populated only by `Datasets::sync_many` — the full-snapshot variant. On an **upsert-only** source (`upsert_many`, `upsert_many_with_provenance`, `upsert_many_derived`) removals never occur, so the declaration can never fire and reads as coverage it does not provide. Declare it where the write is `sync_many` (`cordis-topic-stats`), and leave it off where the write is upsert-only — `grants-gov`'s was removed for exactly this reason on 2026-08-13, with the reasoning recorded above the block. **Known inert:** `ca-grants`, `eu-sedia` and `state-licensing` still declare one on upsert-only writes (`state-licensing` writes via `upsert_many_with_provenance` + `upsert_many_stamped`; found in the round-21 sweep, which is also why this list is now three names and not two — an audit found the list itself had drifted).

### A verdict is recorded, not maintained — so it is served with its age

`enforce_contracts` writes the latest verdict per `<app>/<dataset>` into an in-memory map on `AppState`. That map has exactly one mutation in the whole workspace — the worker's `insert`. There is no remove, no clear, no catalog generation, and the worker only iterates *this run's* datasets. A verdict therefore outlives the run that produced it, the dataset that stopped producing, and the source that was retired.

Since round 24 both read surfaces age the verdict instead of serving it bare. Each rendered verdict carries, beside the `job_id`/`checked_at` the worker already stamps:

- `age_secs` — seconds since `checked_at` (null if the stamp is unparsable).
- `stale` — `true` when the verdict is older than the source's freshness window, or when **no live catalog source declares that `<app>/<dataset>` any more** (retired/renamed/deleted: `/sources` joins by health-store id, which keeps rows through `retired`). `false` when inside the window. **`null` = cannot be judged**, never a silent `false`.
- `stale_reason` — present whenever `stale` is `true` for a non-age reason or `null`.

The window is `Source::freshness_window_secs(grace)` — the *same* expression `/catalog/health` judges dataset writes by (cadence × grace 2, tightened but never loosened by `max_staleness_hours`), so "did it run" and "was the last verdict current" cannot drift apart. A source with neither a cadence expectation nor `max_staleness_hours` yields `stale: null`.

### Configured enforcement vs observed enforcement

`Catalog::load()` treats a malformed file as a hard error, and `enforce_contracts` is **deliberately fail-open**: it warns and returns *per job*, so one unparsable TOML file means the whole fleet evaluates zero contracts while nothing blocks delivery. That is the intended trade — but until round 24 the only evidence was two log lines and two 500s (`/catalog/sources`, `/catalog/health`), while `/sources` kept serving `contracts_enforce: true` beside the last-good verdicts.

`ContractsStatus` (`crates/core/src/catalog.rs`) is now that difference, and it is reported in two places:

- **At boot**, one line from `main::log_contract_observability`: `error!` naming the parse error and its consequence when the catalog will not load, `info!` with `declared`/`enforce` when it will. Nothing blocks — visibility only.
- **On `GET /sources`**, as the `contracts` object: `{enforce_configured, enforce_observed, catalog_ok, catalog_error?, declared, reason?}`. `enforce_observed: false` beside `enforce_configured: true` means the catalog would not parse and nothing is being checked. `declared: 0` with `catalog_ok: true` means enforcement is real but has nothing to judge. The older top-level `contracts_enforce` is unchanged and remains the *configured* value.

**Virtual namespaces are watchable before their first record.** `trades` joins `grants` as a virtual
namespace in `registry::VIRTUAL_NAMESPACES`: no app is called `trades`, and five apps
(`state-tax`, `state-licensing`, `trade-wages`, `homewyse-pricing`, `valuation-multiples`) publish
`trades/operator_economics` + `trades/compliance` into it. Since round 21 every one of them declares
those datasets via `index_datasets`, which is what lets `/catalog/health`, watches, dataset triggers
and `enforce_contracts` see their revisions at all — the worker's fan-out is scoped by
`run_indexed_apps`, so an undeclared cross-source dataset is invisible to every one of those
mechanisms no matter how often it is rewritten. `cms-fee-schedule` is deliberately not a publisher:
it is in the same product group but writes only its own datasets and never rebuilds the join.

## API

- `GET /catalog/sources?market=&status=&category=` → `{count, sources: [Source]}`. Filters are exact-match on the trimmed field; an absent or empty filter matches everything.
- `GET /catalog/health` → `{checked, stale, contracts_enforce, sources: [{id, app, dataset, cadence, expected_max_age_secs, last_write_at, age_secs, stale, monitored, reason?, contract?}]}`. `contract.last_verdict` carries its own `age_secs`/`stale`/`stale_reason?` (see above). The freshness monitor: for every **live** source that names a `dataset` and a cadence with a freshness expectation, it reports when that dataset was last written and whether the age exceeds the cadence window × a grace multiplier of **2** (so a daily source is flagged only past ~2 days). `monitored: false` when the source names no dataset/app or its cadence carries no expectation (`on-demand`, `one-time`, unknown). Cadence windows: daily 1d, weekly 7d, monthly 31d, quarterly 93d, annual 366d.

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
- **`/sources` observes the catalog, it does not police it.** A verdict is marked stale; it is never removed, and a stale `pass` is still the last thing the source actually did. Nothing re-evaluates a contract for a dataset that stopped producing — only a new run writes a new verdict.
- Verdicts are per-process and in-memory: after a restart every `contract` key is absent until the source runs again, which reads as "no verdict", not as a failure.
- The boot catalog check logs; it does not gate. A fleet can start, and keep running, with a catalog that will not parse.
