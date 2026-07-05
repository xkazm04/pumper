# Data-Source Catalog

**One place to see what this machine scrapes and how.** `data-sources.toml` is the
authoritative, machine-readable list of every data pipeline — one `[[source]]`
entry each. Humans skim the table below; LLMs parse the TOML. When you add or
change a scraping app, update the TOML in the **same change** (it's part of the
Path B contract — see `ONBOARDING.md` §10).

This format is **reusable by any app on this machine** — copy the schema and keep
your own `data-sources.toml`. The point is a uniform, greppable answer to "what
data do we have, how fresh, how trustworthy, and by what mechanism".

## Schema (the standard table structure)

Every `[[source]]` has these fields:

| field | type | allowed values / format |
|---|---|---|
| `id` | string (req) | stable kebab-case slug; equals the Pumper app `name()` when 1:1 |
| `app` | string | Pumper app crate under `crates/apps/<app>` serving it; `""` if not built yet |
| `market` | string | jurisdiction id in the app's scheme — `us`, `us-ca` (California), `eu`, `au`, `gb`, `cz`, `ca` (Canada)… **`us-ca` = California, `ca` = Canada** |
| `name` | string | human name of the source |
| `url` | string | primary endpoint / portal |
| `category` | enum | **`open-calls`** (opportunities + deadlines) · **`awarded-history`** (who funded whom) · **`registry`** (org identity/eligibility) · **`labor-market`** (job postings + wage benchmarks — a separate domain, for the `kp` app) |
| `engine` | enum | **`http`** · **`browser`** · **`claude`** · **`bulk`** — the import mechanism (`http`/`browser` = web, `claude` = LLM, `bulk` = file download) |
| `access` | enum | `key-free` · `api-key` · `bulk` · `scrape` |
| `cadence` | enum | `one-time` · `on-demand` · `daily` · `weekly` · `monthly` · `quarterly` · `annual` |
| `cron` | string | exact 6-field expr (`sec min hour dom mon dow`) when on the Pumper scheduler; `""` otherwise |
| `status` | enum | `live` (registered & running) · `planned` · `blocked` |
| `confidence` | int 1–5 | how much this source makes downstream outputs valid/trustworthy |
| `dataset` | string | the `Datasets` dataset it writes via `ctx.upsert` (e.g. `opportunities`); `""` if n/a |
| `notes` | string | freeform flags / gotchas |

The three `category` values encode the key insight from the market research: **open-call
feeds are scarce**, awarded-history and registry data are abundant. A market is
"launch-grade" only when it has both a structured `open-calls` feed and a `registry`.

## Current pipeline state (snapshot of `data-sources.toml`)

> Rendered from the TOML — regenerate when you edit it. TOML is the source of truth.

| id | market | category | engine | cadence | status | conf |
|---|---|---|---|---|---|:--:|
| **grants-gov** | us | open-calls | http | daily | **live** | 5 |
| grants-gov-xml | us | open-calls | bulk | daily | planned | 5 |
| irs-eo-bmf | us | registry | bulk | monthly | planned | 5 |
| propublica-nonprofits | us | registry | http | on-demand | planned | 4 |
| usaspending | us | awarded-history | http | monthly | planned | 4 |
| **ca-grants** | us-ca | open-calls | http | daily | **live** | 5 |
| ny-grants | us-ny | open-calls | browser | daily | planned | 4 |
| tx-grants | us-tx | open-calls | browser | daily | planned | 4 |
| il-grants | us-il | open-calls | browser | weekly | planned | 3 |
| oh-grants | us-oh | open-calls | browser | weekly | planned | 3 |
| au-grantconnect | au | open-calls | browser | weekly | planned | 5 |
| au-acnc | au | registry | bulk | monthly | planned | 5 |
| **eu-sedia** | eu | open-calls | http | daily | **live** | 5 |
| uk-360giving | gb | awarded-history | bulk | daily | planned | 5 |
| uk-charity-commission | gb | registry | http (api-key) | on-demand | planned | 5 |
| cz-eufunds | cz | open-calls | http | weekly | planned | 4 |
| cra-charities | ca | registry | bulk | annual | planned | 5 |
| **mpsv-vpm** | cz | labor-market | http | daily | **live** | 5 |
| **mpsv-ispv** | cz | labor-market | http | quarterly | **live** | 5 |
| mpsv-vpm-prirustky | cz | labor-market | http | daily | planned | 5 |
| jooble-cz | cz | labor-market | http (api-key) | on-demand | planned | 4 |
| startupjobs-cz | cz | labor-market | http (api-key) | on-demand | planned | 3 |
| jobs-cz | cz | labor-market | browser | on-demand | blocked | 2 |

**State:** 5 live · 17 planned · 1 blocked. Build order follows the market research
(`grant-writing-nonprofits/docs/data-source-market-map.md`): US federal → CA →
Australia → EU SEDIA → UK, then the browser/LLM scrapers.

## Adding a source

1. Build the app (Path B in `ONBOARDING.md`).
2. Append a `[[source]]` block to `data-sources.toml` (copy the field list from the
   header comment there). Set `status = "live"` and fill `app` + `dataset`.
3. Add its row to the snapshot table above.
4. If it isn't a Pumper app yet (a source you've only researched), still add it with
   `status = "planned"` and `app = ""` — the catalog is the roadmap too.
