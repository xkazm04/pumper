# Declarative extraction & WASM plugins

## Rule sets (`extract.rs`)

A `RuleSet` maps output fields to rules, compiled once and run over document batches across all cores (rayon; simd-json for JSON rules). Rule types:

- `css` — selector → text or `attr`; `all: true` collects every match. `html: true` yields the matched element's serialized HTML instead of its flattened text — pair with a `to_markdown` transform for clean scoped Markdown of e.g. `article.content` (the text path fuses headings/lists/tables; `SKIP` chrome like a nested `<form>` still drops).
- `regex` — capture `group` over the raw document.
- `json` — RFC 6901 JSON Pointer into a JSON body.
- `xpath` — XPath over the HTML (pure-Rust `skyscraper`); attribute nodes yield their value, text nodes content, elements recursive text; `all` supported; invalid expressions fail at compile. Covers parent/ancestor axes CSS can't express.
- `const` — literal value.
- `each` — **repeating container** for list pages: `{"type":"each","selector":".card","fields":{…}}` runs `fields` **scoped to each matched element**, yielding one object per match (`Value::Array` of objects). This is the correct list-page shape — unlike `css` + `all: true`, which returns independent parallel arrays that silently mis-zip the moment one item is missing a field. Inner fields may be `css` (selects descendants of the element), `regex` (over the element's own HTML), `const`, or a nested `each`; `json`/`xpath` inner rules are rejected at compile. Each item's fields stay bound together, so a missing `.price` is a `null` on *its own* item. (The extractor still upserts one dataset record per document; fanning an `each` array out into one record per item is a separable follow-on.)
  - Optional `container` — the **enclosing listing element** (`{"type":"each","selector":".job","container":"#listing"}`). Items are then selected inside it, and an empty result splits into two distinguishable statuses: `container_empty` (the listing was found and held nothing — a job board with no postings this week) versus `empty` (the listing itself is gone — the selector broke). Without `container` both collapse into `empty`, and no later analysis can undo the conflation. A nested `each`'s own `container` resolves inside its item.

**Transforms**: each field takes an optional `transforms` chain applied after the rule (element-wise over arrays): `trim`, `lowercase`, `uppercase`, `to_number`, `to_int`, `to_bool`, `regex_replace {pattern, replacement}`, `split {sep, index?}`, `to_markdown` (HTML fragment → clean Markdown; pair with a `css` rule's `html: true`), `default {value}` (on null). Backward compatible — plain rule JSON still parses (serde-flattened `FieldRule`).

`to_number`/`to_int` parse the **first valid decimal number** in the string, tolerating a leading currency symbol and `,` thousands separators — without concatenating digits across separators: `"1-2"` → `1` (a range, not `-12`), `"$1,234.50"` → `1234.5`, `"3.5%"` → `3.5`, `"2026-07-10"` → `2026`. A sign only binds when it directly precedes the digits (`"-5"` → `-5`).

Exposed via the `extractor` app: fetch a URL (tiered) and apply a params-supplied rule set.

## Extraction quality report

Every field extraction carries a **status** so a broken selector no longer collapses into the same silent `Null` as a genuinely absent field:

- `matched` — the rule ran and produced a non-empty value.
- `empty` — the rule ran but produced nothing (`null`, empty string, or empty array): the field is absent in this document, not mis-configured.
- `container_empty` — an `each` rule with a `container` whose listing matched but held zero items. **Not a miss**: the selector still binds.
- `error` — the rule could not run because the document was the wrong format (a `json` rule over a non-JSON body, or an `xpath` rule over unparseable HTML), with a `detail` string.

Status reflects the **rule match, before transforms** — it answers "did the selector find anything?" independent of downstream coercion. A second, orthogonal **coercion status** answers the rest, per field with a transform chain:

- `coerced` — transforms ran and left a non-empty value.
- `coercion_failed` — the selector matched and the transform chain reduced it to nothing. This is the wrong-element signature: `to_number` on `"Add to cart"` yields null while the field still reports `matched`, so a coercion-failure rate that rises while the match rate stays flat has almost no explanation other than a rebound selector.
- `no_transforms` — nothing to coerce.

API (`extract.rs`):

- `extract_one_with_report(rules, doc) -> (Value, DocReport)`
- `extract_batch_with_report(rules, docs) -> Vec<(Value, DocReport)>`

`DocReport` is `{fields: {field -> {status, detail?}}, coercion: {field -> status}}` (`FieldStatus` is a `status`-tagged enum; `coercion` is omitted when empty). Both are serde-stable for downstream serialization. **Wire note**: `DocReport` was a serde-*transparent* field map before the coercion status existed, so a reader of `POST /extract/preview` now finds the statuses one level down under `report.fields`.

### Input modes

The `extractor` app takes **either** `urls` **or** `source` (exactly one):

- **`urls`** (`"mode": "urls"`) — fetch each URL live (tiered, `strategy` param). Failed/empty fetches are attributed in `failed` and skipped, never upserted as all-null records. Fetch fan-out is **bounded** by the `concurrency` param (default 16, matching `crawl`): the per-host governor serializes same-host requests but caps nothing globally, so without this a large `urls` list would open one socket per URL at once (fd exhaustion). The `plugin` app takes the same `concurrency` param, using order-preserving buffering since it zips results back to keys positionally.
- **`source: {app, dataset, keys?}`** (`"mode": "source"`) — read stored bodies from a dataset instead of re-fetching. Each record must carry `artifact_path` (a body basename) and `job_id` (the origin job); the body is resolved at `data/artifacts/<source.app>/<job_id>/<artifact_path>` (the shared artifacts root, two levels above the extractor's own per-job dir). This is the crawl→extract seam: the crawl already wrote every kept page's body to disk, so re-extracting reads it instead of double-fetching. `keys` precedence: explicit `source.keys` → the firing trigger's `_trigger.keys` (dataset-trigger fan-out) → all live records (not removed, not `gone`), capped at 10,000. Records with no `artifact_path`/`job_id`, or an unreadable file, are counted in `missing` and listed per key in `missing_keys` — never silently null.

### `extractor` result shape

Both modes share the extraction + quality-report path and report aggregate quality:

- urls mode: `requested`, `fetched`, `skipped`, `failed` (skipped URLs).
- source mode: `source {app, dataset}`, `requested`, `loaded`, `missing`, `missing_keys` (`[{key, reason}]`).
- both: `new` / `changed` / `unchanged` (upsert outcome), `fields_matched` / `fields_total` (matched extractions over total attempted), and `worst_fields` — fields that missed at least once, worst first: `{field, misses, errors, miss_rate}` (a miss is an `empty` or `error` status — never `container_empty`; `miss_rate` is misses ÷ docs). Records are tagged `_url` = source URL / record key.
- both: `health` — the extraction-health verdict for this run (`{verdict, diagnosis, score, state, previous_state, statistical_coverage, reasons, drift}`), or `null` when detection is off. urls mode also reports `fetch_ok_rate`. See [resilient-extraction.md](resilient-extraction.md).

**Artifact retention**: source mode depends on the origin job's bodies still being on disk. Crawl bodies live in per-job dirs (`data/artifacts/<app>/<job_id>/`). Retention is **off by default** (`[storage] artifact_retention_days = 0`) — bodies persist indefinitely unless an operator opts in. When it is on, the janitor reclaims bodies older than the window *except* any body a **replayable** revision points at (`artifact_sha` + `rules_hash` both stamped), which is pinned regardless of age, and except VCR cassettes unless `artifact_retention_include_cassettes` is set. A body reclaimed (or manually deleted) surfaces its key in `missing_keys` on the next extract — never as a silent null. `GET /retention/preview` reports reclaimable bytes per app without deleting anything. See [datasets.md § Retention](datasets.md).

## RuleSet preview (`POST /extract/preview`)

Test a `RuleSet` against one document **without enqueuing a job** — the fast feedback loop for authoring selectors, so a typo is caught before a job fetches everything. Body: `{rules, html}` **or** `{rules, url}` (exactly one of `html`/`url`; both or neither → `400 bad_request`).

- `rules` — a bare `{field: rule}` map (the same shape apps take), e.g. `{"title": {"type":"css","selector":"h1"}}`. Rules are compiled **field-by-field** (each as a single-field `RuleSet`), so **every** bad field is reported at once, not just the first. On any failure the response is `400 bad_request` with a per-field `fields: [{field, error}]` list covering deserialize errors (unknown rule `type`, missing keys) and compile errors (bad CSS selector / regex / XPath). A non-object `rules` is `400`.
- `url` mode fetches through the shared **HTTP tier only** (`FetchStrategy::Http` — no browser render, and never the paid Claude tier), under a modest budget: a 15s fetch timeout (exceeded → `400`) and an 8 MiB body cap (over → `413 too_large`). A non-`http(s)` url or a fetch failure is `400`.

On success (`200`): `{values, report, fields_matched, fields_total}` — the extracted values plus the report (`report.fields`: each field `matched`|`empty`|`container_empty`|`error`; `report.coercion`: `coerced`|`coercion_failed`|`no_transforms`, see above), so a selector that silently matches nothing — or matches the wrong thing — is visible immediately. `fields_matched`/`fields_total` are the matched-over-attempted counts.

## HTML → Markdown

`pumper_core::html_to_markdown` — boilerplate-skipping converter used by the fetcher (`to_markdown`), `readable`/`watch` apps, and SEDIA clean-text enrichment.

`<table>` renders as a **GitHub pipe table**. The first row is the header: `<th>` cells become the headers, and a `<th>`-less table promotes its first `<tr>` to the header. `<thead>`/`<tbody>`/`<tfoot>` wrappers are traversed; ragged rows are padded to a rectangular grid; cells with nested block content degrade to inline text (whitespace collapsed, `|` escaped); a nested table's text is flattened into its enclosing cell.

## WASM plugin sandbox (`engine-wasm`, `plugin` app)

Hot-swappable `.wasm` extractor modules loaded from the plugins dir (`plugins-src/` holds sources), executed under wasmtime with **fuel + memory limits**. `GET /plugins` lists, `POST /plugins/reload` rescans. `max_memory_mb` bounds **one** store, so the host also enforces a **global concurrency cap** (`[plugins] max_concurrent`, `0` = one per CPU core) via a semaphore acquired before each run — otherwise a wide fan-out admits `max_memory_mb × concurrent_calls` of aggregate wasm memory and can saturate tokio's blocking pool.

**Params envelope + manifest.** A plugin can be reused across jobs with different config instead of recompiling a module per variation. The `plugin` app forwards a `plugin_params` object; a params-aware module exports `extract_v2(ptr, len) -> u64` whose input is a `{"doc": .., "params": ..}` JSON envelope (vs the legacy `extract`, which receives just the document). The host prefers `extract_v2` and falls back to `extract` when it's absent, so **plugins built before the envelope keep working unchanged**. A module may also export `describe() -> u64` returning a self-describing manifest (`{name, version, description, params_schema, output_schema}`), read once at load; `GET /plugins` then returns real metadata per plugin (name-only when `describe` is absent). `plugins-src/title-extractor` is the reference implementation of both (`params.tag` extracts an arbitrary tag into `value`). The `plugin` app runs a named plugin over documents in **either** input mode (like `extractor`, exactly one): `urls` (fetch each live, tiered `strategy` incl. `auto_with_research`) or `source: {app, dataset, keys?}` — run over already-crawled stored bodies with no re-fetch, keys defaulting to the firing trigger's `_trigger.keys` then all live records. The crawl→plugin seam shares `AppContext::read_source_artifact` (one hardened path-traversal guard) with the extractor. Source-mode result: `source {app, dataset}`, `requested`, `loaded`, `missing`, `missing_keys`.

## Extraction health

Rule sets rot when sites change, and the quality report above only makes one run's misses visible — it cannot tell a broken selector from a genuinely absent field *in aggregate*. [resilient-extraction.md](resilient-extraction.md) covers the per-source degradation detector built on it: per-field sketches, a markup-shape fingerprint (`dom_simhash`) next to the existing text one, mined invariants, and a health ladder that stops a degrading source from tombstoning its dataset or pushing downstream.

When health detection is on, the extractor runs `resilience::extract_and_fingerprint_batch` instead of `extract_batch_with_report`: same rayon fan-out, same records and reports, but the DOM each document is parsed into is **shared** with fingerprinting rather than rebuilt for it. Measured 1.8× faster and 38% lower peak RSS on a 2000-document / 110 MB batch, with byte-identical fingerprints — see [resilient-extraction.md §2.8](resilient-extraction.md#28-what-it-costs-at-this-scale). Rule sets with no CSS rule are unaffected in parse count: they never parsed HTML for extraction and still pay exactly one parse, the fingerprint's.

## Known gaps

- Plugin fuel/memory telemetry isn't surfaced per-run (backlog). No schema-less/LLM-assisted extraction yet (backlog moonshots: NL→RuleSet, self-healing selectors). Only the `extractor` app reports runs to the health detector; `plugin` and the hardcoded-Rust apps do not yet.
