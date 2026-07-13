# Declarative extraction & WASM plugins

## Rule sets (`extract.rs`)

A `RuleSet` maps output fields to rules, compiled once and run over document batches across all cores (rayon; simd-json for JSON rules). Rule types:

- `css` — selector → text or `attr`; `all: true` collects every match.
- `regex` — capture `group` over the raw document.
- `json` — RFC 6901 JSON Pointer into a JSON body.
- `xpath` — XPath over the HTML (pure-Rust `skyscraper`); attribute nodes yield their value, text nodes content, elements recursive text; `all` supported; invalid expressions fail at compile. Covers parent/ancestor axes CSS can't express.
- `const` — literal value.

**Transforms**: each field takes an optional `transforms` chain applied after the rule (element-wise over arrays): `trim`, `lowercase`, `uppercase`, `to_number`, `to_int`, `to_bool`, `regex_replace {pattern, replacement}`, `split {sep, index?}`, `default {value}` (on null). Backward compatible — plain rule JSON still parses (serde-flattened `FieldRule`).

`to_number`/`to_int` parse the **first valid decimal number** in the string, tolerating a leading currency symbol and `,` thousands separators — without concatenating digits across separators: `"1-2"` → `1` (a range, not `-12`), `"$1,234.50"` → `1234.5`, `"3.5%"` → `3.5`, `"2026-07-10"` → `2026`. A sign only binds when it directly precedes the digits (`"-5"` → `-5`).

Exposed via the `extractor` app: fetch a URL (tiered) and apply a params-supplied rule set.

## Extraction quality report

Every field extraction carries a **status** so a broken selector no longer collapses into the same silent `Null` as a genuinely absent field:

- `matched` — the rule ran and produced a non-empty value.
- `empty` — the rule ran but produced nothing (`null`, empty string, or empty array): the field is absent in this document, not mis-configured.
- `error` — the rule could not run because the document was the wrong format (a `json` rule over a non-JSON body, or an `xpath` rule over unparseable HTML), with a `detail` string.

Status reflects the **rule match, before transforms** — it answers "did the selector find anything?" independent of downstream coercion. API (`extract.rs`):

- `extract_one_with_report(rules, doc) -> (Value, DocReport)`
- `extract_batch_with_report(rules, docs) -> Vec<(Value, DocReport)>`

`DocReport` is a serde-transparent map `{ field -> {status, detail?} }` (`FieldStatus` is a `status`-tagged enum). Both are serde-stable for downstream serialization.

### Input modes

The `extractor` app takes **either** `urls` **or** `source` (exactly one):

- **`urls`** (`"mode": "urls"`) — fetch each URL live (tiered, `strategy` param). Failed/empty fetches are attributed in `failed` and skipped, never upserted as all-null records.
- **`source: {app, dataset, keys?}`** (`"mode": "source"`) — read stored bodies from a dataset instead of re-fetching. Each record must carry `artifact_path` (a body basename) and `job_id` (the origin job); the body is resolved at `data/artifacts/<source.app>/<job_id>/<artifact_path>` (the shared artifacts root, two levels above the extractor's own per-job dir). This is the crawl→extract seam: the crawl already wrote every kept page's body to disk, so re-extracting reads it instead of double-fetching. `keys` precedence: explicit `source.keys` → the firing trigger's `_trigger.keys` (dataset-trigger fan-out) → all live records (not removed, not `gone`), capped at 10,000. Records with no `artifact_path`/`job_id`, or an unreadable file, are counted in `missing` and listed per key in `missing_keys` — never silently null.

### `extractor` result shape

Both modes share the extraction + quality-report path and report aggregate quality:

- urls mode: `requested`, `fetched`, `skipped`, `failed` (skipped URLs).
- source mode: `source {app, dataset}`, `requested`, `loaded`, `missing`, `missing_keys` (`[{key, reason}]`).
- both: `new` / `changed` / `unchanged` (upsert outcome), `fields_matched` / `fields_total` (matched extractions over total attempted), and `worst_fields` — fields that missed at least once, worst first: `{field, misses, errors, miss_rate}` (a miss is an `empty` or `error` status; `miss_rate` is misses ÷ docs). Records are tagged `_url` = source URL / record key.

**Artifact-retention caveat**: source mode depends on the origin job's bodies still being on disk. Crawl bodies live in per-job dirs (`data/artifacts/<app>/<job_id>/`) and there is **no retention/GC policy** — bodies persist until manually removed, and once removed those keys land in `missing_keys` on the next extract.

## RuleSet preview (`POST /extract/preview`)

Test a `RuleSet` against one document **without enqueuing a job** — the fast feedback loop for authoring selectors, so a typo is caught before a job fetches everything. Body: `{rules, html}` **or** `{rules, url}` (exactly one of `html`/`url`; both or neither → `400 bad_request`).

- `rules` — a bare `{field: rule}` map (the same shape apps take), e.g. `{"title": {"type":"css","selector":"h1"}}`. Rules are compiled **field-by-field** (each as a single-field `RuleSet`), so **every** bad field is reported at once, not just the first. On any failure the response is `400 bad_request` with a per-field `fields: [{field, error}]` list covering deserialize errors (unknown rule `type`, missing keys) and compile errors (bad CSS selector / regex / XPath). A non-object `rules` is `400`.
- `url` mode fetches through the shared **HTTP tier only** (`FetchStrategy::Http` — no browser render, and never the paid Claude tier), under a modest budget: a 15s fetch timeout (exceeded → `400`) and an 8 MiB body cap (over → `413 too_large`). A non-`http(s)` url or a fetch failure is `400`.

On success (`200`): `{values, report, fields_matched, fields_total}` — the extracted values plus the per-field match report (`DocReport`: each field `matched`|`empty`|`error`, see above), so a selector that silently matches nothing is visible immediately. `fields_matched`/`fields_total` are the matched-over-attempted counts.

## HTML → Markdown

`pumper_core::html_to_markdown` — boilerplate-skipping converter used by the fetcher (`to_markdown`), `readable`/`watch` apps, and SEDIA clean-text enrichment.

`<table>` renders as a **GitHub pipe table**. The first row is the header: `<th>` cells become the headers, and a `<th>`-less table promotes its first `<tr>` to the header. `<thead>`/`<tbody>`/`<tfoot>` wrappers are traversed; ragged rows are padded to a rectangular grid; cells with nested block content degrade to inline text (whitespace collapsed, `|` escaped); a nested table's text is flattened into its enclosing cell.

## WASM plugin sandbox (`engine-wasm`, `plugin` app)

Hot-swappable `.wasm` extractor modules loaded from the plugins dir (`plugins-src/` holds sources), executed under wasmtime with **fuel + memory limits**. `GET /plugins` lists, `POST /plugins/reload` rescans. The `plugin` app runs a named plugin over a fetched page.

## Known gaps

- Plugin fuel/memory telemetry isn't surfaced per-run (backlog). No schema-less/LLM-assisted extraction yet (backlog moonshots: NL→RuleSet, self-healing selectors).
