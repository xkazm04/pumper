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

### `extractor` result shape

The `extractor` app skips fetch failures instead of upserting all-null records, and reports aggregate quality:

- `fetched` / `skipped` — docs that fetched vs. were dropped; `failed` lists the skipped URLs.
- `fields_matched` / `fields_total` — matched extractions over total attempted across all kept docs.
- `worst_fields` — fields that missed at least once, worst first: `{field, misses, errors, miss_rate}` (a miss is an `empty` or `error` status; `miss_rate` is misses ÷ docs).
- `mode` — `"urls"` (see the crawl→extract seam below for `"source"`).

## HTML → Markdown

`pumper_core::html_to_markdown` — boilerplate-skipping converter used by the fetcher (`to_markdown`), `readable`/`watch` apps, and SEDIA clean-text enrichment.

`<table>` renders as a **GitHub pipe table**. The first row is the header: `<th>` cells become the headers, and a `<th>`-less table promotes its first `<tr>` to the header. `<thead>`/`<tbody>`/`<tfoot>` wrappers are traversed; ragged rows are padded to a rectangular grid; cells with nested block content degrade to inline text (whitespace collapsed, `|` escaped); a nested table's text is flattened into its enclosing cell.

## WASM plugin sandbox (`engine-wasm`, `plugin` app)

Hot-swappable `.wasm` extractor modules loaded from the plugins dir (`plugins-src/` holds sources), executed under wasmtime with **fuel + memory limits**. `GET /plugins` lists, `POST /plugins/reload` rescans. The `plugin` app runs a named plugin over a fetched page.

## Known gaps

- Plugin fuel/memory telemetry isn't surfaced per-run (backlog). No schema-less/LLM-assisted extraction yet (backlog moonshots: NL→RuleSet, self-healing selectors).
