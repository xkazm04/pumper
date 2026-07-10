# Declarative extraction & WASM plugins

## Rule sets (`extract.rs`)

A `RuleSet` maps output fields to rules, compiled once and run over document batches across all cores (rayon; simd-json for JSON rules). Rule types:

- `css` — selector → text or `attr`; `all: true` collects every match.
- `regex` — capture `group` over the raw document.
- `json` — RFC 6901 JSON Pointer into a JSON body.
- `xpath` — XPath over the HTML (pure-Rust `skyscraper`); attribute nodes yield their value, text nodes content, elements recursive text; `all` supported; invalid expressions fail at compile. Covers parent/ancestor axes CSS can't express.
- `const` — literal value.

**Transforms**: each field takes an optional `transforms` chain applied after the rule (element-wise over arrays): `trim`, `lowercase`, `uppercase`, `to_number` (currency/thousands tolerant), `to_int`, `to_bool`, `regex_replace {pattern, replacement}`, `split {sep, index?}`, `default {value}` (on null). Backward compatible — plain rule JSON still parses (serde-flattened `FieldRule`).

Exposed via the `extractor` app: fetch a URL (tiered) and apply a params-supplied rule set.

## HTML → Markdown

`pumper_core::html_to_markdown` — boilerplate-skipping converter used by the fetcher (`to_markdown`), `readable`/`watch` apps, and SEDIA clean-text enrichment.

## WASM plugin sandbox (`engine-wasm`, `plugin` app)

Hot-swappable `.wasm` extractor modules loaded from the plugins dir (`plugins-src/` holds sources), executed under wasmtime with **fuel + memory limits**. `GET /plugins` lists, `POST /plugins/reload` rescans. The `plugin` app runs a named plugin over a fetched page.

## Known gaps

- Plugin fuel/memory telemetry isn't surfaced per-run (backlog). No schema-less/LLM-assisted extraction yet (backlog moonshots: NL→RuleSet, self-healing selectors).
