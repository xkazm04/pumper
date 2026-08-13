# Declarative extraction & WASM plugins

## Rule sets (`extract.rs`)

A `RuleSet` maps output fields to rules, compiled once and run over document batches across all cores (rayon; simd-json for JSON rules). Rule types:

- `css` — selector → text or `attr`; `all: true` collects every match. `html: true` yields the matched element's serialized HTML instead of its flattened text — pair with a `to_markdown` transform for clean scoped Markdown of e.g. `article.content` (the text path fuses headings/lists/tables; `SKIP` chrome like a nested `<form>` still drops).
- `regex` — capture `group` over the raw document.
- `json` — RFC 6901 JSON Pointer into a JSON body.
- `xpath` — XPath over the HTML (pure-Rust `skyscraper`); attribute nodes yield their value, text nodes content, elements recursive text; `all` supported; invalid expressions fail at compile. Covers parent/ancestor axes CSS can't express. **Non-node results keep their own JSON type**: `count(//li)` → `3` (a number), `string(//h1)` → `"Hi"`, `not(//x)` → `true`, and a QName its lexical form. An expression that *parses* but cannot **evaluate** (unsupported function, undefined variable, division by zero) reports `error` with the evaluation message — never `empty`, which is a claim about the document a rule that never ran has no right to make.
- `const` — literal value.
- `each` — **repeating container** for list pages: `{"type":"each","selector":".card","fields":{…}}` runs `fields` **scoped to each matched element**, yielding one object per match (`Value::Array` of objects). This is the correct list-page shape — unlike `css` + `all: true`, which returns independent parallel arrays that silently mis-zip the moment one item is missing a field. Inner fields may be `css` (selects descendants of the element), `regex` (over the element's own HTML), `const`, or a nested `each`; `json`/`xpath` inner rules are rejected at compile. Each item's fields stay bound together, so a missing `.price` is a `null` on *its own* item. (The extractor still upserts one dataset record per document; fanning an `each` array out into one record per item is a separable follow-on.)
  - Optional `container` — the **enclosing listing element** (`{"type":"each","selector":".job","container":"#listing"}`). Items are then selected inside it, and an empty result splits into two distinguishable statuses: `container_empty` (the listing was found and held nothing — a job board with no postings this week) versus `empty` (the listing itself is gone — the selector broke). Without `container` both collapse into `empty`, and no later analysis can undo the conflation. A nested `each`'s own `container` resolves inside its item.

**Transforms**: each field takes an optional `transforms` chain applied after the rule (element-wise over arrays): `trim`, `lowercase`, `uppercase`, `to_number`, `to_int`, `to_bool`, `regex_replace {pattern, replacement}` (`$1`-style capture references), `split {sep, index?}`, `to_markdown` (HTML fragment → clean Markdown; pair with a `css` rule's `html: true`), `url_absolute`, `default {value}`. Backward compatible — plain rule JSON still parses (serde-flattened `FieldRule`).

### `url_absolute` — links that work outside the page they came from

`{"type":"css","selector":"a","attr":"href","transforms":[{"op":"url_absolute"}]}` resolves an extracted URL against **the document's own URL** (RFC 3986, via the `url` crate — never string concatenation, which gets `../`, query-only and scheme-relative references wrong):

| input on `https://shop.test/cat/page` | output |
| --- | --- |
| `/item/1` | `https://shop.test/item/1` |
| `../item/2` | `https://shop.test/item/2` |
| `//cdn.shop.test/x` | `https://cdn.shop.test/x` (protocol-relative takes the page's scheme) |
| `https://other.test/z`, `mailto:a@b` | unchanged (already absolute) |
| `?page=1` / `#sec` | resolved against the page |

**Without a base URL it is a reported no-op, never a guess.** A value that cannot be resolved — no document URL, a document URL that will not parse, or a join error — comes back **unchanged**, never null: a relative URL is still the truth the page contained. A *blank* value stays blank (joining `""` against a base yields the base itself, which would turn "this field found nothing" into "this field found the page you were already on"). When the rule set asks for `url_absolute` and the extraction had no base, `report.base_url_missing: true` says so — the alternative is a `url` column that is relative on some runs and absolute on others with nothing marking which.

Where the base comes from: the `extractor` app passes each document's own URL (the fetched URL in `urls` mode, the record key in `source`/backfill modes, the original URL for a Wayback capture), replay-CI passes each stored body's URL, and `POST /extract/preview` uses `base_url` or the `url` it fetched. A source dataset whose keys are ids rather than links has no base, and the run reports `base_url_missing` (a count of such documents). Two known limits: the base is the **requested** URL, not the post-redirect one (`FetchOutcome` carries `url = the request`; the HTTP engine's `final_url` is not plumbed through the tiered fetcher), and an archived Wayback body whose hrefs were rewritten by the archive resolves them against the original URL, matching its `_url` provenance.

`induce` emits `url_absolute` automatically on URL-bearing attribute slots (`href`/`src`/`poster`), so an induced rule set hands back usable links rather than a `_url` field the user has to fix by hand.

`default` fires on a **blank** result — `null`, a whitespace-only string, or an empty array — which is the same predicate that decides the `empty` status below, so the two cannot disagree. (It used to fire only on `null`: a selector that matched an empty `<span>` reported `empty` *and* kept the `""`, so the declared default silently never applied.) Falsey values are data, not absence: `0` and `false` are never replaced.

`to_number`/`to_int` parse the **first valid decimal number** in the string, tolerating a leading currency symbol and `,` thousands separators — without concatenating digits across separators: `"1-2"` → `1` (a range, not `-12`), `"$1,234.50"` → `1234.5`, `"3.5%"` → `3.5`, `"2026-07-10"` → `2026`. A sign only binds when it directly precedes the digits (`"-5"` → `-5`). A value the target type cannot hold is `null`, never a clamped stand-in: a 400-digit string overflows `f64`, so **both** yield `null`; a finite value outside `i64` (`1e20`) is `null` for `to_int` while `to_number` keeps the double it really parsed.

Exposed via the `extractor` app: fetch a URL (tiered) and apply a params-supplied rule set.

## Extraction quality report

Every field extraction carries a **status** so a broken selector no longer collapses into the same silent `Null` as a genuinely absent field:

- `matched` — the rule ran and produced a non-empty value.
- `empty` — the rule ran but produced nothing (`null`, empty string, or empty array): the field is absent in this document, not mis-configured.
- `container_empty` — an `each` rule with a `container` whose listing matched but held zero items. **Not a miss**: the selector still binds.
- `error` — the rule could not run, with a `detail` string: the document was the wrong format (a `json` rule over a non-JSON body, an `xpath` rule over unparseable HTML), or the rule itself failed at evaluation (an `xpath` that parsed but hit an unsupported function / undefined variable).

Status reflects the **rule match, before transforms** — it answers "did the selector find anything?" independent of downstream coercion. A second, orthogonal **coercion status** answers the rest, per field with a transform chain:

- `coerced` — transforms ran and left a non-empty value.
- `coercion_failed` — the selector matched and the transform chain reduced it to nothing. This is the wrong-element signature: `to_number` on `"Add to cart"` yields null while the field still reports `matched`, so a coercion-failure rate that rises while the match rate stays flat has almost no explanation other than a rebound selector.
- `no_transforms` — nothing to coerce.

### Inside a listing: per-inner-field counts

An `each` rule reports **one** status for the whole array, and that status is `matched` as long as the container matched — the array is full of objects, they just all carry `price: null`. A listing whose inner `price` selector dies is therefore invisible at the document scope. `report.each` is the missing signal: **per inner field, aggregated across the listing's items**.

Keys are the dotted path from the top-level field: `products.price`, and `products.variants.sku` for a nested `each`. Each value is `{items, matched, empty, container_empty, error}` — **counts, never per-item lists**, so a report over a 5000-row listing is exactly the size of one over a 5-row listing (one entry per inner field of the rule set, whatever the document does).

- `items` is the denominator: how many listing items the inner rule was attempted on. A container that matched but held nothing gives every inner field an honest `items: 0` row — present (so the field is discoverable) and *not* a failure.
- A **hit** is `matched + container_empty`, exactly mirroring `container_empty` not being a miss at the document scope; a **miss** is `empty + error`, exactly mirroring `FieldStatus::is_miss`.
- **Dead vs sparse**: `items > 0 && hits == 0` means the selector bound on *no* item — listing rot. Anything in between is a sparse field (some cards carry a badge, some don't). Collapsing the two is the failure the array-level `matched` forces.

The listing's own `report.fields` entry is unchanged (still `matched`/`empty`/`container_empty`), and `report.each` is omitted entirely from the JSON for rule sets with no `each` rule — this is additive on the existing report shape.

API (`extract.rs`):

- `extract_one_with_report(rules, doc) -> (Value, DocReport)`
- `extract_batch_with_report(rules, docs) -> Vec<(Value, DocReport)>`
- `extract_one_with_report_at(rules, doc, base: Option<&str>)` / `extract_batch_with_report_at(rules, docs, bases: &[Option<String>])` — the same, told the document's own URL for `url_absolute`. `bases` is positional and may be short (or `&[]`), so a mixed batch degrades per document rather than all-or-nothing. `CompiledRuleSet::needs_doc_url()` answers "does this rule set need a base at all", before the fan-out.

`DocReport` is `{fields: {field -> {status, detail?}}, coercion: {field -> status}, each: {dotted.path -> {items, matched, empty, container_empty, error}}, base_url_missing?: true}` (`FieldStatus` is a `status`-tagged enum; `coercion`/`each` are omitted when empty and `base_url_missing` when false). All are serde-stable for downstream serialization. **Wire note**: `DocReport` was a serde-*transparent* field map before the coercion status existed, so a reader of `POST /extract/preview` now finds the statuses one level down under `report.fields`.

**Who reads `each` today**: the `extractor` app's `worst_fields` roll-up and the replay-CI `inner_fields` rows (both below). The resilience per-field sketches, the `provisioner` accept gate and the DataHub/reliability *scoring* still key off `report.fields` only, so listing rot shows up in a run's result and in the host reliability record's echoed `worst_fields` — but does not yet move a health score or a provisioning verdict.

### Modes — exactly one per job, enforced

The `extractor` app has **four** modes, each declared by its own params root:

| mode | roots | writes records? |
| --- | --- | --- |
| urls | `rules` + `urls` | yes |
| source | `rules` + `source` | yes |
| replay | `replay` | no (report + artifact) |
| induce | `induce` | no (report + artifact) |

**Any other combination is refused**, at two layers that must agree:

- the app's params schema, so the shared enqueue check (`POST /apps/extractor/jobs`, `POST /schedules`, the trigger fire paths, MCP `enqueue_job`) answers **422** before a job exists;
- `resolve_run_mode` inside the app, for anything that reaches `run()` without a door.

The error names **every** conflicting root: `conflicting extractor modes: replay + rules + urls — a job runs exactly ONE mode`.

**This is a behavior change for callers that relied on the old silent precedence.** The roots used to be tested in a fixed order — `replay` > `induce` > `rules`, and inside write mode `source` > `urls` — and the first match ran while the rest were ignored with a `200`. A job submitted as `{rules, urls, replay}` ran a **read-only replay** and wrote nothing, and no field of the result said so. Such a job is now refused instead of quietly doing something else. A `rules` with neither `urls` nor `source` (and an input list with no `rules`) is likewise a 422 at the door rather than a burnt job attempt.

A JSON `null` counts as absent, so a params template that spells "not this run" as `"replay": null` still enqueues.

- **`urls`** (`"mode": "urls"`) — fetch each URL live (tiered, `strategy` param). Failed/empty fetches are attributed in `failed` and skipped, never upserted as all-null records. Fetch fan-out is **bounded** by the `concurrency` param (default 16, matching `crawl`, **ceiling 64**): the per-host governor serializes same-host requests but caps nothing globally, so without this a large `urls` list would open one socket per URL at once (fd exhaustion). The ceiling is declared once and enforced twice — the schema's `maximum` refuses `concurrency: 65` at the door, and the code clamps it for any caller that reaches the app another way, so the two layers cannot disagree about what the bound is. The `plugin` app takes the same `concurrency` param under the same two-layer rule (it uses order-preserving buffering, since it zips results back to keys positionally). *That was a documented claim before it was a true one*: the plugin app declared `maximum: 64` in its schema while `parse_concurrency` clamped only the lower end, so a caller reaching the app past the enqueue door — a trigger fan-out, an embedder — got exactly the fd exhaustion this sentence promised was impossible. Both apps now clamp both ends, each with a test asserting the upper one.
  Every URL in the fan-out is fetched through the **metered chokepoint** (`AppContext::fetch`), never the raw tiered fetcher. That is what makes a `urls`-mode run of `extractor`/`plugin`: (a) **metered** — one cost event per URL carrying the winning engine and the URL, visible on `GET /jobs/{id}/costs` and `/economics`; (b) **budget-clamped** — with `strategy: "auto_with_research"` and no `budget_usd` headroom left (including the `$0` a DataHub `cost:pause` forces), the paid Claude tier is *skipped*, not failed: the fetch soft-downgrades to the free tiers and says so in the cost event's `detail` and the outcome's `escalations`; (c) **tier-learned** — per-host HTTP wins/losses train the router; and (d) **VCR-faithful** — a job enqueued with `record: true` writes one cassette entry per URL, and `replay_of` serves those bytes with no network and `$0` spend. `crates/core/tests/fetch_chokepoint.rs` pins the raw-engine call sites so a new bypass fails CI.
- **`source: {app, dataset, keys?, limit?}`** (`"mode": "source"`) — read stored bodies from a dataset instead of re-fetching. Each record must carry `artifact_path` (a body basename) and `job_id` (the origin job); the body is resolved at `data/artifacts/<source.app>/<job_id>/<artifact_path>` (the shared artifacts root, two levels above the extractor's own per-job dir). This is the crawl→extract seam: the crawl already wrote every kept page's body to disk, so re-extracting reads it instead of double-fetching. `keys` precedence: explicit `source.keys` → the firing trigger's `_trigger.keys` (dataset-trigger fan-out) → all live records (not removed, not `gone`), capped by `source.limit` (default and ceiling **10,000**, most-recently-updated first). Records with no `artifact_path`/`job_id`, or an unreadable file, are counted in `missing` and listed per key in `missing_keys` — never silently null (that echo is bounded at 100 entries; `missing` keeps the full count).

  **When the cap bites, the run says so**: `truncated: true` alongside `limit`. Judged on the page the store returned, *before* the removed/gone filter — how many rows survived that filter says nothing about whether more exist past the cap. Previously a 12,000-record dataset extracted 10,000 and reported `requested: 10000`, a number indistinguishable from a dataset that really holds 10,000 rows. Resume the rest by narrowing with `keys`; `truncated` is always `false` when the caller named the key set, because no cap applied to it.

### `extractor` result shape

Both modes share the extraction + quality-report path and report aggregate quality:

- urls mode: `requested`, `fetched`, `skipped`, `failed` (skipped URLs).
- source mode: `source {app, dataset}`, `requested`, `limit`, `truncated`, `loaded`, `missing`, `missing_keys` (`[{key, reason}]`).
- every write mode: `dataset` — **the dataset the records actually landed in**. Normally the requested name; the shadow `<dataset>@q` when `[resilience] enforce = true` diverted a quarantined source (see [resilient-extraction.md](resilient-extraction.md)). Without it a diverted run looked identical to a normal one and the reader went looking in the wrong table. `null` on a backfill that wrote no batch.
- every write mode: `rules_hash` — the content-addressed pin registered for this run's rule set and stamped on every revision it wrote, or **`null` plus `rules_registration_error`** when the registry write failed. Registration is best-effort by design (provenance is additive and must never fail a working scrape), but unstamped revisions are permanently non-replayable, and that used to be visible only as a log line.
- both: `new` / `changed` / `unchanged` (upsert outcome), `fields_matched` / `fields_total` (matched extractions over total attempted), and `worst_fields` — fields that missed at least once, worst first. Two row scopes:
  - **document scope** (top-level fields): `{field, misses, errors, miss_rate}` — a miss is an `empty` or `error` status, never `container_empty`; `miss_rate` is misses ÷ docs.
  - **item scope** (inner fields of an `each` listing, keyed `products.price`): the same four keys plus `{scope: "item", items, hits, dead}`. Here `miss_rate` is misses ÷ listing **items**, which is why `scope` is on the row; `dead: true` means the selector bound on no item at all (listing rot) as opposed to a sparse field.

  `fields_matched`/`fields_total` stay document-scoped on purpose: they are the run's rule-level match rate, and folding item counts in would let one wide listing outvote every other field in the rule set. Records are tagged `_url` = source URL / record key.
- both: `base_url_missing` — how many documents a `url_absolute` rule set ran over with no document URL to resolve against (`0` for every other rule set). Those documents' links stayed exactly as the page wrote them; the count is what keeps that from reading as a clean run. Also carried in the `backfill` checkpoint, so a resumed scan keeps a cumulative figure.
- both: `health` — the extraction-health verdict for this run (`{verdict, diagnosis, score, state, previous_state, statistical_coverage, reasons, drift}`), or `null` when detection is off. urls mode also reports `fetch_ok_rate`. See [resilient-extraction.md](resilient-extraction.md).

**Backfill** (`source.backfill: true`, `"mode": "backfill"`) writes per batch and reports the same quality surface, pooled across every batch of the — possibly resumed — scan: `fields_matched` / `fields_total` / `base_url_missing` as before, and now `worst_fields` with **cumulative denominators** (the miss breakdown of those same counters; reporting one cumulatively while the other covered a single batch would be worse than reporting neither) plus `health`. Verdicts are deliberately *not* pooled: a verdict is the source's **state**, and an average of four states is not a state — so `health` is the verdict of the last batch that produced one, i.e. where the source ended up. Every batch's verdict is recorded in the health store regardless. The pooled breakdown rides in the backfill checkpoint, so a reaped-and-resumed scan's `worst_fields` covers the same span its counters do. Backfill returns no `records` echo (it never did — the records are in the dataset).

The manifest's `output_shape` names these keys mode by mode and is pinned by a test against a real run, because it used to promise `{extracted, errors, removed?}` — three keys no mode has ever emitted.

#### The `records` echo is a sample, and `index_datasets` is why that is safe

`urls`/`source`/`archive` return `records` — **a bounded prefix**, not the corpus. `records_echo` sets the bound (default **100**, ceiling **1000**, `0` = counts only); when it bites, the result carries `records_truncated: true` and `records_total` (the honest count of records written). `backfill` returns no echo at all, as it never did.

Why: the echo used to be *every* record. A 10,000-record run wrote a multi-MB JSON blob into the `jobs` row, and that blob then rode the `job.succeeded` webhook, the SSE event and `GET /jobs/{id}/receipt` — permanently, restating data already durably in the dataset. The write path also deep-cloned every record purely to build it; the clone is now paid only for the records actually echoed.

Bounding it is safe because search coverage moves to the mature path. Every write mode's result declares:

```json
"index_datasets": [{ "app": "extractor", "dataset": "<the dataset actually written>" }]
```

The worker indexes those datasets **delta-driven from the change feed** (`dataset_search_docs`): one document per record the run touched, with stable `<app>:<dataset>:<key>` ids that re-index in place and honour removals — strictly better than the old id-per-job-result-element documents. The declaration is **withheld** when the source's own extraction-health verdict says its rows do not belong in the index (`degraded`/`quarantined`), because the worker's gate reads the health of the *spec's* pair and a diverted `<dataset>@q` is a pair nothing ever judges — the same producer-side gate `grants-common` uses.

`records` remains the quick sample a human or agent wants when reading a job result; the dataset (`GET /datasets/extractor/<dataset>`) is the record of truth.

### Replay-CI (`replay` param)

`{"replay": {"rules": …, "baseline_rules"?: …, "against": {app, dataset, url_pattern?, versions, max_pages}, "bisect_field"?: …}}` runs a **candidate** rule set over stored bodies and diffs it against a baseline — strictly read-only (job result + a `replay-report.json` artifact, never a dataset record). The report carries `fields` (per top-level field: `match_rate`, `baseline_match_rate`, `delta`, and bounded `added`/`lost`/`changed` value samples), `regressions`/`regressed_urls` per URL, and `bisect` (the adjacent observation pair where a field's match flipped).

`inner_fields` is the item-scoped companion, one row per inner field of an `each` listing: `{field: "products.price", scope: "item", items, match_rate, dead, baseline_items?, baseline_match_rate?, delta?}`, worst regression first. Without it a rule edit that kills an inner selector diffs to `delta: 0.0` — both sides still report one `matched` for the whole array. These rows carry **no value samples**: the values live inside the array items and the report aggregates counts rather than echoing per-item values (that bound is what keeps a wide listing's report small). A field that needs value-level diffing is diffed by lifting it out of the listing.

**Artifact retention**: source mode depends on the origin job's bodies still being on disk. Crawl bodies live in per-job dirs (`data/artifacts/<app>/<job_id>/`). Retention is **off by default** (`[storage] artifact_retention_days = 0`) — bodies persist indefinitely unless an operator opts in. When it is on, the janitor reclaims bodies older than the window *except* any body a **replayable** revision points at (`artifact_sha` + `rules_hash` both stamped), which is pinned regardless of age, and except VCR cassettes unless `artifact_retention_include_cassettes` is set. A body reclaimed (or manually deleted) surfaces its key in `missing_keys` on the next extract — never as a silent null. `GET /retention/preview` reports reclaimable bytes per app without deleting anything. See [datasets.md § Retention](datasets.md).

## RuleSet preview (`POST /extract/preview`)

Test a `RuleSet` against one document **without enqueuing a job** — the fast feedback loop for authoring selectors, so a typo is caught before a job fetches everything. Body: `{rules, html}` **or** `{rules, url}` (exactly one of `html`/`url`; both or neither → `400 bad_request`), plus optional `base_url`.

- `rules` — a bare `{field: rule}` map (the same shape apps take), e.g. `{"title": {"type":"css","selector":"h1"}}`. Rules are compiled **field-by-field** (each as a single-field `RuleSet`), so **every** bad field is reported at once, not just the first. On any failure the response is `400 bad_request` with a per-field `fields: [{field, error}]` list covering deserialize errors (unknown rule `type`, missing keys) and compile errors (bad CSS selector / regex / XPath). A non-object `rules` is `400`.
- `url` mode fetches through the shared **HTTP tier only** (`FetchStrategy::Http` — no browser render, and never the paid Claude tier), under a modest budget: a 15s fetch timeout (exceeded → `400`) and an 8 MiB body cap (over → `413 too_large`). A non-`http(s)` url or a fetch failure is `400`.
- `base_url` — the document's own URL for `url_absolute` (see above). Defaults to `url` when fetching (a fetched page IS its own base); supply it with `html` to preview link rules against a body you pasted in. An explicit `base_url` wins over `url`, so a rule set can be previewed against a mirror of the real page.

On success (`200`): `{values, report, fields_matched, fields_total}` — the extracted values plus the report (`report.fields`: each field `matched`|`empty`|`container_empty`|`error`; `report.coercion`: `coerced`|`coercion_failed`|`no_transforms`, see above; `report.base_url_missing`: present only when a `url_absolute` transform had no base), so a selector that silently matches nothing — or matches the wrong thing — is visible immediately. `fields_matched`/`fields_total` are the matched-over-attempted counts.

## HTML → Markdown

`pumper_core::html_to_markdown` — boilerplate-skipping converter used by the fetcher (`to_markdown`), `readable`/`watch` apps, and SEDIA clean-text enrichment.

`<table>` renders as a **GitHub pipe table**. The first row is the header: `<th>` cells become the headers, and a `<th>`-less table promotes its first `<tr>` to the header. `<thead>`/`<tbody>`/`<tfoot>` wrappers are traversed; ragged rows are padded to a rectangular grid; cells with nested block content degrade to inline text (whitespace collapsed, `|` escaped); a nested table's text is flattened into its enclosing cell.

## WASM plugin sandbox (`engine-wasm`, `plugin` app)

Hot-swappable `.wasm` extractor modules loaded from the plugins dir (`plugins-src/` holds sources), executed under wasmtime with **fuel + memory limits**. `GET /plugins` lists, `POST /plugins/reload` rescans. `max_memory_mb` bounds **one** store, so the host also enforces a **global concurrency cap** (`[plugins] max_concurrent`, `0` = one per CPU core) via a semaphore acquired before each run — otherwise a wide fan-out admits `max_memory_mb × concurrent_calls` of aggregate wasm memory and can saturate tokio's blocking pool.

The admission permit is held by the **work**, not by the caller: wasm runs on an uncancellable blocking thread, so a caller that stops waiting (a worker timeout, a dropped request) does *not* return the slot — it comes back when the store is actually gone. The bound therefore holds under cancellation, which is exactly when it used to break.

**Cost telemetry.** The sandbox now reports what it enforces. Every call measures CPU **fuel used** (the budget minus what the store had left) and the **linear-memory high-water** (wasm memory only grows and every call gets a fresh store, so the size after the call is that call's high-water — exact, not sampled). `Plugins::run_metered` returns it alongside the value; `Plugins::run` is that call with the cost dropped, so both take one execution path. A host that cannot meter (`NoPlugins`, in-process stubs) gets a default impl reporting `None` for every field — "unmetered" is deliberately distinct from a measured zero, which would read as free.

- **`GET /plugins`** — each entry carries `telemetry: {calls, fuel_last, fuel_max, fuel_avg, fuel_budget, memory_bytes_last, memory_bytes_max, memory_bytes_cap}`. The budgets ride along with the usage, because "18M fuel" answers nothing and "18M of 200M" does. A plugin nothing has run reports `calls: 0` rather than being omitted. In-memory: cleared on restart and on `POST /plugins/reload` (after a hot-swap the name refers to a different binary).
- **`plugin` app job result** — a `cost` object (`signal`, `calls_metered`, `calls_unmetered`, `fuel_total`, `fuel_max`, `fuel_avg`, `fuel_budget`, `memory_bytes_max`), or `null` when nothing was measured. Deliberately on the RESULT, never merged into the dataset records: fuel varies run to run, so a per-record `fuel_used` would mark every record `changed` on every re-run and destroy the change signal the datasets exist to carry. Backfill reports the **attempt's** cost — it is not checkpointed, because a resumed attempt did not pay for the batches an earlier one ran.
- **Observatory rows** — `cost_signal` (`"fuel"` when the host meters, `"elapsed_ms"` otherwise) plus `avg_fuel_used` / `max_fuel_used` / `fuel_budget` / `max_memory_bytes`. `avg_elapsed_ms` stays, now as the labelled fallback rather than as the headline: fuel is deterministic, so a rise between runs is a statement about the plugin; wall clock also measures whatever else the machine was doing. Unlike the job result's `cost`, these *are* on the record — which is why every one of them is declared a **derived path** (below): the same reason cost was kept off records in the first place applies here, and excluding them from the change-detection hash is what buys it back.

**Failure classes.** A failed plugin call reports a typed class alongside its message, so consumers classify on the type rather than on the wording: `unknown_plugin` (no module of that name — build/install it), `plugins_disabled` (`[plugins] enabled = false`), `missing_export` (the module loaded but exports no `memory`/`alloc`/`extract*` — e.g. a describe-only dynamic-app module), `trap` (the sandbox stopped it: explicit trap, fuel exhaustion, or the memory cap), `malformed_output` (it returned, but the bytes are not the contract), `host_error` (the host itself failed around the call). The `plugin` app's observatory mode buckets replays by this class; the trigger decision ledger records it per hook. At the HTTP boundary a plugin failure is a **500** — the sandbox runs in-process, so 502 would blame an upstream that does not exist — with the class carried to the ledger/report surfaces instead of the response body.

**The run door, and what a failed run reports.** The `plugin` app refuses a `plugin` name this host cannot **execute** — before any fetch, any dataset read and any rules-registry write. The check is `Plugins::has`, which answers executability rather than mere presence, so a describe-only module (loaded, but exporting no `extract`/`extract_v2`) is refused too. The refusal is an `Error::BadRequest`, i.e. **terminal for the job**: the `plugin` param, the installed module set and `[plugins] enabled` are all fixed for the life of a job, so the retry ladder could only re-read them and re-refuse. The message names the plugin, points at `GET /plugins`, and lists what *is* runnable — or, when nothing is loaded at all, says so and names `[plugins] enabled` + `just plugins-install` rather than sending an operator to read an empty list. Previously the door was a type check only, so a typo / an uninstalled build / a disabled subsystem produced one `{"error": …}` per URL, `ran: 0`, zero dataset writes and a **green job**: a `succeeded` SSE event, a fired result webhook, an empty dataset. (Observatory mode has always validated its plugin list correctly; the asymmetry inside one app was the defect.)

Per-document failures are then **typed and counted, not stringified**. Every write-mode result carries:

- `ran` — documents whose plugin call **returned**, whatever it returned.
- `errors` — documents that produced no plugin answer, and `errors_by_class`, a `{class: count}` object over `fetch` / `empty_document` / the host's own `unknown_plugin` · `plugins_disabled` · `missing_export` · `trap` · `malformed_output` · `host_error`. Classes that did not occur are **absent**, never zero. Each echoed failure record keeps its `{"error": …}` shape and gains `error_class` beside it.
- `plugin_reported_errors` — outputs the module returned carrying its own `error` key. That is the plugin saying *it* could not extract: **data, not a host failure**. Those records are still not written to the dataset (a record that is nothing but an error message is not a fact about the page), so this count is the only place they are visible.

**Failure policy: partial failure is data, total failure is a failed run.** A run where some documents failed still produced records and stays a success. A run where **every attempted document failed** now fails the job (a retryable `Error::App` naming the counts per class — a site being down is transient, unlike the door refusal). A run that attempted *nothing* — an empty source, a resumed backfill with no rows left — is a quiet success, not a failure.

**Params envelope + manifest.** A plugin can be reused across jobs with different config instead of recompiling a module per variation. The `plugin` app forwards a `plugin_params` object; a params-aware module exports `extract_v2(ptr, len) -> u64` whose input is a `{"doc": .., "params": ..}` JSON envelope (vs the legacy `extract`, which receives just the document). The host prefers `extract_v2` and falls back to `extract` when it's absent, so **plugins built before the envelope keep working unchanged**. A module may also export `describe() -> u64` returning a self-describing manifest (`{name, version, description, params_schema, output_schema}`), read once at load; `GET /plugins` then returns real metadata per plugin (name-only when `describe` is absent). The probe runs under the **configured** `[plugins] fuel` / `max_memory_mb` — a manifest read is a plugin call — rather than under a separate hidden budget that no config could raise. A module with no `describe` export logs at debug (a legal legacy shape); a `describe` that exists and then traps, overruns its budget, or returns non-JSON logs at **warn** on both the plugin-load and the dynamic-app-discovery path, instead of being silently swallowed on one of them. `plugins-src/title-extractor` is the reference implementation of both (`params.tag` extracts an arbitrary tag into `value`).

### The `plugin` app's four modes and its result shape

| mode | declared by | writes records? |
| --- | --- | --- |
| urls | `plugin` + `urls` | yes |
| source | `plugin` + `source` | yes |
| backfill | `plugin` + `source.backfill: true` | yes (per batch) |
| observatory | `observatory` (no `plugin` param) | yes, into the `observatory` dataset |

`urls` fetches each URL live through the metered chokepoint (tiered `strategy`, incl. `auto_with_research`). `source: {app, dataset, keys?, limit?}` runs over already-crawled stored bodies with no re-fetch; `keys` precedence is explicit `source.keys` → the firing trigger's `_trigger.keys` → all live records. The crawl→plugin seam shares `AppContext::read_source_artifact` (one hardened path-traversal guard) with the extractor. `source.as_of` / `source.versions: "all"` resolve through the crawl's versioned archive, and `source.backfill: true` fans the whole `page_versions` archive in checkpointed batches. Observatory mode is covered in its own section below.

**Every write mode** reports `{mode, plugin, dataset, ran, errors, errors_by_class, plugin_reported_errors, new, changed, unchanged, cost|null}` — one definition, not three (`output_shape` promised `errors` and `dataset` for a long time while **no mode emitted either**). `dataset` is where the rows actually landed: normally the requested name, the shadow `<name>@q` when `[resilience] enforce = true` diverted a quarantined source. Per mode on top of that: urls `{requested}`; source `{source{app,dataset}, requested, limit, truncated, loaded, missing, missing_keys[]}`; backfill `{resumed_from_checkpoint, scanned, skipped_pattern, loaded, batches, missing, missing_keys[]}`.

**The `records` echo is a bounded sample.** `urls`/`source` return `records` — a **prefix**, not the corpus — sized by `records_echo` (default **100**, ceiling **1000**, `0` = counts only), alongside `records_total` (the honest count of outcomes the run produced) and `records_truncated`. `backfill` echoes nothing, as it never did. Why: the echo used to be every output — up to 10,000 in source mode, one per URL in urls mode with no `maxItems` on the schema — written into the `jobs.result` column and then re-sent on the terminal SSE event, the result webhook and `GET /jobs/{id}/receipt`, and turned into **one Tantivy doc per element** while the worker's own comment on that indexing path assumed the echo was bounded. **Known gap:** unlike the extractor, this app does not declare `index_datasets`, so bounding the echo also bounds how many of a run's outputs become search documents (the first `records_echo` of them, under `_records`). Declaring it would move indexing to the delta-driven dataset path — better ids, removals honoured — but it is a behavioural change to the indexing identity for this app and is deliberately not bundled here.

**The no-keys sweep says when it truncated.** `source.limit` (default and ceiling **10,000**, most-recently-updated first) caps the live-record sweep, and the result carries `limit` + `truncated`. Judged on the page the store returned, *before* the removed/gone filter — `Datasets::list` does not exclude tombstones in SQL, so they consume slots, and how many rows survived the filter says nothing about whether more exist past the cap. Previously a 12,000-record source ran the plugin over 10,000 and reported `requested: 10000`, a number indistinguishable from a dataset that really holds 10,000 rows. `truncated` is always `false` when the caller named the key set, because no cap applied to it.

### Observatory mode — differential replay against the stored web

`{"observatory": true}` (or an object narrowing it) replays each audited plugin over N sampled stored pages per **site**, classifies each replay `ok` / `trap` / `empty` / `schema_invalid`, and upserts one row per **(plugin, configuration, site)** into the `observatory` dataset with a drift score against the previous run's row. Zero new fetches: it reads bodies the crawl already stored (`pages` + the `page_versions` archive). Sample = newest half + a deterministic-random rest seeded by the site name, so a re-run over an unchanged corpus picks the same pages; every row reports `sampled`/`total_pages`, and a site with fewer than 5 stored pages is marked `low_confidence`. `sample_per_site` defaults to 25 with a ceiling of **500** (the replay count is sites × plugins × this, and the host's semaphore caps parallelism, not count).

**Change detection on this dataset is the intended consumer, and it now works.** Every measurement a row carries is volatile by construction — `run_at`, `prev_run_at`, `avg_elapsed_ms`, `avg_fuel_used`, `max_fuel_used`, `max_memory_bytes`, `drift_score` — and change detection hashes the whole canonical value, so writing them through a plain upsert marked **every row `changed` on every run**: `unchanged` was structurally always 0, a watch on `plugin/observatory` fired on 100% of its rows every run, and the drift signal the feature exists to raise was buried in universal noise. Those seven fields are now declared [derived paths](datasets.md#derived-paths--a-producer-can-say-which-fields-arent-its-own-news), excluding them from the hash and nothing else — the stored row and every revision still carry them in full. Everything that is a **finding** stays in the identity (`outcomes`, `rates`, `shape`, `total_pages`, `sampled`, `classified`, `unreadable`, `empty_artifacts`, `low_confidence`, `empty_rate_rising`, `params`), so real extraction rot still fires. `drift_score` is derived because it is a statement about the *pair* of runs — it settles `null` → `0.0` on the second run ever and `d` → `0.0` after any real change — and it is computed from `rates`, which is in the identity. **The first run after deploy re-hashes every stored row, so they report `changed` once and then settle.**

**Each plugin is replayed with the params it is configured with.** A `plugins` entry may be a bare name — which inherits the job-level `plugin_params` envelope — or `{"name": .., "params": {..}}`, which overrides it. Before this every replay passed `params: null`, so a params-aware module (the reference `title-extractor` reads `params.tag`) was classified `empty` at every site forever; because the rate never *rose*, `empty_rate_rising` never flagged it, `drift_score` compared two meaningless distributions, and the row read `low_confidence: false` and looked authoritative. Two configurations of one plugin are two rows — keyed `plugin@<fingerprint>|site` — so neither overwrites the other's drift history; an unconfigured replay keeps the historic `plugin|site` key, so existing rows and watches survive.

**A rotting corpus is reported as a corpus problem.** A sampled page whose stored artifact is unreadable, or reads fine and holds **zero bytes**, never reaches the plugin. Both are counted as corpus facts on the row (`unreadable`, `empty_artifacts`) and in the run result (`pages_unreadable`, `pages_empty`), and excluded from `classified`, `rates` and `pages_replayed`. The empty case used to short-circuit to a null output *without calling the plugin* and then land in the plugin's `empty` bucket — so a crawl that stored zero-byte bodies inflated the site's empty rate, could trip `empty_rate_rising` and inflated `drift_score`: a false positive on the exact canary this mode exists to raise, blamed on the plugin.

**Known gaps.** The corpus reads are capped at 10,000 rows *globally across all sites* while each row reports `total_pages` as if complete. The replay loop is serial (artifact reads included) and does not read `concurrency`. `plugin/observatory` has no reader in this repo today — a watch or dataset trigger is the intended one, and the change-detection defect above is exactly what made that unusable.

**Yield attribution (a correction).** `extract_yields` keys a yield entry on the JSON **path** where it finds `new`/`changed`, so this app's root-level summary is attributed to the empty-string path — and adding a root `dataset` field does not change that (the extractor, which has had one since r12, is attributed the same way). Re-keying would mean nesting the summary under a dataset-named object, and the walk keeps descending below a match, so the run would then report its counts twice. What is pinned by test is that a run produces exactly **one** yield entry with the right counts, and that the result names its dataset for human/agent readers.

## Extraction health

Rule sets rot when sites change, and the quality report above only makes one run's misses visible — it cannot tell a broken selector from a genuinely absent field *in aggregate*. [resilient-extraction.md](resilient-extraction.md) covers the per-source degradation detector built on it: per-field sketches, a markup-shape fingerprint (`dom_simhash`) next to the existing text one, mined invariants, and a health ladder that stops a degrading source from tombstoning its dataset or pushing downstream.

When health detection is on, the extractor runs `resilience::extract_and_fingerprint_batch` instead of `extract_batch_with_report`: same rayon fan-out, same records and reports, but the DOM each document is parsed into is **shared** with fingerprinting rather than rebuilt for it. Measured 1.8× faster and 38% lower peak RSS on a 2000-document / 110 MB batch, with byte-identical fingerprints — see [resilient-extraction.md §2.8](resilient-extraction.md#28-what-it-costs-at-this-scale). Rule sets with no CSS rule are unaffected in parse count: they never parsed HTML for extraction and still pay exactly one parse, the fingerprint's.

## Known gaps

- Plugin cost telemetry covers calls that **returned**. A call that traps propagates the error and the fuel it burned on the way is not carried (the error type has nowhere to put it), so a plugin failing constantly shows up in the outcome counts rather than in the fuel figures. `GET /plugins` telemetry is in-memory: it resets on restart and on `POST /plugins/reload`.
- No schema-less/LLM-assisted extraction yet (backlog moonshots: NL→RuleSet, self-healing selectors). Only the `extractor` app reports runs to the health detector; `plugin` and the hardcoded-Rust apps do not yet.
