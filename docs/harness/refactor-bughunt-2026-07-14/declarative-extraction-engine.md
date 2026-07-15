# Declarative Extraction Engine — refactor + bug-hunt findings

> Total: 5 findings (Critical: 0, High: 1, Medium: 4, Low: 0)
> Files scanned: `crates/core/src/extract.rs`, `crates/core/src/markdown.rs` (cross-checked `crates/apps/extractor/src/lib.rs`, `crates/server/src/routes.rs` to confirm consumer impact)

## 1. JSON-pointer rules are never validated, so a malformed pointer reports `Empty` (real miss) instead of `Error` — defeating the DocReport quality signal
- **Severity**: High
- **Lens**: bug-hunter
- **Category**: silent-failure / validation-gap
- **File**: `crates/core/src/extract.rs:116` (compile), `:407-410` (extract), consumed at `crates/server/src/routes.rs:2482-2523` and `crates/apps/extractor/src/lib.rs:26-45`
- **Scenario**: An operator writes `{"type":"json","pointer":"data/0/name"}` (RFC-6901 requires a leading `/`; this one lacks it) — or any other invalid pointer. `RuleSet::compile` handles CSS via `Selector::parse`, regex via `Regex::new`, XPath via `skyscraper::xpath::parse`, and transform regexes — every one surfaces a compile error. But `Rule::Json` compiles to `CompiledRule::Json { pointer: pointer.clone() }` — a no-op string clone with **no validation**. At extract time, `serde_json::Value::pointer("data/0/name")` returns `None` for an invalid pointer, yielding `Value::Null` with `ran=true`, which `FieldStatus::classify` labels `Empty` (`:346-352`).
- **Root cause**: JSON is the one rule type whose "compilation" is a pass-through; the other four fail-closed at compile. The design assumes a pointer string is always well-formed, so it never distinguishes "pointer is broken" from "the JSON simply lacks this key."
- **Impact**: Wrong result that is actively misleading. The whole point of `DocReport`/`FieldStatus` (per its own doc comment) is to separate a mis-configured rule from a genuinely-absent field. A broken JSON pointer masquerades as `Empty` on *every* document forever; `summarize_reports` files it under harmless "empty" misses (not the `error` bucket at `extractor/lib.rs:38`), and `/extract/preview` reports `empty` — so the author sees "field absent in this page" and never learns the rule is dead. The preview endpoint's field-by-field compile (`routes.rs:2493-2504`), advertised as catching "every bad field," silently passes malformed pointers.
- **Fix sketch**: Validate the pointer in `RuleSet::compile` — reject a non-empty pointer that does not start with `/` (and optionally pre-split segments), returning `Error::Parse(format!("bad json pointer '{pointer}'"))`, mirroring the other four arms.

## 2. `default` transform only replaces `Null`, so `all:true` misses (`[]`) and whitespace-only matches (`""`) are never defaulted
- **Severity**: Medium
- **Lens**: bug-hunter
- **Category**: edge-case / transform-semantics
- **File**: `crates/core/src/extract.rs:177-185` (`apply`), classification contrast at `:346-351`
- **Scenario**: A field uses `{"type":"css","selector":".tags","all":true,"transforms":[{"op":"default","value":[]}]}` or, more tellingly, `{"selector":".price","transforms":[{"op":"trim"},{"op":"default","value":"n/a"}]}`. When an `all:true` CSS/XPath rule matches nothing, `css_extract`/`xpath_extract` return `Value::Array([])` (`:488`, `:451`), **not** `Null`. When a single element matches but is whitespace-only, `trim` produces `Value::String("")`. In `apply`, the `Default` arm only fires on `Value::Null` (`:178`); `[]` and `""` hit the passthrough arm `(Self::Default { .. }, v) => v` (`:179`) and are returned unchanged.
- **Root cause**: `default` is defined narrowly as "replace a **null** result," but the rest of the engine treats empty-string and empty-array as equivalent misses — `FieldStatus::classify` explicitly maps `""` and `[]` to `Empty` (`:348-349`). The two notions of "empty" diverge only inside `default`.
- **Impact**: Wrong/surprising result. An author adds `default` to guarantee a fallback for missing fields; it silently does nothing for the two most common "empty" shapes (list selectors that miss, blank cells), so the output record carries `[]`/`""` where the configured sentinel was expected. Chaining `trim` before `default` — the natural idiom — makes it worse, not better.
- **Fix sketch**: Broaden the `Default` match to also replace `Value::String(s)` where `s.trim().is_empty()` and `Value::Array(a)` where `a.is_empty()` — i.e. align `default` with the same emptiness test `FieldStatus::classify` already uses.

## 3. XPath results that are not nodes (atomics from `count()`, `string-length()`, boolean/number expressions) are serialized as Rust `Debug` strings
- **Severity**: Medium
- **Lens**: bug-hunter
- **Category**: silent-failure / wrong-result
- **File**: `crates/core/src/extract.rs:460-473` (`xpath_item_value`)
- **Scenario**: A rule uses `{"type":"xpath","xpath":"count(//li)"}` (or `string-length(...)`, or a comparison/number-producing expression). `xpath.apply` returns an `XpathItem` that is not `XpathItem::Node`, so it falls through to `other => Value::String(format!("{other:?}"))` (`:472`). The extracted value becomes the Rust `Debug` rendering of skyscraper's internal atomic type (e.g. `"AnyAtomicType(Integer(3))"`-style text) rather than `3` or `"3"`.
- **Root cause**: The `match` only decodes the three node kinds (attribute / text / element) and treats "everything else" as debug-printable. The design assumes every XPath result is a node from a location path; atomic-valued XPath expressions were not accounted for.
- **Impact**: Wrong result that also passes `FieldStatus::classify` as `Matched` (it is a non-empty string), so nothing flags it. Any XPath function or expression producing a scalar yields internal-type garbage into the dataset. (Node-path XPath — the dominant usage — is unaffected; reachability depends on which expressions skyscraper 0.7 evaluates to atomics, so severity is Medium, not High.)
- **Fix sketch**: Match the atomic/function variants explicitly and render their underlying value (number → `Value::Number`, string → `Value::String`, bool → `Value::Bool`); at minimum extract the inner scalar's `Display` rather than the enum's `Debug`.

## 4. `parse_first_number` treats a decimal comma as a thousands separator → 100× error on EU-locale numbers
- **Severity**: Medium
- **Lens**: bug-hunter
- **Category**: edge-case / locale
- **File**: `crates/core/src/extract.rs:253-299` (`parse_first_number`), esp. `:285` (`b',' if is_digit(j + 1) => j += 1`)
- **Scenario**: `to_number`/`to_int` over `"2,50 €"` (European price for 2.50) or `"1.234,56"` (German grouping). The parser drops *any* comma that sits between two digits, regardless of grouping width, so `"2,50"` → `250` and `"1.234,56"` → `1.234` (the `.` becomes the decimal point, `,56` is then a stray token that breaks the run) — either way, a value that is off by ~100×, or truncated.
- **Root cause**: The comma-handling rule (`:284-285`) encodes an implicit US/UK locale assumption — comma = thousands, period = decimal — and does not require thousands groups to be exactly three digits. On a platform whose `to_number` doc explicitly advertises tolerating `€ £`, EU-formatted numbers are a realistic input, but the parser has no locale awareness.
- **Impact**: Silent, high-magnitude wrong result on money — the exact data a scraper most needs correct. Reports it as `Matched` (a number was produced), so the corruption is invisible. Related nit in the same function's caller: `coerce_number` for `to_number` returns `Null` on a non-finite result (`serde_json::Number::from_f64` rejects it, `:242`), whereas `to_int` does `n.trunc() as i64` (`:241`) which **saturates** a huge/overflowing value to `i64::MAX` instead of nulling — an inconsistent, silently-wrong outcome for the same input across the two ops.
- **Fix sketch**: Either document the US-locale assumption as a hard contract and reject ambiguous input, or add an optional locale/decimal-separator to the `ToNumber`/`ToInt` transform; separately, make `to_int` fall back to `Null` (not `i64::MAX`) when `n` is non-finite or outside `i64` range, matching `to_number`.

## 5. Markdown table builder walks rows in DOM/source order and ignores `<thead>`/`<tbody>`/`<tfoot>` roles → a source-first `<tfoot>` becomes (or precedes) the header
- **Severity**: Medium
- **Lens**: bug-hunter
- **Category**: edge-case / structural
- **File**: `crates/core/src/markdown.rs:169-189` (`collect_rows`), `:144-163` (`render_table`)
- **Scenario**: HTML/HTML5 permit (and legacy HTML4 required) `<tfoot>` to appear **before** `<tbody>` in source; html5ever/`scraper` keeps the DOM in source order (only CSS moves the foot visually). For `<table><thead><tr>…H…</tr></thead><tfoot><tr>…Totals…</tr></tfoot><tbody><tr>…D1…</tr><tr>…D2…</tr></tbody></table>`, `collect_rows` recurses into the wrappers via the `_ => collect_rows(child, …)` arm (`:185`) and appends rows in document order → `[H, Totals, D1, D2]`. `render_table` then unconditionally takes `rows[0]` as the header (`:153`) and emits every remaining row as data — so the **Totals footer prints directly under the header, above the real data**. If the table is `<tfoot>`-first with no `<thead>`, the footer row itself is promoted to the header.
- **Root cause**: `collect_rows`/`render_table` flatten all `<tr>` regardless of their `thead`/`tbody`/`tfoot` parent and hard-code "first collected row = header." The doc comment claims header handling based on `<th>` presence, but the code never inspects `td` vs `th` (`:177` collects both identically) nor section role — so section ordering leaks straight into output order.
- **Impact**: Wrong table structure/ordering for a realistic (financial/generated) table shape, and the resulting Markdown is what gets fed to the Claude engine, so the model reads a totals row as if it were column headers or the top data row.
- **Fix sketch**: In `collect_rows`, bucket rows by section (`thead` → header rows, `tbody` → body, `tfoot` → appended last) and let `render_table` prefer a `thead`/`<th>` row as the header, emitting `tfoot` rows after `tbody` rows regardless of source order.
