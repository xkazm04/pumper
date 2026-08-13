---
slug: trades-numbers-mean-what-they-say
type: perfect/direction
context: "[[trades-pricing]]"
lens: robustness
status: accepted
size: M
proposed: 2026-08-13
accepted: 2026-08-13
shipped: —
commit: —
---

## What & why
These six apps take model output and store it as market data other products read. Three separate
defects mean a stored number does not reliably mean what it says — and in each case **the correct
implementation already exists inside this same family** and the siblings did not adopt it.

**1. Numeric strings are stored raw, and then silently dropped.** `validate::num` deliberately
accepts numeric strings, stripping `,` and `$` (`trades-common:240-246`). But `state-tax:227,187`,
`state-licensing:493`, `trade-wages:297` and `valuation:260` all store `s.clone()` / `t.clone()` —
the raw model JSON. So `"top_marginal_rate": "13.3"` passes `require_rate` as 13.3 and is stored as
the **string** `"13.3"`. Downstream, `sync_operator_economics:995` does `.and_then(Value::as_f64)` →
`None` → that state **silently drops out of `median_state_rate`**. The catalog's `ranges` contract
cannot catch it either, because ranges are checked only when the field is present and *numeric*
(`catalog.rs:114-116`). `homewyse-pricing:255-261` fixed exactly this and wrote the reasoning into
the code; four siblings ignored it.

**2. The unit convention is undeclared, and the only test of it encodes the opposite one.**
`state-tax`'s prompt demands percentages — *"Rates are percentages (e.g. 13.3, and 0 for
no-income-tax states)"* (`:148-149`). `require_rate` accepts `[0,100]`, which admits `13.3` **and**
`0.133` identically. And `trades-common`'s own consumer test,
`state_tax_context_carries_the_real_rate_not_a_median` (`:1294-1305`), uses **fractions**: `0.133`,
`0.37`, `0.153`. The producer's prompt and the consumer's only test disagree about the unit, nothing
records which is authoritative, and a downstream forecast reading `13.3` as a fraction computes a
1330% tax set-aside. **This is the family's one test pinning a wrong contract, and it is in the
shared crate.**

**3. A closed vocabulary that nothing closes.** `state-tax` prompts for `income_tax_type` as
`"none"|"flat"|"graduated"` (`:146`) and schemas it as bare `string` (`:308`). Nothing normalizes or
rejects, so `"progressive"`, `"Graduated"` and `"N/A"` all store and flow into `state_tax_context`
(`trades-common:1150`). The sibling app does this correctly — `normalize_requirement_level`
(`state-licensing:525-539`) returns `None` for junk and *rejects* the record.

**Rider — the one outright fabrication in the family.** `homewyse-pricing:254`:
`"unit": j.get("unit")…unwrap_or("flat")`, and `unit` is **not** in the schema's required list
(`:334`). An hourly labor rate whose `unit` the model omitted is stored as a **flat job price** —
$150/hour becomes a $150 job. Everything else in the family honours honest-Null (`cms:42` even pins
"never a fabricated 0.00"). The app that fixed the numeric-string hole is the one that fabricates a
semantic field.

**Rider — a `$0` that means "unknown".** `state-licensing:264-266` does
`output.cost_usd.unwrap_or(0.0)`, reproducing verbatim the anti-pattern `app.rs:786-796` was written
to kill: *"An envelope with no `total_cost_usd` is not a free call… Recording it as a bare `$0`
makes it indistinguishable from a genuinely free cache hit."* The ledger records `cost_unreported`;
the job result says `$0.00`; the two disagree.

## Evidence
- `crates/apps/trades-common/src/lib.rs:240-246` — `validate::num` accepts numeric strings
- `crates/apps/state-tax/src/lib.rs:227, 187` · `state-licensing:493` · `trade-wages:297` ·
  `valuation-multiples:260` — store the raw model value
- `crates/apps/homewyse-pricing/src/lib.rs:255-261` — the fix, **with its reasoning in a comment**
- `crates/apps/trades-common/src/lib.rs:995` — `.and_then(Value::as_f64)` → the silent drop
- `crates/core/src/catalog.rs:114-116` — `ranges` only sees numeric values
- `crates/apps/state-tax/src/lib.rs:148-149` — prompt: percentages
- `crates/apps/trades-common/src/lib.rs:1294-1305` — the test using fractions
- `crates/apps/trades-common/src/lib.rs:309-315` — `require_rate` accepts `[0,100]`, admitting both
- `crates/apps/state-tax/src/lib.rs:146, 308` — open `string` for a declared closed vocabulary
- `crates/apps/state-licensing/src/lib.rs:525-539, 456-464` — the correct pattern, in the family
- `crates/apps/homewyse-pricing/src/lib.rs:254, 334` — `unwrap_or("flat")`, `unit` not required
- `crates/core/src/app.rs:786-796` — the `$0`-means-unknown anti-pattern, documented

## Acceptance criteria
1. Every app in the family stores **validated numbers, not raw model values**, for fields it
   validates. A test proves that a model answer with `"13.3"` (string) round-trips as a JSON number
   and reaches `median_state_rate` — it must fail against today's code.
2. The rate unit is **declared and enforced**, not implied. Decide percentages-vs-fractions from the
   producer prompts (which say percentages), make `require_rate` or the storage path enforce it, and
   fix `state_tax_context_carries_the_real_rate_not_a_median` to the authoritative convention.
   **Do not simply edit the test to match the code or vice versa — establish which is correct, say
   so in a doc comment, and make the other side conform.**
3. `income_tax_type` is normalized against its closed vocabulary and a junk value rejects the record,
   following `normalize_requirement_level`'s shape. Reuse that helper's pattern rather than forking a
   fourth spelling of it.
4. `homewyse-pricing` stops fabricating `unit`: an absent unit is honest-Null (or the record is
   rejected), matching the family's stated convention. A test named for the anti-pattern pins it.
5. `state-licensing` stops reporting `$0.00` for unreported cost — absence is reported as absence,
   consistent with what the ledger already records.
6. Where you add a shared helper, it goes in `trades-common` and **every** app in the family adopts
   it. A fix applied to four of five apps is how this defect was born; do not recreate it. If one app
   legitimately differs, say why in a comment.

## Risks / non-goals
- **Non-goal:** re-validating model output against the JSON schema after the loose-parse/salvage
  fallbacks. That is a real, banked finding; it is a bigger design and is not this direction.
- **Non-goal:** changing `validate::num`'s deliberate acceptance of numeric strings — the bug is in
  what gets *stored*, not what gets *accepted*.
- **Note:** `trades-common` is also consumed by `census-density`, `census-nesd` and `census-nonemp`
  (for `taxonomy::registry_naics`). Changing taxonomy signatures blasts across 8 apps — prefer
  additive changes, and if you must break a signature, say so in your report.
- Risk: enforcing a rate unit could reject historically-stored fractional values on read. Enforce at
  write; if a read-path migration is implied, report it rather than performing it.

## Build record
(filled during build)
