---
slug: url-absolutize
type: perfect/direction
context: "[[extraction-core]]"
lens: feature
status: shipped
size: M
proposed: 2026-08-12
accepted: 2026-08-12
shipped: 2026-08-12
commit: ee5d8e4
---

## What & why
Extracted links are relative and stay relative: transforms are pure value→value with no
access to the page URL, so every listing scrape that pulls `a[href]` yields `/item/123`
strings that break each downstream consumer (crawl seeding, watch targets, peer mirrors,
any external client). A `url_absolute` transform resolved against the document's own URL
makes extracted link fields directly usable — the single most-requested-shaped gap in the
declarative engine.

## Evidence
- crates/core/src/extract.rs — no absolutize/join anywhere; CompiledTransform::apply has no
  document context (verified by grep 2026-08-12).
- crates/apps/extractor/src/lib.rs:313-350 — per-doc source_url exists in metas (the app
  KNOWS each page's URL; it just can't hand it to transforms).
- crates/core/src/induce.rs — induced `a[href]` fields emit relative hrefs today.

## Acceptance criteria
- A `url_absolute` transform resolves relative URLs against a per-document base URL
  (protocol-relative and absolute inputs pass through unchanged; garbage input → Null or
  unchanged, documented choice); uses a real URL library, not string concat.
- The extraction call path carries the document URL from every source mode that has one
  (fetched pages AND stored-pages/replay where the stored artifact has a URL); modes without
  a URL degrade to no-op with an honest report signal, not silent wrong output.
- induce emits url_absolute for href-like attribute fields.
- Tests: relative + absolute + protocol-relative + missing-base cases; an extractor-app-level
  test proving an extracted href comes out absolute.
- /extract/preview honors it (same code path — verify, don't fork).

## Risks / non-goals
- Non-goal: rewriting URLs inside markdown output (separate, banked with markdown-fidelity).
- Risk: base-URL choice after redirects — use the final fetched URL if the fetch layer
  records it; builder verifies what the artifact/meta actually stores before wiring.

## Build record
Continuation builder (E2), commit `ee5d8e4`. Seam: base passed PER CALL —
`CompiledTransform::apply(value, base: Option<&Url>)` — not compile-bound (one
CompiledRuleSet is shared by reference across a rayon batch of different-URL docs;
rebinding would recompile every selector per doc). Additive entry points
`extract_one/batch_with_report_at`; originals delegate with None. `absolutize` via
url::Url::join (RFC 3986); unresolvable → UNCHANGED never null; blank → blank
(`base.join("")` returns the base — the fabrication is guarded by
`absolutize_never_fabricates_a_url_from_nothing`). `DocReport.base_url_missing` (doc
count in all four extractor result shapes; `#[serde(default)]` on BackfillState — no
checkpoint bump). Fused extract+fingerprint path carries no URL → rule sets with
`needs_doc_url()` take base-carrying extraction + signals_batch second parse, paid only
by opt-ins (proper fusion needs a resilience/mod.rs signature change — follow-up).
induce emits url_absolute on href/src/poster slots. Preview: `base_url` param, tested
`preview_base` precedence (explicit > fetched url > none), same code path verified.
KNOWN LIMIT (builder refutation of the brief): tiered FetchOutcome discards the HTTP
engine's final_url (fetcher.rs:638/841/1015 set url: req.url) — base is the REQUESTED
URL; cross-origin redirects resolve against the pre-redirect origin. Fix = plumb
final_url through FetchOutcome (fetcher.rs, out of write set). **Banked as a
tiered-fetcher anchor.** Gates: check + workspace lib + 3 app-level tests + routes:: 79
green; fmt clean; no new clippy in touched files.
