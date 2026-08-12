---
slug: url-absolutize
type: perfect/direction
context: "[[extraction-core]]"
lens: feature
status: accepted
size: M
proposed: 2026-08-12
accepted: 2026-08-12
shipped: —
commit: —
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
(pending)
