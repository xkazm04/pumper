---
name: extraction-core            type: perfect/context
group: Core Platform             category: lib
opportunity: 6                   # engine itself strong (round-3 work); headroom sits in induce.rs + each-report blindness
last_proposed: 2026-08-12        cooldown_until: round-13
directions: ["[[each-field-reports]]", "[[extract-honesty-sweep]]", "[[url-absolutize]]"]
supersedes: "[[declarative-extraction-engine]] (old map; its 4 shipped directions carry over; rules:'auto' LLM drafting REJECTED — do not re-propose)"
---

## Current state (scout brief 2026-08-11 — BANKED, no slate drafted: round-10 pool cap
## reached by browser-transact + api-surface. This context is the round-11 cursor;
## brief is <1 round old, re-verify only the line numbers.)

extract.rs heavily consumed (extractor both modes, replay, provisioner dry-run, /extract/preview,
/provenance replay, resilience shared-DOM fingerprinting, datahub lineage). induce.rs (M09,
2026-07-31) wired to exactly ONE consumer (extractor `induce` param root) — no HTTP route (the
designed POST /extract/suggest never built), zero automated feedback loop, run_induce has ZERO
tests (no induce_mode.rs integration test — largest coverage hole in the context). json_salvage.rs
is an LLM-output helper mis-grouped here (all consumers agentic: trades-common, research,
provisioner ×2).

Top candidates for the round-11 slate (all scout-verified with file:line):
1. **each-inner-field report blindness** — the biggest structural gap: Each emits ONE FieldStatus
   for the whole array (extract.rs:659-667, extract_scoped :827-876 no report) → worst_fields,
   replay deltas, health sketches, provisioner gates, datahub lineage all blind inside listings —
   exactly the shape induce emits and extraction.md recommends.
2. **XPath honesty pair**: non-node results render as Rust Debug output (:772 — count()/string()
   yield garbage strings); runtime failure misclassified Empty not Error (:747-749).
3. **induce quality bundle**: Tailwind utility classes make meaningless selectors (:408-429,
   div.border.flex matches every flexbox); pass-1 exact-signature census vs pass-2 descendant
   selector count different things (:434-442 vs :255-261); instances denominators diverge
   (:262-277 vs MAX_INSTANCES cap); no transforms suggested; only a[href] attrs; naming arbitrary
   (:454-481); all DOMs held simultaneously (:123, 500-page ceiling = multi-GB).
4. **default-transform dead on blanks** ("":[]-blank keeps value, default ignored — :310 vs
   :524-531); to_number/to_int disagree on overflow (inf→Null vs i64::MAX, NaN→0).
5. **URL absolutization transform missing** entirely (no access to source URL in extract_one;
   induce emits relative hrefs; markdown links relative too).
6. markdown.rs: colspan/rowspan misalignment, caption dropped, code-fence language lost,
   form/figure/head in fixed SKIP (gov portals' content lives in <form>), SKIP change silently
   invalidates every stored text simhash (resilience/mod.rs:306 coupling).
7. Untested: uppercase + regex_replace transforms (only 2 of 10 with zero coverage), nested each,
   rayon panic isolation (one doc's panic kills the batch, :728; two guarded unwraps :626,:665).
8. Docs: induce + replay modes absent from docs/features/ entirely; apps.md:51 stale
   trades_common::salvage_json path; known-gaps omits the each-blindness.

## Direction history
- (as declarative-extraction-engine, round 3): 4/4 shipped (quality signal 70221c1, tables+numbers
  ebe5f89, stored-pages 66b063f, preview 387a509). REJECTED: rules:"auto" LLM drafting — third
  LLM-feature rejection; deterministic substrate only unless the owner asks.
- 2026-08-11 (round 10): scouted, slate NOT drafted (pool cap). No cooldown — round-11 cursor.
- 2026-08-12 (round 11, director-self-gated): banked brief re-verified inline. CONFIRMED:
  each-blindness (:659-671), XPath Debug garbage (:772) + Err→Null→Empty (:747), default dead
  on blanks (:310 vs is_blank :524), markdown SKIP form/figure, induce all-DOMs (:123),
  url-absolutize absent. **SEED CORRECTED (decay rule strikes again): run_induce has 9 in-file
  unit tests (induce.rs:488+) — the "zero tests" claim was false at scout time; the real gap is
  app-level induce integration + no HTTP surface.**
  ACCEPTED 3: [[each-field-reports]] (robustness M) · [[extract-honesty-sweep]] (robustness S) ·
  [[url-absolutize]] (feature M).
  REJECTED-deferred: **induce-surface** (POST /extract/suggest + the induce quality bundle:
  Tailwind-utility selectors — usable_class filters only build digests, div.border.flex still
  passes; census/denominator mismatches; naming; DOM memory ceiling) — BANKED as this context's
  next anchor; correctness debt outranked new surface at the round cap.
  REJECTED-deferred: **markdown-fidelity** (form/figure/caption/code-fence) — SKIP-list changes
  invalidate every stored text simhash (resilience/mod.rs:306 coupling); needs a guarded design
  (simhash migration or versioned SKIP) as its own direction, never a side-fix.

## Shipped
- (carried from declarative-extraction-engine — see that note)
