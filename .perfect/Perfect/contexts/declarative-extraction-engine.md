---
name: "Declarative Extraction Engine"
type: perfect/context
group: "Data Extraction & Storage"
category: lib
opportunity: 7
last_proposed: 2026-07-13
cooldown_until: —
directions: ["[[extract-from-stored-pages]]", "[[ruleset-preview-endpoint]]", "[[extraction-quality-signal]]", "[[markdown-tables-tonumber]]"]
---

## Current state (scout brief digest, 2026-07-13)

- Engine is clean and parallel-correct: RuleSet (css/regex/json/xpath/const + 9 transforms) compiles ONCE per job (Arc<CompiledRuleSet>), rayon across documents, lazy per-doc parsing (HTML/simd-json/xpath trees only when a rule needs them). Compile-time syntax validation; tested.
- **Every miss is a silent Null** — no per-field match status, nothing distinguishes "field absent" from "selector broken" (extract.rs:290-316, :372). No fields_matched metrics. THE prerequisite gap for self-healing.
- **No submit-time validation / preview** — rules validated only when a job runs; `RuleSet::compile()` is a pure fn a preview endpoint could call (extract.rs:101).
- **Extractor re-fetches everything**: only accepts `params.urls`, fetches live (extractor/lib.rs:63-75); crawl's `pages` records carry `artifact_path` to bodies already on disk (crawl.rs:596-612) — crawl→extract today double-fetches. Trigger plumbing exists (changed keys injected via `_trigger`) but extractor doesn't read `_trigger.keys`.
- Failed fetch → empty string → all-null record still upserted (extractor/lib.rs:70-74); only aggregate `fetched` count.
- No first-class RuleSet entity (lives in job params only; no naming/versioning/reuse).
- T5 LLM-assisted extraction OPEN (extraction.md:27); all primitives exist: ctx.research (metered/cached/budgeted), json_schema → could force Claude to emit a valid RuleSet, html_to_markdown for token-shrink, save_artifact, role presets.
- markdown.rs gaps: NO <table> structure (td/th fall through — no pipe tables), fixed skip-list, `to_number` transform is lossy ("1-2" → -12, strips non-[0-9.-]).

## Direction history
- 2026-07-13 (round 3): 5 proposed, 4 accepted (stored-pages seam, preview endpoint, quality signal, markdown tables + to_number). **REJECTED**: rules:"auto" LLM drafting (wildcard) — third rejection of an LLM-driven feature (after provenance + exit-readiness); taste: deterministic substrate first, LLM features only on explicit ask. Do not re-propose T5 drafting/healing without a user steer.

## Shipped
- [[extraction-quality-signal]] → 70221c1 — FieldStatus (Matched|Empty|Error) + DocReport, extract_*_with_report; failed fetches attributed + never upserted as all-null; fields_matched/total + worst_fields in results.
- [[markdown-tables-tonumber]] → ebe5f89 — GitHub pipe tables (th header / first-row promotion, ragged padding, nested degradation); parse_first_number ("1-2"→1, "$1,234.50"→1234.5); grep confirmed no consumer relied on old concatenation.
- [[extract-from-stored-pages]] → 66b063f — source:{app,dataset,keys?} mode; resolution data/artifacts/<app>/<job_id>/<artifact_path>; keys: source.keys → _trigger.keys → all (cap 10k); missing_keys attribution; anti-double-fetch PROVEN by integration test with panic-on-fetch stub engines.
- [[ruleset-preview-endpoint]] → 387a509 — POST /extract/preview {rules, html|url}; per-field compile collects ALL bad fields (not just first); url mode = HTTP tier only (never the paid claude tier), 15s + 8MiB budget; returns values + report + fields_matched/total. Live-verified incl. 3-simultaneous-bad-rule diagnostics.
Context COMPLETE: 4/4 shipped.
