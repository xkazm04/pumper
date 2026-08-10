---
name: source-provisioner
type: perfect/context
group: Content & Research Apps
category: lib
opportunity: 7
last_proposed: 2026-08-04
cooldown_until: 2026-08 +2 rounds
directions: ["[[provisioner-sample-stage-fix]]", "[[provisioner-coherent-scoring]]", "[[proposal-lifecycle]]", "[[provisioner-record-honesty]]"]
---

## Current state (scouted 2026-08-04, HEAD 8adfc91; §0 bug Director-verified in code)

Single file `crates/apps/provisioner/src/lib.rs`. Pipeline: discovery (Claude research,
metered/cached) → sample candidates (tiered fetch, MAX_CANDIDATES=3) → draft↔dry-run loop
(≤5 iterations, resume_session) → emit proposal into `provisioner/proposals`.

**Top findings:**
1. **SHOWSTOPPER (verified): sample stage discards the fetched page.** `lib.rs:554-568`
   reads `outcome.text`/`.markdown`, never `.html`, never sets `req.to_markdown` — but the
   http/browser/archive tiers put the body in `html` and only fill `markdown` when asked
   (`fetcher.rs:885-906`). Only Claude-tier (unreachable under `FetchStrategy::Auto`) and
   api-recipe replay fill `text`. Net: normal fetches → empty body → all candidates skipped
   → hard error AFTER the paid discovery call. readable/watch set `to_markdown = true`;
   provisioner copied the read pattern without the flag. No test covers the sample stage.
2. **Proposals rot — round-4 flag CONFIRMED.** Nothing reads `provisioner/proposals`: no
   route, MCP tool, doctor check, or promote path. Manifest claims "a human applies it via
   the catalog reconciler" — no such path exists; real promotion is ONBOARDING Path B
   (write app crate + registry + [[source]]), which the proposal barely feeds.
3. **Cross-document scoring incoherent**: draft written against `bodies[0]` only but scored
   over up to 3 bodies from DIFFERENT sites; sample count silently changes the pass bar
   (2 samples: primary-only field passes; 3: fails → repair burns on unfixable mismatch).
4. **Degenerate drafts score high**: `Const` rules always bind; `ContainerEmpty` counts as
   match; computed `CoercionStatus` (wrong-element signal) entirely unused.
5. **Cost honesty**: `budget_usd` is per-CALL not total (worst ~6×); repair iterations
   bypass the research cache (resume_session); terse repair prompt sent even when
   `session_id` is None (guaranteed wasted paid call); budget pre-check inert without a
   job-level budget.
6. **Emitted row corrupts documented vocabulary**: confidence written 0-100 into catalog's
   documented 1-5 scale; `access: "public"` not in vocabulary; `engine: "http"` hardcoded
   (real winning tier discarded); `row.dataset` names a nonexistent dataset; `accepted`
   flag lost from the pasted catalog_row; `accepted:false` proposals emitted same-shaped.
7. **Docs: the app does not exist** — absent from docs/features/apps.md and every other doc;
   only frozen moonshot reports mention it.
8. Tests: good pure-fn coverage; **no test drives run()** (Researcher stubs exist in
   core/testing.rs, unused here); sample stage, budget break, accepted:false path untested.

## Direction history
- 2026-08-04 (round 7): presented 5, **accepted 4** — sample-stage fix (confirmed
  showstopper), coherent scoring, proposal lifecycle, record honesty. **REJECTED:
  budget-honesty (optimization)** — first rejection since round 3; implied reason: spend is
  already hard-bounded by the iteration cap (≤6 calls), so total-budget accounting is low
  urgency next to a broken pipeline. Note for taste: cost-accounting work on a
  not-yet-working feature doesn't clear the bar.

## Shipped
- [[provisioner-sample-stage-fix]] → `0978626` — the app can complete a run (html-first
  body selection; real engine recorded; run()-level e2e).
- [[provisioner-record-honesty]] → `99fe5cc` — 1-5 confidence, valid vocabulary, rejected
  proposals visible, app documented.
- [[provisioner-coherent-scoring]] → `6c16962` — primary-doc bar; degenerate drafts
  rejected before paid repairs.
- [[proposal-lifecycle]] → `522219c` — list/validate/promote/expire; proposals have
  consumers; never-writes-catalog invariant intact.
