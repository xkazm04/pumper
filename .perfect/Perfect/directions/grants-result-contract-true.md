---
slug: grants-result-contract-true
type: perfect/direction
context: "[[us-federal-grants]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-13
accepted: 2026-08-13
shipped: 2026-08-13
commit: 8753553
---

## What & why

`GET /apps` and the MCP tool manifest publish each app's `output_shape` as the contract a consumer
codes against. grants-gov's declares two keys it never emits and omits twelve it does.

- **`hit_count`** — the result emits `hitCount` (`:527`). A consumer keying on the declaration
  reads `undefined`.
- **`removed?`** — never emitted anywhere. `grep -c "removed"` over the crate returns **1**, and
  that single hit *is the declaration itself*. It is not merely unemitted but structurally
  unemittable: `upsert_many_with_provenance` can never populate `UpsertSummary.removed`, because
  only `sync_many` produces removals.
- **Missing**: `source`, `oppStatuses`, `pages`, `digestDays`, `closingSoonCount`, `closingSoon[]`,
  `truncated`, `unified:{new,changed,events,dataset,trust,sourceState}`, `swept`, `warnings[]`,
  `index_datasets[]`, `details.resumedFrom`. Both siblings declare theirs correctly
  (`ca-grants:83-91`, `eu-sedia:117-131` — both list `truncated`, `warnings[]`,
  `index_datasets[]`, `unified:{…}`).

Two more truth gaps in the same surface:

- **The crate contradicts itself three times, in one file, about its own default.** `:20` says the
  detail harvest is "default **ON** since 2026-08-04"; `:151` says "Default TRUE"; `:353` says
  "// NOFO detail harvest (default OFF)" and `:360` is `unwrap_or(false)`. The runtime default
  when the param is absent is *false*; only `default_params` supplies true. A caller who builds
  params without merging `default_params` silently gets no harvest and no warning.
- **Absent data is fabricated as empty, and the tests pin it.** `applicant_types` returns `[]` when
  the field is absent (`:996`, asserted `:1204`) and `attachment_manifest` returns `[]` when the
  whole block is absent (`:1006`, asserted `:1226`) — while money in the very same block follows
  the honest-Null rule (`:932-934`). A consumer cannot tell "this NOFO published no eligible
  applicant types" from "the field was renamed". This is the r15 census shape at small scale, and
  because the tests assert it, it is a pinned contract rather than an oversight.

## Evidence

- `crates/apps/grants-gov/src/lib.rs:186-200` — the `output_shape` declaration.
- `crates/apps/grants-gov/src/lib.rs:524-545` — what the result actually emits.
- `crates/apps/grants-gov/src/lib.rs:342` — `upsert_many_with_provenance`;
  `crates/core/src/datasets.rs:314` — `UpsertSummary.removed`, populated only by `sync_many`.
- `crates/apps/grants-gov/src/lib.rs:20`, `:151`, `:353`, `:360` — the three-way default
  contradiction.
- `crates/apps/grants-gov/src/lib.rs:980-999`, `:1006-1047` — the `[]`-for-absent constructors;
  `:1204`, `:1226` — the tests pinning them; `:932-934` — honest-Null money for contrast.
- `crates/server/src/routes/meta.rs:404` — where `output_shape` reaches consumers.
- Correct siblings: `crates/apps/ca-grants/src/lib.rs:83-91`,
  `crates/apps/eu-sedia/src/lib.rs:117-131`.
- `docs/features/apps.md:25` — the grants-gov row; contains no `truncated` (cordis's row on `:27`
  documents its equivalent).

## Acceptance criteria

1. `output_shape` and the emitted result agree **in both directions** — no declared key that is
   not emitted, no emitted key that is not declared. Derive the list from the `json!` block and
   the merge sites (`cross.merge_into`, the `details` insert, the warnings tail), not from prose.
2. `removed?` is resolved as the structural impossibility it is: removed from the declaration, or
   the app starts producing removals — not left declared-and-dead. State which and why.
3. A test that pins declaration↔emission agreement so the next added key cannot drift. An
   inventory/EXPECTED-diff shape is the repo idiom (`crates/server/src/routes/mod.rs`); prefer it
   over a hand-listed assertion.
4. The harvest default is stated **once**, correctly, and the code matches. Decide whether the
   true default is on or off and make the absent-param path agree with the doc — a caller that
   omits the param should not silently get a different pipeline than the scheduler does.
5. `applicant_types` and `attachments` distinguish **absent** from **empty**, following the
   honest-Null rule the money fields in the same block already use. The two tests that currently
   pin the fabrication get updated to pin the honest behavior instead — *fix the code, then the
   assertion; do not just flip the assertion.*
6. **Rider** (you are already in the digest code): `closing_soon_digest` (`:649`) filters on
   `chrono::Utc::now().date_naive()`, while `is_past_due_open`/`deadline_end_utc`
   (`grants-common:1008-1016`, `:1732-1748`) judge the same rows at `D+1T12:00:00Z` anywhere-on-
   Earth. For ~12 hours a grant is open in `grants/unified` and `GET /grants/closing-soon` but
   absent from the job's own `closingSoon` digest. `grants-common:998-1004` names this exact class
   as a bug it already fixed — the digest was not brought along. Also `:653-656` treats a hit with
   **no** `oppStatus` as posted.
7. `docs/features/apps.md`'s grants-gov row reflects the corrected shape.

## Risks / non-goals

- **HARD WRITE-SET CONSTRAINT: do not edit `crates/server/src/registry.rs`.** Its publisher
  assertion (`:307`) is a substring check — `shape.contains("unified")` passes on the prose "counts
  unified rows" rather than on a declared `unified: {…}` block — and tightening it is genuinely
  worth doing, but that file drags `ONBOARDING.md` and `docs/features/runtime.md` in through the
  doc-sync map and a sibling builder owns those. **Report the exact edit; the Director applies it.**
  Note that criterion 1 will likely make grants-gov pass that assertion structurally for the first
  time — say so in your report.
- **Non-goal**: `crates/core/**` and `catalog/data-sources.toml` — a sibling direction in your own
  lot owns the catalog; coordinate within your lot, not across lots.
- Hazard: changing `[]` to null for `applicant_types` may ripple into `grants-common`'s
  normalizer. Trace the consumers (the scout found **zero** readers for both fields, which makes
  this cheap — verify that before relying on it).

## Build record

**Verdict: KEEP.** `8753553`. The `output_shape` fix is the least interesting part; the guard behind
it is the direction.

Prose cannot hold this line — the declaration and the `json!` block sit ~350 lines apart — so
`tests/result_contract.rs` **derives the emitted shape from a REAL run** and the declared shape from
the published string, and diffs them **in both directions**, one level deep. Verified non-vacuous:
re-introducing `hit_count` and `removed?` fails it with exactly those two names. That is the
difference between fixing a drift and closing the class.

The declaration now carries `sweep`, `truncated`, `detailCorpus`, `unified`, `index_datasets[]`,
`warnings[]` and the rest of the twelve omitted keys, and it states **why there is deliberately no
`removed`** — the listing goes through `upsert_many_with_provenance`, which never tombstones. A
consumer reading the contract now learns the same fact `6489ec2` acted on.

Three more truths in the same surface, each a real user-visible defect:

- **The harvest default is stated ONCE** (`HARVEST_DETAILS_DEFAULT`). The crate contradicted itself
  three times in one file — header and `params_schema` said ON, `// default OFF` sat above an
  `unwrap_or(false)` — so the runtime default was *false* and only `default_params` supplied true.
  A caller who built params by hand (**the documented way to narrow a pull**) silently ran a
  different pipeline from the scheduler's. The test pins `default_params()["harvestDetails"]` against
  the same constant, so the two cannot drift again.
- **Absent is not empty.** `applicant_types` and `attachment_manifest` answer `Null` when the source
  published no such field and `[]` only when it published an empty one — matching the honest-Null
  money rule in the same synopsis block, on the only dataset carrying federal eligibility at all.
  The tests that pinned the fabricated `[]` were corrected, which is what the direction asked for.
- **Two clock bugs found beyond the brief, and both are real.** `is_posted_hit` no longer reads an
  *absent* `oppStatus` as `posted` (under a wholesale rename the posted-only digest would have
  published the entire forecasted corpus as closing-soon alerts) — and refusing absence is made safe
  by `digest_status_drift`, which is loud when every hit is blind, because *"a quiet fortnight looks
  identical"*. And `closing_soon_digest` now judges at `grants_common::deadline_end_utc`, the same
  anywhere-on-Earth instant the unified sweep and `GET /grants/closing-soon` use: for ~12 hours a
  grant was open in `grants/unified` and absent from this job's own digest. `deadline_end_utc` was
  made `pub` **with the reason written into its doc** so the next surface does not re-derive it.
  `now` is now a parameter, so both boundary classes are testable without waiting for a clock.
