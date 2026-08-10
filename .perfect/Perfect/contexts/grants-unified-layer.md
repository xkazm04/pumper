---
name: grants-unified-layer
type: perfect/context
group: Grants Intelligence
category: lib
opportunity: 7
last_proposed: 2026-08-04
cooldown_until: —
directions: ["[[unified-health-inheritance]]", "[[coalesce-unified-finalize]]", "[[federal-award-amounts]]", "[[close-date-timezone-honesty]]", "[[grant-recurrence-relation]]"]
---

## Current state (scout brief, 2026-08-04 — engine level, file:line verified)

**BANKED SEEDS WERE STALE — corrected.** The round-3/4 note claiming "sweep_closed O(n) full read +
link_duplicates O(n²)" is **no longer true on either count**: `bee7854` narrowed the sweep to
open/forecasted rows only, and `51ce092` (round 4) gave `duplicate_pairs` a banded index which
`link_duplicates` inherits with **zero changes in this crate**. Opportunity re-scored 8 → 7 and the
slate was rebuilt from live evidence. (Third time a banked seed has decayed — see config.md log.)

**Normalization** all lives in `grants-common/src/lib.rs`, not in the source apps: `normalize_grants_gov`
(:133-158), `normalize_ca_grants` (:169-197), `normalize_eu_sedia` (:212-238). Canonical key
`<source>:<source_id>`. Every mapper is **null-honest** — money returns `Null` on prose or $0
(:748-793), `parse_date` returns `None` on anything unrecognized (:801-817), and a record missing its
identifying key is dropped rather than upserted with a fabricated id (:924-927).

**cordis is NOT a producer** of `grants/unified` despite being framed as one — zero `grants_common`
references in `crates/apps/cordis/src/lib.rs`. It maintains `cordis/projects` + `cordis/topic_stats`
and donates a history block into open SEDIA topics before eu-sedia's own upsert.

**Lifecycle events** (`grants/events`): `DeadlineExtended|Accelerated`, `ForecastPosted`, `AwardRaised`,
`Reopened`, `ClosedEarly` (:329-357). `classify_events` is pure and well-tested (:389-460, tests
:1064-1202); `record_events` costs O(change set) by construction — 2 revisions per changed key
(:467-515). Runs BEFORE the sweep deliberately, so "prior" is the source's state, not our inference.

**Rough (all now proposed):**
- **Unified writes bypass health entirely** — `sync_unified` (:309-323) calls `upsert_many_stamped`
  directly, skipping `write_target` (`app.rs:635-648`), which only gates the source's OWN dataset.
  The index gate on the virtual pair `("grants","unified")` is structurally a no-op.
- **Three producers each run the full-corpus passes** — `sweep_closed` (2 unindexed `json_extract`
  scans, `limit 1_000_000`) + `link_duplicates` (full live key+simhash read) once per producer job;
  the code flags dedup as future work (:534-535).
- **`min_award` can never match a federal record** — Search2 hits carry no amounts (:131-132,143-145).
- **Dates are calendar-truncated and swept against UTC** (:801-817, :527, :578-585) — a grant closing
  23:59 local can be retired while still open, and `closing-soon` hardcodes `status="open"`
  (`query.rs:194-197`) so it vanishes.
- **Duplicate links can't tell "same program next year" from "same program other portal"** (:57,630).
- Status vocabularies split across two code paths that must agree (`norm_status` :826-836 vs
  `sedia_status` :270-276 numeric codes) — a 4th source risks leaking a raw code into `status`.

**Tests:** rich pure/unit coverage of every mapper, money, dates, sweep predicate and all event
transitions (:838-1220). **No `#[tokio::test]` anywhere in the crate** — `record_events`,
`sweep_closed`, `link_duplicates` have no integration coverage against a real store, and nothing
tests the 3×/day multi-producer race. (Banked seed for a later round.)

**Dead ends:** NY/TX/IL/OH are catalog rows with `app = ""` and no crate; `grants-gov-xml` bulk
source declared but unbuilt (and previously rejected at the gate).

## Direction history
- 2026-08-04 (round 5): 5 proposed, **5/5 accepted**, zero rejections. Slate rebuilt after both
  banked seeds were falsified against live code.

## Shipped
(pending build)
