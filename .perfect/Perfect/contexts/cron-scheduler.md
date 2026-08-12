---
name: cron-scheduler
type: perfect/context
group: Job Orchestration
category: lib
opportunity: 5
last_proposed: never
cooldown_until: —
directions: []
alias_of_old_map: "[[job-worker-cron-scheduler]] (round-2 pass covered scheduler.rs)"
---

## Current state
Not yet scouted on the 46-map. Files: crates/server/src/scheduler.rs, crates/server/src/
refresher.rs. Round 2 shipped cron tz + misfire policy + scheduled retries (c544db2).
Since then the tick piggybacks reaping/DLQ/DataHub polling (round-4 queue note) and
refresher.rs joined the shutdown lifecycle in r10 (c9c2c68). Tick contention and
schedule-vs-governance interplay unswept.

## Direction history
- (as job-worker-cron-scheduler, round 2): see [[job-worker-cron-scheduler]].

## Shipped
- (inherited — see [[job-worker-cron-scheduler]])
