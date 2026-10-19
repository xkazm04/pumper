# Wave 3 — Events, Consumers, Research (2026-09-02)

Five builders, five worktrees, all five items merged into `master`. Each builder reported the five
cargo gates green in its worktree; the coordinator ran `cargo check` after each merge and the full
gate set after the last (§Verification).

## Merges (in landing order)

| Item | Branch | Builder commits | Notes |
| --- | --- | --- | --- |
| N25 Research as a living KB | `moonshot/n25-research-kb` | 4 | no migration; clean merge |
| N24 Vendor-neutral lineage | `moonshot/n24-openlineage` | 6bb24e3, d84f404, ab6f968, 46dcd9d | no migration; `config.rs` both sections kept |
| N11 Index-time enricher hook (+ doc-map carry-forward) | `moonshot/n11-enricher` | 8 | no migration; clean merge |
| N04 Param binding + fan-out + 5 MCP tools | `moonshot/n04-param-binding` | 848b07b … 6ee39b4 (5) | migration **0049**; clean merge |
| N05 Durable event log + subscriptions (+ both K carry-forwards) | `moonshot/n05-event-log` | db734a6, 9539955, 20f7f21, 441bb2c | migration renumbered 0049→**0050**; `config.rs` both sections kept |

Master migrations now end at **0050**.

## What master can do now

- **Event log ON by default** (`[events] log_enabled = true`): every bus event is a row, `seq`
  survives restarts, SSE/MCP-live resume from the table with zero `reset`, `GET /events/log`
  keyset page, `subscriptions` (selector → sink, cursor advanced only on `delivered`, `plugin:`
  sinks included), watches ride the same outbox as a thin adapter, `@pumper/sync` gains
  `subscribe(cursor)`. `transaction.pending|submitted` and `source.repair_promoted|rolled_back`
  are real event kinds. `Storage::upsert_managed_schedule` replaced the raw `sqlx` peer upsert.
- Triggers: `bind` (JSON pointers from the event into target params), `each` (per-element fan-out,
  `fan_out_cap` 50, truncation stated), ledger outcomes `bind_miss` / `fan_out_empty`, dry-run
  shows the plan; five MCP authoring tools (`create_trigger`, `test_trigger`, `trigger_decisions`,
  `create_watch`, `create_ingress_source`) behind `allow_enqueue`; `fetch` still last.
- Search: `[search] enrichers = ["builtin", "plugin:<name>"]`; one stored `entities` JSON field
  so a new entity KIND no longer wipes the index; `search-backfill --re-enrich`; reference
  `enrich-money-date` core-module plugin built and ABI-verified. **One last schema bump** — an
  existing index is rebuilt once through the documented drift recovery.
- Lineage: `LineageEvent` model with DataHub (`From<&LineageEvent>`, byte-identical, pinned) and
  OpenLineage writers; `[lineage] openlineage_url` (COMPLETE/FAIL; START not wired), `emit_sources`
  (catalog sources as `web`-platform upstreams), `emit_quality` (contract verdicts → assertions,
  health + trust → tags). `GET /datahub/status.lineage`.
- Research: `research/findings` + `research/sources` with provenance, `snapshot_sources`
  (metered `ctx.fetch`, `artifact_sha` only when a body landed), `watch_sources` emits
  ready-to-POST schedule bodies, `topic` keys the knowledge base across follow-ups.

## Findings worth keeping

- **Additive column changes need a round-trip test, not a compile** (N04): the SELECT list gained
  `bind`/`each_path` but the INSERT did not; sqlx silently discarded the extra binds and every
  trigger read back `bind: None` — indistinguishable from the old behaviour until an e2e drove a
  real ingress event through storage.
- **An empty replay ring reported `Events([])`, not `Reset`** (N05): a resume across a restart
  returned nothing, silently. Fixed; the ring is trusted only when its first buffered event is
  exactly `after + 1`.
- **Two more load-sensitive tests** seen once under five concurrent builders and green in
  isolation: `app-crawl::tests::an_abandoned_crawl_has_already_committed_what_it_learned` (25 ms
  cadence) and `search-backfill::tests::rerunning_backfill_after_a_tombstone_removes_the_ghost`
  (Windows file-lock denial on a Tantivy commit). Flake-register candidates alongside
  `fanout_offslot`.

## Carry-forward seams (not built by any builder)

1. **`[research] max_watched_sources` is not plumbed** — `registry::apps()` takes no `&Config`.
   → wave 4 builder P (app config plumbing).
2. **App-declared schedules** — `watch_sources` proposes bodies instead of creating schedules.
   → wave 4 builder P (`AppContext::request_schedule`).
3. **Event-log retention prunes from the outbox drain, hourly-gated**, because `store_janitor`
   in `main.rs` returns early unless one of its own knobs is on. Moving it into the janitor is a
   `main.rs` change. → wave 4 P.
4. **OpenLineage START not wired** (no emitter call site at job start; adding one puts a network
   post on the permit path). Documented; not scheduled.
5. **`trigger-plugins.md` cross-reference** for shaping-vs-binding (N04 left it for M; M did not
   know). → wave 4 P, one paragraph.
6. **`scripts/docs/feature-doc-map.json` has no entry for `docs/features/ingress.md`** (pre-existing
   gap, `routes/ingress.rs` maps to nothing). → wave 4 P.
7. **Write amplification of the event log unmeasured** (`job.progress` every ~2 s is logged like
   everything else). Stated in the doc.
8. **Superseded research findings are not removed** (a run with 3 findings for a topic that had 5
   leaves `#3`/`#4`) — `sync_many` is dataset-wide. Stated in the doc.

## Verification on master

After the fifth merge (N05), on `master`, each in its own invocation:

| gate | result |
| --- | --- |
| `cargo fmt --all -- --check` | exit 0 |
| `cargo check --workspace` | exit 0 |
| `cargo clippy --workspace --all-targets -- -D warnings` | exit 0 |
| `cargo test --workspace` | exit 0, no FAILED/panicked |

Not run: `just ci` and `scripts/smoke.ps1` (campaign close).
