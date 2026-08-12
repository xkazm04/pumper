---
slug: wasm-ledger-honesty
type: perfect/direction
context: "[[wasm-plugin-host]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-12
accepted: 2026-08-12
shipped: 2026-08-12
commit: 10fa27d
---

## What & why
"Why didn't my trigger fire" — the exact question the r6 decision ledger was built to
answer — is unanswerable for every sandbox failure class. A plugin that traps, burns
its fuel, emits garbage, or lacks the extract ABI lets the hop fire ungated with only
a warn!; the ledger cannot even REPRESENT those outcomes. Worse, a crashed predicate
under `on_error: skip` records as an ordinary `predicate_veto`, so the operator sees
a healthy gate decision for a crashed sandbox. And the dry-run endpoint reports
`would_fire: true` for a trigger gated by an uninstalled plugin. This direction makes
every hook failure class truthful in the ledger, in `has()`, and in dry-run.

## Evidence
- apply_plugin_hooks takes `&dyn Plugins, trigger, obj` — no storage handle;
  structurally cannot write rows. crates/server/src/triggers.rs:240-298 (all four
  failure classes warn!-only: :258-266, :268-277, :283-287, :288-292).
- Outcome allowlist has no trap/fuel/malformed outcome — crates/core/src/storage.rs:2830-2866.
- predicate_veto conflation: triggers.rs:920, :1050 + storage.rs:2842-2843 comment.
- has() lies: load_dir validates compile+imports only (engine-wasm/src/lib.rs:190-224);
  fixture extract_only.wasm (:467-471) proves a no-ABI module answers has()=true.
- Dry-run lies both ways: routes/triggers.rs:340-458 never calls missing_hook_plugins;
  on_error:skip branch fabricates "returned pass=false" (:427).
- plugin_missing amplification: state.rs:289-293 (NoPlugins when enabled=false) +
  docs/features/trigger-plugins.md:132-133 (one row per event, unbounded).

## Acceptance criteria
1. Every hook failure class (trap/fuel-exhaustion, malformed output, missing export,
   unknown plugin) produces a trigger-runs row with a truthful, distinct, allowlisted
   outcome (detail names plugin + class). apply_plugin_hooks stays pure/testable:
   return hook incidents for callers to record (extracted-function doctrine), don't
   thread storage into it.
2. Error-under-`on_error: skip` is distinguishable from a genuine pass=false in the
   ledger (own outcome, or veto whose detail names the error — builder picks, says why).
3. has()/list()/GET /plugins reflect executability: a module lacking the extract ABI
   is not reported as a usable hook plugin, while describe-only dynamic-app modules
   still list for discovery (load stays permissive). report_missing_plugins and
   dry-run see the same truth.
4. POST /triggers/{id}/test stops lying both ways: names missing/non-executable
   plugins and reports hook errors honestly instead of would_fire:true or a
   fabricated pass=false reason.
5. plugin_missing row-per-event amplification bounded (dedup/once-per-state-change or
   an explicit documented bound); if a real bound proves out of scope, say so in the
   report and update the known-gaps doc honestly instead.
6. e2e extends crates/server/src/e2e/trigger_plugins.rs (LAYER ON the 485-line real-
   host harness, never fork a parallel one); docs updated: trigger-plugins.md failure
   table + known gaps, triggers.md outcome list.

## Risks / non-goals
- Non-goal: changing fail-open semantics. Hops still fire on hook failure unless
  on_error says otherwise — this is honesty, not behavior change.
- Risk: new allowlisted outcomes must not break the EXPECTED-diff inventory
  conventions; extend the allowlist test deliberately.
- Non-goal: the plugin APP's door (typo'd plugin ⇒ green job) — that is
  plugin-runner's anchor, banked there.

## Build record
Shipped 10fa27d (Lot W, opus). HookVerdict{obj, incidents} — apply_plugin_hooks
stays pure, callers record; 4 new outcomes (hook_trap/hook_malformed/
hook_not_executable/hook_host_error) classified from the typed PluginFailure.
ACCEPTED DEVIATION (better than both offered options): error-under-skip keeps its
OWN failure outcome with the consequence in detail and emits NO predicate_veto —
conflation structurally impossible, veto means exactly one thing. executable flag:
load stays permissive, has()/list() answer executability, manifests carry the
flag. Dry-run reports hooks.{unusable_plugins, incidents} both on stop and fire.
plugin_missing/hook_not_executable deduped once per (trigger,plugin,outcome),
re-armed by POST /plugins/reload (known-gap row amplification closed). Builder
refutations: report_missing_plugins row moved to the incident path (double-record
avoided); no allowlist test EXISTED — wrote the EXPECTED-diff one. e2e layered on
the existing harness (+5 tests incl. once-not-per-event). First-run test failure
fixed in code not assertion (detail-wording parity). Review: KEEP.
