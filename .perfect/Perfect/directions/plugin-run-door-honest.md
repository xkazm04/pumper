---
slug: plugin-run-door-honest
type: perfect/direction
context: "[[plugin-runner]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-13
accepted: 2026-08-13
shipped: 2026-08-13
commit: 7b6d5f1
---

## What & why

A plugin job that fails 100% of the time reports **SUCCEEDED**, and the repo's only
`run()`-level test proves it.

`run()` validates the plugin name with `ctx.require_str("plugin")?` (`lib.rs:481`) — which
is `get(key).and_then(Value::as_str)` (`app.rs:526-531`) and nothing more. No
`ctx.plugins.has()`. So a typo, an uninstalled build, or `[plugins] enabled = false`
produces one `{"error": "unknown plugin 'x'"}` per URL, `ran: 0`, zero dataset writes, and
`Ok(...)`. The user sees a green job on `GET /jobs`, a `succeeded` SSE event, a fired
result webhook, and an empty dataset.

**Observatory mode already does it right** — `observatory.rs:259-264` calls
`ctx.plugins.list()` and refuses an unknown name. So does the trigger pipeline
(`triggers.rs:237` filters on `plugins.has`). The asymmetry between two modes of the *same
app* is the fix's shape, and since r14 the check has been centralized to **one** call site
(`lib.rs:481`, before mode dispatch at `:492-498`) — the banked note said three; it is now
one line.

The proof that nothing guards this is in the test suite: `app_fetch_chokepoint.rs:86`
builds `{"plugin": "noop", …}`, `testing.rs:356` wires `plugins: Arc::new(NoPlugins)`,
`plugin.rs:143-153` makes every call `Err(PluginFailure::Disabled)` — and
`Plugin.run(ctx).await.unwrap()` at `:185` and `:236` **passes green on a run where every
single document failed.** Any assertion of `ran > 0` would have caught this the day it was
written.

Underneath sits a second, deeper loss. Round 14 gave the host typed failures
(`PluginFailure` at `core/src/error.rs:95`, `run_metered`), and observatory classifies on
that type with a good anti-regression test (`observatory.rs:66-87`, `:571`). The app throws
the type away: `lib.rs:574` and `:820` flatten `Unknown | Disabled | Trap |
MalformedOutput` into one untyped `json!({"error": e.to_string()})` string. That is also
why `engine-wasm/src/lib.rs:25-27`'s contract — "extraction propagates the error" — is
false for the app that *is* extraction.

## Evidence

- `crates/apps/plugin/src/lib.rs:481` — the whole door
- `crates/core/src/app.rs:526-531` — `require_str` is a type check, nothing more
- `crates/apps/plugin/src/observatory.rs:259-264` — the same app, validating correctly
- `crates/server/src/triggers.rs:237` — the repo's other consumer, using `plugins.has`
- `crates/apps/plugin/src/lib.rs:574`, `:820` — typed failure flattened to a string
- `crates/apps/plugin/src/lib.rs:592`, `:774`, `:936` — `ran` counts results *lacking* an `error` key
- `crates/core/src/error.rs:95` — `PluginFailure`, the type being discarded
- `crates/engine-wasm/src/lib.rs:25-27` — "extraction propagates the error" (false here)
- `crates/engine-wasm/src/lib.rs:412-414` — `has()` answers *executability*, so a describe-only module is refused too
- `crates/server/src/e2e/app_fetch_chokepoint.rs:86, 185, 236` + `crates/core/src/testing.rs:356` + `crates/core/src/plugin.rs:143-153` — the green test on a total-failure run

## Acceptance criteria

1. An unknown / unloadable plugin name is refused **before any fetch happens**, with an
   error naming the plugin and pointing at `GET /plugins`. Use `ctx.plugins.has()` — verify
   it answers executability (`engine-wasm/src/lib.rs:412-414`) so a describe-only module
   is also refused.
2. The refusal is classified so the job does not burn its retry ladder on a deterministic
   configuration error (`is_terminal_for_job`, `core/src/error.rs:337-342`). Check which
   variant fits rather than widening one — r18's profile fix is the precedent for auditing
   construction sites before widening.
3. The typed `PluginFailure` survives out of the fan-out instead of being stringified.
   Carry `Result<Value, _>` (or an equivalent) through `run_plugin_batch` so the result can
   report failures **by class** (trap / disabled / malformed-output / unknown), not as one
   opaque string. Observatory's `classify_outcome` (`observatory.rs:66-87`) is the shape.
4. **Decide the per-document failure policy explicitly and say why**: does a run where
   every document failed still succeed? A reasonable answer is "partial failure is fine,
   total failure is not" — but pick it deliberately, state it in a doc comment, and make
   the job status match. Do not silently keep today's behavior.
5. `crates/server/src/e2e/app_fetch_chokepoint.rs` no longer passes on a 100%-failure run.
   Its fetch-chokepoint and cost-event-per-URL assertions are **load-bearing and must keep
   working** (`fetch_chokepoint.rs` pins them) — so either give it a host that actually
   runs, or assert the new refusal. Whichever you pick, the metering invariants stay green.
6. The first `run()`-level test in `crates/apps/plugin/tests/` (the directory does not
   exist yet), named after the anti-pattern it defends.

## Risks / non-goals

- The `{"error": …}` records never reach the dataset — `upsert_items` drops them
  (`lib.rs:1000`). Do not "fix" a leak that isn't there; the leak is into the *result echo*,
  and that is [[plugin-result-bounded-and-true]]'s job. Coordinate, don't overlap.
- Non-goal: changing the WASM host. If a fix genuinely needs `crates/engine-wasm/`, that is
  out of your write set — report it.
- A plugin's own legitimate `{"error": "no <title> found"}` output is **data**, not a
  failure. Today the code cannot tell the difference; your typed path must not make that
  confusion worse. (Untangling the two counts is [[plugin-result-bounded-and-true]]'s
  criterion — make sure your seam permits it.)

## Build record

**Shipped `7b6d5f1`. Director verdict: KEEP.** All six criteria met.

Criterion 1: `require_runnable_plugin` refuses via `ctx.plugins.has()` **before any fetch, dataset
read or `register_rules` write**. `unloadable_plugin_error` distinguishes "typo among these" from
"nothing loaded at all → check `[plugins] enabled` / `just plugins-install`". Confirmed en route
that `has()` really does answer executability (`engine-wasm/src/lib.rs:412` filters on the same
`executable` flag `list()` uses), so a describe-only module is refused too.

Criterion 2: `Error::BadRequest` — and **the audit this note demanded found the reason not to
widen**: widening `Error::Plugin` would have made `trap` terminal. (Also `core/src/error.rs` was
the sibling lot's file this round, so a widening would have straddled the partition.)

Criterion 3: `DocOutcome = Result<Value, DocError>` carries
`DocFailure::{Fetch, EmptyDocument, Plugin(PluginFailure)}` through `run_plugin_batch` into all
three modes. The result gains `errors`, `errors_by_class` (**absent classes omitted, never
zeroed** — a zero is a claim), and `plugin_reported_errors`, which is what finally separates a
plugin's own legitimate `{"error": "no <title> found"}` *data* from a call that failed. Each
echoed failure carries `error_class`.

Criterion 4 — **policy decided explicitly and documented on the predicate**: partial failure
succeeds, **total failure fails**, via `every_document_failed` + a *retryable* `Error::App` naming
the per-class counts (a site being down is transient, unlike the door refusal). A run that
attempted nothing is a quiet success.

Criterion 5–6, and the part the Director would have missed: the builder gave
`app_fetch_chokepoint.rs` a real `EchoPlugins` host and asserted `ran == 2 / errors == 0` — then
**found the same passes-on-a-run-where-nothing-happened shape in the extractor arm of the same
file** and pinned `fetched == 2` there too. The metering and cost-event-per-URL invariants stayed
green throughout (`crates/core/tests/fetch_chokepoint.rs` 4/4, untouched). First `run()`-level
tests in the app: `tests/run_door.rs`, 9 tests, plus `tests/common/mod.rs` harness.

Gates: `cargo test -p app-plugin` 61/0 (42 lib + 9 run_door + 6 + 4);
`cargo test -p pumper-server app_fetch_chokepoint` 4/0.

**Builder note carried forward**: `TestContext` hard-wires `NoPlugins` and has no seam to override
it (`core/src/testing.rs`, outside the write set); worked around via the public
`AppContext::plugins` field. A `TestContext::plugins()` builder is **banked** as a follow-up.
