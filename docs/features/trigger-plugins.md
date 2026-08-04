# Trigger plugins (sandboxed WASM hooks)

A trigger edge can run **untrusted WebAssembly** at the moment it decides to
fire. Two hook slots, both optional, both attachable to any `source_kind`:

| Hook | Question it answers | Output contract |
|---|---|---|
| `predicate` | Should this hop fire at all? | `{"pass": bool}` (a bare `true`/`false` is accepted) |
| `transform` | What should the `_trigger` envelope look like? | a JSON **object**, which becomes the new envelope |

The predicate runs first; a veto short-circuits the transform entirely.

Two example plugins ship in-tree: `plugins-src/trigger-gate` (predicate — fire
only on batches of at least `min_count`, optionally only for one dataset) and
`plugins-src/delta-slim` (transform — keep only the named keys, or cap the
`keys` sample).

Implementation: `crates/server/src/triggers.rs` (`apply_plugin_hooks`,
`restamp_provenance`, `missing_hook_plugins`) over the host in
`crates/engine-wasm/`. The ABI and the sandbox itself are shared with
extraction plugins — see [extraction.md](extraction.md).

## Build and install

Hooks are configured by **name**, and the name is the file stem of a `.wasm`
module in `[plugins] dir` (default `data/plugins/`). A hook naming a module
that is not there does nothing (see *Failure semantics*), so installing is not
optional:

```bash
just plugins-install     # builds BOTH trigger plugins for wasm32 and installs them
```

The recipe fails with a `rustup target add wasm32-unknown-unknown` hint if the
target is missing. It installs under the **hyphenated crate name**
(`data/plugins/trigger-gate.wasm`), not cargo's underscored artifact name,
because the file stem is the name a trigger references.

`just plugin <crate>` only *builds* a plugin — it does not install it.

Hot swap without a restart: re-run `just plugins-install`, then
`POST /plugins/reload`. `GET /plugins` lists what is loaded, with each module's
`describe()` manifest (`kind: "predicate" | "transform" | "extractor"`).

## Configuration

Hooks are set at trigger create time and are immutable afterwards (there is no
update endpoint — a change is a delete plus a create):

```jsonc
POST /triggers
{
  "source_kind": "dataset",
  "source_app": "grants",
  "target_app": "extractor",
  "plugins": {
    "predicate": {
      "plugin": "trigger-gate",
      "params": { "min_count": 5, "dataset": "unified" },
      "on_error": "fire"          // "fire" (default) | "skip"
    },
    "transform": {
      "plugin": "delta-slim",
      "params": { "keep": ["dataset", "count"] }
    }
  }
}
```

Create-time validation: a non-empty `plugin` name, and `on_error` limited to
`fire | skip` and allowed on the **predicate only**. The named plugin need
**not** be loaded yet — hot reload is the point — so a typo is not caught here;
it surfaces at fire time as `plugin_missing`. An all-empty `plugins` object
stores as no hooks.

## The plugin's view

A hook receives the `extract_v2` envelope: `{"doc": <the _trigger object as a
JSON string>, "params": <the hook's params>}`. The `_trigger` object is the
same one that would be merged into the target job's params — its shape depends
on the source kind (dataset delta, terminal-job summary, or inbound ingress
event); see [triggers.md](triggers.md).

**Provenance is not the plugin's to write.** After a transform runs, the host
re-stamps `trigger_id`, `source_kind`, `source_job_id`, `event_id`,
`source_id`, `depth` and `chain` from the original envelope — and *removes* any
of those the original did not have. Cycle guards, depth limits and delivery
idempotency all read those keys, so a sandbox that could forge or drop them
could escape its own lineage. Everything else is the plugin's to reshape or
discard.

## Failure semantics — fail-open, always

A broken plugin must never wedge a pipeline edge. Every failure path proceeds:

| Situation | Predicate | Transform |
|---|---|---|
| Trap (`unreachable`, panic) | hop fires (`on_error` may flip it to skip) | original envelope kept |
| CPU fuel exhausted (`[plugins] fuel`) | hop fires | original envelope kept |
| Memory cap hit (`[plugins] max_memory_mb`) | hop fires | original envelope kept |
| Output is not valid JSON | hop fires | original envelope kept |
| Output violates the contract (no `pass` bool / not an object) | hop fires | original envelope kept |
| The named plugin is not loaded | hop fires, **ungated** | original envelope kept |
| Plugins disabled (`[plugins] enabled = false`) | hop fires, ungated | original envelope kept |

`on_error: "skip"` on the predicate is the explicit opt-in to failing **closed**
instead — it converts every predicate failure above into a skipped hop, recorded
as `predicate_veto`.

The last two rows are the dangerous ones, because "the gate passed" and "there
was no gate" produce the same job. So a **configured** hook whose plugin is not
loaded is recorded, per evaluation, as a `plugin_missing` row in the decision
ledger (`GET /triggers/{id}/runs` → `decisions`, `detail` = the plugin name)
alongside an error-level log. The hop still fires; the row is a note, not a
veto.

## Sandbox limits

Per call, from `[plugins]`: `fuel` (CPU instruction budget, default
200,000,000), `max_memory_mb` (default 64), and `max_concurrent` (default 0 =
one per core) capping how many plugin executions run at once **across the whole
host** — trigger hooks and extraction plugins share that admission gate. Each
call gets its own wasmtime `Store`, so no state survives between invocations;
only the module's linking is shared (pre-instantiated once at load). Plugins
declare no imports, so they have no filesystem or network access.

## Known gaps

- Hooks cannot be edited on an existing trigger (delete + create).
- No dry-run for a hook on its own; `POST /triggers/{id}/test` runs the hooks as
  part of a whole-trigger dry run, and external triggers cannot be dry-run at
  all.
- `plugin_missing` is recorded per evaluation, so a busy edge with a mis-typed
  plugin name writes one row per event until it is fixed.
- The concurrency gate is shared with extraction plugins and cannot be split.
- Only `predicate` and `transform` slots exist; there is no post-enqueue hook.
