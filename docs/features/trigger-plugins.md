# Trigger plugins (sandboxed WASM hooks)

A trigger edge can run **untrusted WebAssembly** at the moment it decides to
fire. Two hook slots, both optional, both attachable to any `source_kind`:

| Hook | Question it answers | Output contract |
|---|---|---|
| `predicate` | Should this hop fire at all? | `{"pass": bool}` (a bare `true`/`false` is accepted) |
| `transform` | What should the `_trigger` envelope look like? | a JSON **object**, which becomes the new envelope |

The predicate runs first; a veto short-circuits the transform entirely.

Two example plugins ship in-tree: `plugins-src/trigger-gate` (predicate — fire
only on batches of at least `min_count`, optionally only for one dataset; both
rules read fields only **dataset** hops carry, so on job and external hops they
sit out and the hop passes, with `reason` saying so) and
`plugins-src/delta-slim` (transform — keep only the named keys; host-owned keys
come back regardless).

**Both are built and verified by CI.** They live in detached workspaces targeting
`wasm32-unknown-unknown`, so `cargo test --workspace` never compiled them and a
break went fail-open in production rather than red in CI — a gate nobody deployed
reads exactly like a gate that said yes. CI now runs `just plugins-install`,
`just plugins-test` and the artifact tests (`just plugins-verify` locally). Note a
build alone is not enough: deleting `#[no_mangle]` from an export still compiles
clean for wasm32, so `every_shipped_plugin_still_exports_the_host_abi` checks the
declared ABI from source in the ordinary suite, and the artifact tests ask the
host whether each installed module is actually executable.

Implementation: `crates/server/src/triggers.rs` (`apply_plugin_hooks`,
`restamp_host_owned`, `missing_hook_plugins`) over the host in
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

`keep` shapes the **payload**, never the target's work scope: `keys` and
`keys_truncated` are re-added by the host afterwards. (This example used to be a
live footgun — pairing `target_app: extractor` with a keep-list that omits `keys`
dropped the work list, and the extractor's fallback is a full 10,000-record
sweep, so a 3-record incremental extract became a full sweep. The host now
overrules that.)

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

**Provenance and work scope are not the plugin's to write.** After a transform
runs, the host re-stamps two classes of key from the original envelope — and
*removes* any the original did not have:

- **Lineage** — `trigger_id`, `source_kind`, `source_job_id`, `event_id`,
  `source_id`, `depth`, `chain`. Cycle guards, depth limits and delivery
  idempotency all read these, so a sandbox that could forge or drop them could
  escape its own lineage.
- **Work scope** — `keys`, `keys_truncated`. `extractor` and `plugin` read
  `_trigger.keys` as their **work list**, not as a sample, so a transform could
  rescope the target's *work* rather than merely its payload — in both
  directions. An *absent* `keys` is not a smaller payload: it means "every live
  record, up to `SOURCE_LIST_LIMIT` (10,000)". The key throttle is
  `[triggers] key_cap`, which is the host's knob and stays the host's.

Shrinking what a **webhook** carries is legitimate; shrinking what a **job does**
is not. A transform that tries is overruled, and the overridden keys are named in
a `warn!` so the plugin author and the operator both find out, rather than having
to diff two JSON blobs to notice. Everything else is the plugin's to reshape or
discard.

## Failure semantics — fail-open, always

A broken plugin must never wedge a pipeline edge. Every failure path proceeds —
**and every one of them is now recorded**, with its own outcome, in the decision
ledger (`GET /triggers/{id}/runs` → `decisions`). Before this, all of them were a
`warn!` and nothing else: "why did my trigger fire without being gated?" was
unanswerable from the very API that exists to answer it.

| Situation | Predicate | Transform | Ledger outcome |
|---|---|---|---|
| Trap (`unreachable`, panic) | hop fires (`on_error` may flip it to skip) | original envelope kept | `hook_trap` |
| CPU fuel exhausted (`[plugins] fuel`) | hop fires | original envelope kept | `hook_trap` |
| Memory cap hit (`[plugins] max_memory_mb`) | hop fires | original envelope kept | `hook_trap` |
| Output is not valid JSON | hop fires | original envelope kept | `hook_malformed` |
| Output violates the contract (no `pass` bool / not an object) | hop fires | original envelope kept | `hook_malformed` |
| The named plugin is not loaded | hop fires, **ungated** | original envelope kept | `plugin_missing` |
| Plugins disabled (`[plugins] enabled = false`) | hop fires, ungated | original envelope kept | `plugin_missing` |
| The module is loaded but exports no `extract`/`extract_v2` ABI | hop fires, **ungated** | original envelope kept | `hook_not_executable` |
| The host itself broke around the call | hop fires | original envelope kept | `hook_host_error` |
| Predicate ran and answered `pass=false` | hop skipped | not reached | `predicate_veto` |

`detail` on every failure row names the slot, the plugin and the consequence
(`… — on_error=fire, hop NOT gated` / `… — on_error=skip, hop stopped` /
`… — original envelope kept, hop NOT shaped`).

`on_error: "skip"` on the predicate is the explicit opt-in to failing **closed**
instead — it converts every predicate failure above into a skipped hop. The row
keeps the *failure's* own outcome rather than borrowing `predicate_veto`: a
sandbox that crashed and a gate that said no are different facts, and an operator
counting vetoes must not be shown a crash as a healthy decision. `predicate_veto`
now means exactly one thing — a predicate that **ran and answered** `pass=false`.

The un-gated rows are the dangerous ones, because "the gate passed" and "there
was no gate" produce the same job. Those are also facts about the DEPLOYMENT
rather than about the event, so they are recorded **once per (trigger, plugin)**
and then suppressed — `POST /plugins/reload` (the only thing that can change the
answer) re-arms them. The error-level log fires on every evaluation regardless.

`GET /plugins` marks each entry `executable: true|false`, and `has()`/`list()`
answer for executability, so a module without the extract ABI is never offered as
a usable hook plugin. Loading stays permissive — describe-only modules must keep
loading for dynamic-app discovery.

Hook calls are metered like any other plugin call, so a predicate's fuel and
memory cost shows up in that plugin's `GET /plugins` `telemetry` block (see
[extraction.md](extraction.md)). The hook path itself surfaces no per-hop cost:
a trigger decision is not a place to price a plugin, and a per-hop cost field
would ride into target job params.

## Sandbox limits

Per call, from `[plugins]`: `fuel` (CPU instruction budget, default
200,000,000), `max_memory_mb` (default 64), and `max_concurrent` (default 0 =
one per core) capping how many plugin executions run at once **across the whole
host** — trigger hooks and extraction plugins share that admission gate. Each
call gets its own wasmtime `Store`, so no state survives between invocations;
only the module's linking is shared (pre-instantiated once at load). Plugins
declare no imports, so they have no filesystem or network access.

The admission permit belongs to the running work, not to the caller waiting on
it. Wasm executes on an uncancellable blocking thread, so abandoning the call (a
worker timeout, a lost `select!` race) does **not** free the slot — it is
released when the store is actually torn down. `max_concurrent × max_memory_mb`
therefore stays a real ceiling under cancellation.

Failures are reported with a typed class rather than a formatted string —
`trap`, `malformed_output`, `missing_export`, `unknown_plugin`,
`plugins_disabled`, `host_error` — so the ledger and the observatory classify on
the type and a reworded message cannot silently reclassify stored rows. See
[extraction.md](extraction.md) for the shared sandbox contract.

## Known gaps

- Hooks cannot be edited on an existing trigger (delete + create).
- No dry-run for a hook on its own; `POST /triggers/{id}/test` runs the hooks as
  part of a whole-trigger dry run (reporting `hooks.unusable_plugins` and
  `hooks.incidents`), and external triggers cannot be dry-run at all.
- The once-per-(trigger, plugin) bound on `plugin_missing` /
  `hook_not_executable` is **per process**: it is re-armed by a reload or a
  restart, not by a timer, so a long-lived server records the fault once and
  the ledger will not repeat it until something changes.
- `executable` is decided from export **names**, not signatures. A module
  exporting an `alloc` of the wrong type still lists as executable and fails per
  call with `hook_not_executable` instead.
- The concurrency gate is shared with extraction plugins and cannot be split.
- Only `predicate` and `transform` slots exist; there is no post-enqueue hook.
