---
name: engine-contracts
type: perfect/context
group: Core Platform
category: lib
opportunity: 5
last_proposed: 2026-08-13
cooldown_until: round 19
directions: ["[[archive-provenance-visible]]", "[[engine-conformance-suite]]", "[[onboarding-compiles]]"]
---

## Current state (scout brief, 2026-08-13 — "very thorough", read end to end)

**Five capability traits**, all `Send + Sync`, all `#[async_trait]`, all returning
`pumper_core::Result<T>`: `HttpClient` (`engine.rs:986`), `Browser` (`:1008`), `Researcher`
(`:1027`), `Plugins` (`plugin.rs:74`), `Search` (`search.rs:211`). `EngineSet` (`engine.rs:1041`)
holds http/browser/fetch public and **`claude` `pub(crate)`** (`:1044`) — a deliberate metering
guard. `Plugins` and `Search` are NOT in `EngineSet`; they hang off `AppContext.plugins` and
`AppState.search`.

**There is no capability-negotiation surface at all.** Grep for `capabilities|supports|negotiat`
returns only unrelated HTTP content-negotiation. Capability is expressed two implicit ways:
default-bodied methods that fail at call time, and request fields some implementors silently drop.

Key structural facts worth keeping:
- **`crates/core` depends on NO engine crate** (verified in `core/Cargo.toml`); `crates/server`
  depends on all seven (`server/Cargo.toml:15-21`). Any cross-engine conformance test must live
  in `server`. This constrains every "test all implementors" idea proposed here or later.
- Both existing guard rails (`fetch_chokepoint.rs`, `llm_chokepoint.rs`) police **consumers**.
  **No guard has ever policed an implementor.**
- `HttpRequest` is a union of every implementor's wishes; silent field-dropping is the de-facto
  design (`ArchiveEngine::fetch` drops `profile`, `proxy`, `etag`, `if_modified_since`).

### Verified dead / zero-consumer (do not build features on these)
- `FETCHED_VIA_HEADER` / `SNAPSHOT_TS_HEADER` (`engine.rs:78,81`): 1 writer, **ZERO readers**
  workspace-wide outside engine-archive's own tests. Structurally unreachable — `FetchOutcome`
  has no headers field. → became [[archive-provenance-visible]].
- **`HttpRequest.proxy`** (`engine.rs:140`): 2 readers (cache key, pool selection), **ZERO
  production writers**. Every assignment is `proxy: None` or a test. `FetchRequest` has no proxy
  field, so the tiered fetcher can never set one — yet `docs/features/fetching.md:66` documents it
  as a live per-request override, and half the bounded-client-pool machinery
  (`engine-http:186 pool_key(proxy, profile)`) exists for a knob nothing turns.
- `StepSummary` (`engine.rs:301`): destructured once in engine-browser, no external consumer.
- `PluginRunStats` is not re-exported from `lib.rs`; its 4 consumers use the full path —
  inconsistent with every other engine type.

### Verified reachable (a scout previously might have mis-flagged these — they are fine)
`interaction_outcome`, `pass_fully_succeeded`, `summarize_steps`, `transact_probe_js`,
`parse_transact_probe`, `redact_field`, `is_sensitive_input`, `require_existing_profile`,
`CapturedCall`, `lru_touch*`, `lcg_fraction`, `Search::index_stats`, `Plugins::run_metered`.

### Docs that lie (grepped for implementing code, per the repo's highest-value defect class)
- `ONBOARDING.md:345-353` + `:261` teach `ctx.engines.claude.research(...)` — cannot compile
  (`pub(crate)`) and is banned by `llm_chokepoint.rs:169`. → [[onboarding-compiles]]
- `ONBOARDING.md:266` wrong arity for `ctx.plugins.run`; `:224-231` shows 5 of 7 `ScrapeApp`
  methods, omitting the load-bearing `manifest()`.
- `docs/features/fetching.md:3` says "three engines"; there are **five** tiers.
- `:86` misdescribes `RenderedPage` (claims serde defaults on a `Serialize`-only type) and omits
  three fields; `:82` and `:7` each omit fields.
- `engine-archive` and `engine-remote` have **no feature doc and no `feature-doc-map.json`
  entry**; `ONBOARDING.md` is the target of no map entry at all — the mechanical reason the above
  survived. (Compounded by [[doc-sync-hook-fires]]: the hook never ran.)

### Untested
No shared conformance suite anywhere. `engine-archive` and `engine-remote` have no `tests/` dir.
**`Browser::render` — the only required method of the `Browser` trait — has ZERO CI coverage**:
all four tests in `engine-browser/tests/render.rs` are `#[ignore]`d and `just ci` does not pass
`--ignored`. `crates/core/src/search.rs` has 0 tests.

## Direction history

### Round 17 (2026-08-13) — gate: director-self-gated (autonomous, Athena-dispatched)
**ACCEPTED 3:**
- [[archive-provenance-visible]] — a documented contract (`engine.rs:78`) with literally zero
  implementing code, on the tier whose entire purpose is a freshness/availability trade the
  consumer must be able to see. Director-verified both halves (no headers field; zero readers).
- [[engine-conformance-suite]] — kills a class rather than an instance, and carries a sharp
  concrete bug: `Browser::transact`'s default returns retryable `Error::Browser` while
  `is_terminal_for_job` (`error.rs:311-313`) covers only `BudgetExhausted | Transact`, so an
  unsupported flow burns a job's whole backoff ladder. Director-verified.
- [[onboarding-compiles]] — the agent-facing contract teaches three consecutive wrong snippets,
  and the most prominent one teaches the exact metering bypass the repo guards against twice.

**REJECTED, with reasoning:**
- *`Error::Http` flattening feeds false strikes to the learned tier router* — the scout ranked
  this #5 and I **refuted its causal chain myself**. The claim was that a peer-node outage strikes
  the target host. It cannot: `RemoteEngine::fetch` catches every `try_node` failure, warns, and
  **falls back to local** (`engine-remote/src/lib.rs:176-190`), so the peer error never reaches
  the fetcher. And an archive miss is traced as `FetchTier::Archive`, while `app.rs:388-397`
  strikes only on `FetchTier::Http`. What survives is much narrower (an over-cap body counts as an
  HTTP loss). **Rejected as proposed; the residue is too thin for a slot.** This is the decay rule
  paying again — a banked claim that shrank rather than grew.
- *`Browser::render` has zero CI coverage* — real, severe, and **not this context's**. It is
  browser-engine's coverage gap. Banked as [[browser-engine]]'s anchor with the fix shape already
  known (an offline harness over a local axum server, precedent `engine-http/tests/profiles.rs`).
  Proposing it here would have been misattribution.
- *`HttpRequest.proxy` is a dead knob documented as live* — genuine, but thin, and the r9/r15
  precedent cuts against spending a slot on a zero-consumer surface. Banked.
- *Remote egress is unobservable* (no field says which node served a fetch; every peer failure
  silently degrades to local egress) — a real product-claim gap, but it belongs to
  [[remote-engine]], and [[archive-provenance-visible]] explicitly non-goals it to keep the write
  set honest. Banked there as that context's anchor.

## Shipped
- (rounds 9–10, incidental via other contexts): fetch chokepoint `6237cc8`, `Error::Transact`
  terminality `8e17ca7`, core prelude `684d2c7`, error-code contract `0cfc366`.
- Round 17: (in flight)
