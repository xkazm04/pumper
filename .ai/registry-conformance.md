# Registry conformance — software-engineering

contributor: mkdol-dev-box · audited: 2026-08-24 · bundle: `software-engineering`
(`ai-registry/knowledge/software-engineering/index.json`)

Fourteen subjects selected against pumper's real surfaces — the scraping domain
itself, the SQLite job queue and its delivery paths, the tiered fetcher's LLM
spend, the eval and test harnesses, the gates, the catalog, and the operational
reporting. Subjects the index carries but pumper has no surface for (client
architecture, UI, companion/agent orchestration, people analytics) are not
listed at all rather than padded with `n/a` rows.

Every technique file was read before its row was written — the technique's name
is not its contract, and several rows changed verdict once the file was open.

A `deviation` here is a finding, not a shame. Five were closed in the audit
session itself (three named by the dispatching wave, two the audit surfaced),
and the backlog at the foot of this file is what each later wave drains, ranked
by value rather than by ease.

**Wave 2 (2026-08-24)** drained nine more, listed with their commits under
*Drained 2026-08-24* below: the unguarded dataset hard delete, the CI ladder
that had never bound, three RUSTSEC advisories, the gate-policy projections, the
doc-sync hook's missing third outcome, the boot recovery sweep's private policy,
the claim loop's telemetry metronome, the catalog's unenforced vocabularies, and
the shipped SDK that no job compiled. Deviations: **8 → 4**. One item is
explicitly *blocked for the operator* (authentication) and says why.

**Wave 2b (2026-08-25)** drained the remaining four, listed under *Drained
2026-08-25* below: the store that could not measure itself, the two janitors
firing on a wall clock, `#[ignore]` used as an ownerless permanent quarantine,
and perf harnesses that asserted nothing on no schedule. Deviations: **4 → 0**.

Two operator decisions of 2026-08-24 bind this wave and are applied rather than
re-litigated. **Authentication is deferred** — no production timeline, audience
or auth strategy exists yet, so no auth scheme was invented. **Data retention is
deferred** — keeping expired data *is* the pattern for now, so nothing in this
wave deletes, expires or prunes anything. That constraint shaped the work rather
than merely limiting it: the maintenance window is lossless-only, and neither
janitor was given a harm bound, so under permanent load they defer indefinitely
instead of escalating past the gate to delete. The non-destructive adjacent work
— measuring growth, making kept-forever visible in bytes and pages — is exactly
what shipped.

| subject | technique | status | evidence |
|---|---|---|---|
| web-scraping | dedup-and-datasets | followed | `crates/core/src/datasets.rs:274` — `RemovalGuard` is unforgeable, so absence-processing cannot run without asking source health |
| web-scraping | dry-run-preview | followed | `crates/server/src/routes/runtime.rs:403` — `/extract/preview` runs the production extractor against a live fetch and writes nothing |
| web-scraping | extraction-rule-dsl | partial | `crates/core/src/extract.rs:86` — no per-rule failure semantics; required-ness lives only in the dataset contract, and `all:false` silently takes the first match |
| web-scraping | llm-assisted-rule-authoring | followed | `crates/apps/provisioner/src/lib.rs:384` — the model's draft is run through the real engine against the body it was drafted from before it can be promoted |
| web-scraping | scrape-scheduling | partial | `crates/core/src/storage.rs:738` — the ladder gates writes only; N broken runs never disable or stretch the cron, and a schedule has no disable reason/since |
| web-scraping | shape-change-detection | followed | `crates/core/src/resilience/detect.rs:290` — a bad fetch yields `Inconclusive` rather than a judgement on the extractor; `fetcher.rs:315` separates block from outage |
| web-scraping | soak-mode-and-verdict-replay | followed | `crates/core/src/resilience/preview.rs` replays stored verdicts and re-judges nothing; the two honesty gaps this wave found are fixed below |
| health-checks | check-scheduling | partial | `crates/server/src/scheduler.rs:381` guards overlap only — no per-target backoff on repeated identical verdicts, and no digest rhythm separate from the observation rhythm |
| health-checks | health-rollup | partial | `crates/server/src/routes/doctor.rs:190` — `healthy: findings.is_empty()` reports green when a member was unverifiable (`doc_count: null` fires no finding) |
| health-checks | probe-caching | partial | `crates/core/src/cache.rs:98` has per-caller staleness tolerance but no in-flight coalescing; `state.contract_verdicts` has no eviction (`routes/health.rs:394`) |
| health-checks | probe-design | partial | `crates/server/src/routes/meta.rs:31` — `/health` is a static `{"status":"ok"}` touching no dependency, yet `scripts/smoke.ps1:222` gates boot on it |
| health-checks | remediation-affordances | partial | `crates/core/src/doctor.rs:50` — `remediation` is prose, so fixability is not a tier a surface can branch on |
| health-checks | three-state-outcomes | followed | `crates/core/src/resilience/mod.rs:136` — an unreadable state parses to `Unknown`, never `Healthy`; joined by `Inconclusive`/`BelowCohort` and `stale: Option<bool>` |
| background-jobs | adaptive-cadence | followed | `crates/server/src/worker.rs:85` — coalescing `Notify` OR a 2s fallback poll, fixed-delay, spurious wakes safe under the atomic claim |
| background-jobs | job-progress-and-cancellation | partial | `crates/server/src/routes/jobs.rs:449` answers `{cancelled:true}` while the row stays `running` — no `cancelling` ack state, and progress is an opaque app blob |
| background-jobs | loop-health-telemetry | partial | `crates/server/src/scheduler.rs:130` — `PassTally` reaches a log line only; no per-loop tick snapshot, last-success, or silence detection |
| background-jobs | loop-supervision | partial | `crates/server/src/main.rs:245` — four bare `tokio::spawn` loops, no registration door, roster, first-tick stagger, or single-instance claim on the store |
| background-jobs | startup-sweeps | followed | `crates/core/src/storage.rs:660` — bounded by `RECOVERY_SWEEP_LIMIT` rows and `RECOVERY_SWEEP_BUDGET` of wall clock, idempotent, stamping `interrupted at boot (executor gone)` per row and reporting requeued/failed/skipped/truncated |
| background-jobs | tick-isolation | partial | Layers 1–3 present (`crates/server/src/scheduler.rs:84`); layer 4 absent — no consecutive-failure count, escalating backoff, quarantine, or per-tick deadline |
| job-coordination | job-observability | partial | `crates/core/src/storage.rs:18` — `heartbeat_at` is never in `JOB_COLUMNS`, so no surface can show lease age, holder, or a transition trail |
| job-coordination | job-state-machines | partial | Vocabulary and strict parse are solid (`crates/core/src/job.rs:42`), but ~9 hand-written conditional `UPDATE`s (`storage.rs:319-441`) replace one transition door |
| job-coordination | lease-renewal | partial | `crates/server/src/worker.rs:757` discards the `bool` saying the lease was lost, so a replaced-but-alive executor runs on (harmless only because writes are attempt-fenced) |
| job-coordination | step-position-and-resumability | partial | `crates/apps/research/src/lib.rs:53` — position is an opaque ordinal cursor; no platform step ids, plan version, or per-step re-run-safety declaration |
| job-coordination | terminal-state-recovery | followed | `crates/core/src/storage.rs:700` — one `issue_recovery_verdicts` behind all three triggers (boot sweep, lease reaper, drain deadline), differing only in selection and in the reason stamped on the row |
| delivery-guarantees | atomic-claiming | followed | `crates/core/src/storage.rs:299` — a single conditional `UPDATE … RETURNING` is the election; every later write is re-conditioned on `(status, attempts)` |
| delivery-guarantees | dead-letter-design | partial | `crates/core/src/storage.rs:1695` — `oldest_undelivered` excludes `dead`; no discard verb, triage state, bulk redrive, or oldest-untouched alarm |
| delivery-guarantees | guarantee-selection | followed | `docs/features/events-webhooks.md:18` — at-least-once with a stable `x-pumper-delivery-id`, written per class where the maintainer trips over it |
| delivery-guarantees | non-delivery-ledgers | partial | The trigger lane is exemplary (`crates/core/src/storage.rs:2849`), but the scheduler's `Refused`/`Held` firings persist nowhere (`scheduler.rs:178`) |
| delivery-guarantees | retry-escalation | followed | `crates/core/src/storage.rs:300` — the claim itself increments the counter, so reaper- and boot-requeued attempts count; the threshold is a transition, not a log |
| delivery-guarantees | stuck-reaping | followed | `crates/core/src/storage.rs:668` — lease-expiry detection routed through the same `fail()` verdict path; slow-but-alive loses politely via the attempt fence |
| embedded-db | connection-pooling | partial | `crates/core/src/storage.rs:181` — one shared pool with pragmas in the factory. **Pool waits are now measured** (wave 2b): acquire is its own instrument phase with its own slow line, never folded into query time, and `StoreInstrument::pool_saturated` (`store_instrument.rs:698`) derives a saturation signal the maintenance gate reads. `max_connections(8)` is still an unjustified number — no measurement yet says 8 is right, which is precisely the question the new acquire rings can now answer |
| embedded-db | db-self-instrumentation | followed | `crates/core/src/store_instrument.rs:85` — keys are `(op, table, phase)` from two closed vocabularies with a vocabulary test; **pool acquire is its own phase** with its own slow line, never folded into query time; local-store slow lines (2/5/25/50/1000 ms) are *published* as `pumper_store_slow_line_seconds` so every slow count carries its predicate; rows-touched and busy/locked ride every record, contention classified by the driver's **result code** (`Error::is_store_contention`), never message text; the write path is one clock read + a packed `u64` into a fixed 256-slot lock-free ring, no allocation and **no use of the database**. Three consumers: a rate-limited warn channel whose limiter carries suppressed-count + worst-suppressed-duration on rollover, the maintenance gate, and `/datasets/doctor`'s `store` block with a `derived_by` map naming every recomputation (`routes/doctor.rs:301`). Queue depth *extended* not duplicated, plus oldest-queued/oldest-running ages (`meta.rs:441`) and `pumper_store_bytes{main,free,wal}` (`meta.rs:483`). Census is honestly partial — 7 families instrumented, the unmeasured populations named in the HELP text and the doctor JSON |
| embedded-db | extension-lifecycle | n/a | the store loads no extensions, UDFs or collations — 40 migrations, no `VIRTUAL TABLE`, no `load_extension` |
| embedded-db | journal-and-durability-modes | partial | WAL is set at `crates/core/src/storage.rs:179` and the file-set clause is tested, but the effective mode is never read back and `synchronous` is never stated |
| embedded-db | quiet-window-maintenance | followed | `crates/server/src/maintenance.rs:141` — `decide()` is a pure function of clock, gauge and harm, so every rung is testable without a database or a timer. Two-condition gate (gauge zero AND minimum interval); the activity gauge (`activity.rs`) is fed at the real front doors — an axum layer and the worker's spawn body, via RAII guards, signed `i64` so a leaked decrement shows negative instead of reading as permanently busy — with pool saturation as a third busy signal. Three-rung ladder whose hard bound is **`wal_harm_bytes`, a harm figure, not elapsed time**. Three outcomes recorded (`pumper_store_maintenance_passes_total{task,outcome}`), and `NotDue` deliberately excluded so the deferred count keeps meaning "the application was busy". Chunk/yield/re-check with the connection released before every gauge re-check. **Lossless only** — checkpoint / `optimize` / `ANALYZE`; both janitors' deletion semantics are byte-for-byte unchanged and neither carries a harm bound, so under permanent load they defer indefinitely and visibly rather than escalating past the gate to delete (operator retention deferral, reasoning written into `harm_bound_for`) |
| embedded-db | single-writer-holder-discipline | followed | `crates/engine-search/src/lib.rs:284` — the OS writer lock is taken before any destructive rebuild, and a live holder is named in the refusal |
| embedded-db | storage-accounting-and-pruning | partial | `crates/core/src/storage.rs:1931` reports rows + oldest per table. **The accounting half landed** (wave 2b): `pumper_store_bytes{main,free,wal}` and `pumper_store_pages{total,free}` on `/metrics` (`routes/meta.rs:483`), keyed by the same table label as `ledger_stats` so the technique's "big AND degrading" join is available. **The pruning half is `deferred` (operator 2026-08-24: keep expired data until the production dynamic resolves; revisit retention after)** — reclamation would shrink the file by discarding, which this wave was explicitly forbidden to do. Making kept-forever *visible* was the in-scope half and is what shipped |
| retry-backoff | backoff-design | followed | `crates/core/src/storage.rs:345` — `10s·2^attempts` capped at 3600s, now jittered +0..25% from a deterministic seed (fixed this wave; was lockstep) |
| retry-backoff | circuit-breakers | followed | `crates/core/src/resilience/detect.rs:833` — a closed/open/half-open ladder with hysteresis and evidence-only release into probation |
| retry-backoff | durable-retries | followed | `crates/core/src/storage.rs:1508` — a persisted `next_retry_at`, the attempt count on the row, a jittered ladder, an atomic claim, and a `dead` terminal state |
| retry-backoff | error-classification-for-retry | followed | `crates/core/src/error.rs:412` — classification by typed predicate with an exhaustive-match inventory guard, never by message text |
| retry-backoff | retry-observability | partial | `crates/core/src/storage.rs:1723` — `next_retry_at` is never selected or serialized, so "what is due to retry" is unqueryable |
| retry-backoff | storm-control | partial | Per-key state is bounded (`crates/core/src/governor.rs:30`) but there is no cross-item retry budget per dependency and no warn-once-per-episode latch |
| data-retention | confirm-by-echo | followed | `crates/server/src/routes/datasets.rs:352` — preview (428) → echo → yield guard, the last re-checked inside the deleting transaction; `e2e/dataset_delete_gate.rs` asks after every refusal whether the rows survived. Authn is absent because the server has no identity concept at all — recorded below, not invented |
| data-retention | destructive-override-floor | partial | `crates/core/src/config.rs:836` refuses a bad `keep_min` and inverted windows, but no floor guards the `*_retention_days` values themselves — `= 1` is accepted |
| data-retention | dry-run-preview | partial | `crates/server/src/routes/retention.rs:77` — only the artifact plan is previewable; the ledger and revision prunes get current row counts, so enabling a knob is blind |
| data-retention | erasure-requests | partial | `crates/core/src/datasets.rs:1405` is the removal door but reaches records + revisions only — the archived body, the http/research caches and the DataHub shadow keep copies |
| data-retention | per-tenant-retention-policy | partial | Sentinel-0, declared populations and both window shapes exist, but windows are global with no per-app override, and the table→window map is duplicated (`routes/doctor.rs:47` vs `config.rs:1141`) |
| data-retention | time-budgeted-batch-purge | deferred | `deferred (operator 2026-08-24: keep expired data until the production dynamic resolves; revisit retention after)`. `crates/core/src/storage.rs:1865` is still one unbounded `DELETE` per table with no budget, no batching and a silent zero-deleted — but this technique is *entirely* about how deletion is paced, and with deletion itself deferred there is nothing here to pace. Matches backlog #20. Re-rank when retention unparks |
| cost-metering | budget-enforcement | partial | Typed refusal at one door (`crates/core/src/app.rs:276`) covering jobs/triggers/schedules/MCP; every ceiling is per-run, so aggregate spend is unbounded |
| cost-metering | preflight-estimation | partial | Only `MIN_STEP_BUDGET_USD` (`crates/apps/research/src/lib.rs:254`); no per-call prediction and no stored estimate-vs-actual calibration |
| cost-metering | price-tables | partial | Meter-reported cost is recorded verbatim (`crates/core/src/costs.rs:102`), but an unreadable price books `$0` rather than a conservative default (`app.rs:794`) |
| cost-metering | reversible-debit-and-settle | n/a | no prepaid balance — spend is metered after the call and the ceiling is a launch gate, the shape the technique says not to add a reversal lifecycle to |
| cost-metering | spend-attribution | partial | Job and app are required args of `meter` (`crates/core/src/app.rs:226`), but the model actually served is never stamped — `engine` is only the tier (`"claude"`) |
| cost-metering | spend-observability | partial | Class rollup and row drill-down exist, but failed/unreported spend is not its own series and `GET /costs` returns a windowless total (`routes/jobs.rs:534`) |
| cost-metering | usage-ledgers | partial | Failure-ordered rows with retention that spares the enforced window (`storage.rs:1866`), but `cost_usd REAL NOT NULL DEFAULT 0` cannot say "unknown" |
| eval-harness | assertion-vs-judgment | followed | `crates/core/tests/eval_tier3_extraction.rs:314` — a wholly deterministic scorer, with schema/trace/ledger assertions gating before it and no judge anywhere |
| eval-harness | certification-levels | partial | The offline lane is labelled as exactly what it saw ("Honest ceiling", `eval_tier3_extraction.rs:54`); there is no live-model lane and no declared re-record criteria |
| eval-harness | comparison-modes | partial | The absolute per-case ratchet is the right mode for a gate, but the scorer weights are duplicated in code (`:321`) and `manifest.json` with nothing binding them |
| eval-harness | eval-economics | followed | The suite replays recorded CLI envelopes at zero spend in the default lane, and an unrecorded prompt is a typed error rather than a live call |
| eval-harness | judge-stability | n/a | no model scores any output — the instrument is deterministic Rust, so drift is a code diff, not calibration |
| eval-harness | scenario-design | partial | Real frozen fixtures with ground truth verified against the page (`:404`), but no per-case content version or region tag — coverage is a bare count at `:384` |
| connector-catalog | adapter-normalization | followed | `crates/apps/grants-common/src/lib.rs:349` — per-source `normalize_*` map every provider onto one consumer-designed model; unavailable fields are honest `Null`, never zeros |
| connector-catalog | catalog-as-data | followed | `crates/core/src/catalog.rs:340` — six closed vocabularies checked at parse against one set of constants, all findings at once; a typo'd `cadence` can no longer join `on-demand` in the catch-all, and the doc table was corrected against the constants |
| connector-catalog | catalog-lifecycle | partial | Orphan detection exists (`crates/core/src/catalog.rs:528`), but there is no per-entry content hash and no alias — a renamed `id` dangles its schedule and health row |
| connector-catalog | matching-and-ranking | partial | Exact identity plus a test-pinned redirect table with loud unresolved (`crates/server/src/routes/watches.rs:113`); the rejected reference is never recorded, so nothing mines the backlog |
| connector-catalog | schema-driven-forms | partial | The params half is declarative and enforced at one door (`crates/server/src/routes/jobs.rs:147`); the auth half proves only env-var presence (`crates/core/src/app.rs:837`) — the vacuous green |
| connector-catalog | shipped-vs-operator-ownership | followed | `crates/core/src/storage.rs:2096` — every reconciler write is SQL-fenced `AND managed_by = ?`, so the boot-time clobber of an operator edit is unrepresentable |
| observability-telemetry | crash-record-storage | partial | Panic evidence is durable and located (`crates/server/src/worker.rs:939`), but an abort/OOM/SIGKILL writes nothing — there is no bounded on-disk crash store |
| observability-telemetry | diagnostic-access | partial | Named read-only paths exist (`crates/server/src/routes/doctor.rs:86`); there is no export bundle, no manifest, and no second scrub gate on the way out |
| observability-telemetry | log-architecture | partial | Levels and per-origin `EnvFilter` targeting are right, but `crates/server/src/main.rs:173` writes synchronously to stdout — no bounded channel, drop counter, or shutdown flush |
| observability-telemetry | pre-boot-and-foreign-capture | partial | Chrome's CDP channel is bridged always-on (`crates/engine-browser/src/lib.rs:534`); the Claude CLI child's stderr is dropped entirely on success (`crates/engine-claude/src/lib.rs:335`) |
| observability-telemetry | remote-telemetry-economics | followed | `crates/server/src/worker.rs:176` — first failure, then one report per 5 min, then the recovery with the outage's true length; the suppressed volume survives as `pumper_worker_claim_failures_total`, a local sink a dead channel cannot suppress |
| observability-telemetry | rotation-and-retention | partial | Per-class reapers state their predicate (`crates/server/src/routes/retention.rs:88`), but the first tick is 6h after boot (`main.rs:547`) and there is no total-byte cap |
| test-harness | fixture-economics | partial | `crates/core/src/testing.rs:51` rebuilds the whole migration chain per test — no template-build + cheap-copy split, and no input fingerprint to rebuild a stale template |
| test-harness | flake-lifecycle | followed | `.flake/register.json` — 2 quarantine rows (owner `xkazm04`, entered 2026-08-24, staggered expiries, suspected cause, form + `formReason`, evidence, exit plan) beside a 17-row reasoned `exempt` table, a ceiling of 4 with its rationale, and a stated doctrine that an agent never quarantines to make a build green. `scripts/ci/flake-check.mjs` reconciles the register against the tree **in both directions** — an unregistered flake-reasoned `#[ignore]` and an orphaned row both fail — and additionally catches *laundering* (an `exempt` row whose own source reason says "flaky"), expiry, ceiling, owner-is-a-person and skip-needs-reason, on the repo's 0/2/**3 CANNOT CHECK** exit discipline. History accumulates per stable id `<package>::<target>::<module>::<fn>` via `flake-record.mjs` on both required `test` legs (exit code forwarded byte-for-byte; recording failure is a warning, never a verdict), persisted by an `actions/cache` rolling key saved `if: always()` so the red half of a transition survives. Transitions counted **on the same sha only**; every figure prints window, branch and run count. `#[ignore]` strings now point at the register, so the label is visible where the test appears. nextest rejected on the record — it would change *what runs* on a protected rung |
| test-harness | history-driven-partitioning | n/a | no within-suite worker split exists — one `cargo test --workspace` in one CI job, and no per-test duration store to partition on |
| test-harness | isolation-lanes | partial | `scripts/smoke.ps1:77` builds a fresh GUID scratch profile and reaps it in `finally`; no startup reaping of a crashed run's leftovers, and port 18099 carries no declared serial policy |
| test-harness | live-app-harness | followed | `scripts/smoke.ps1:208` boots the real shipped binary against an isolated profile, polls readiness before asserting, and drives one product-level job to a terminal readback |
| test-harness | long-lane-certification | followed | `.lanes/criteria.json` — 8 lanes, 13 pre-declared bounds, each carrying its own `predicate` and `basis`; measurement stays in Rust (`crates/core/tests/lane_artifact/`, one artifact per run) and judgement moves to `scripts/ci/lane-certify.mjs`, so a verdict is reproducible from artifact + criteria alone (both are written into each verdict file). **Percentiles not averages**, and ratio bounds preferred over absolutes so a bound certifies the index rather than the runner. The bulk harness now emits a 100-sample per-chunk hold *sequence*, giving a **slope-over-the-second-half** leak criterion — which earned itself immediately: a seeded defect broke `chunk-hold-growth` while `chunk-hold-p95` still passed, the entire argument for a trend criterion. Runs on its own clock (nightly cron `41 5 * * *` + `workflow_dispatch`, routed by `github.event.schedule` so the existing weekly jobs were not promoted to nightly). `first-green` is tracked as an explicit lane event and "NO RUNS RECORDED" / "never attempted here" / "**NEVER GREEN**" are three different sentences; a lane that emits no artifact reports CANNOT-SEE and exits 3 rather than passing. The 4 lanes needing Chrome, live network or the Linux-only plugin rung stay listed as `cannot-run` so the gap stays counted |
| test-harness | negative-control-tests | followed | `crates/core/tests/eval_tier3_extraction.rs:582` mutates each recorded answer and demands the score drop, with an instrument floor and the one absorbable case fenced by `RECORDED_PREAMBLE_MISSES` (fixed this wave) |
| test-harness | out-of-graph-artifacts | followed | `.github/workflows/ci.yml` job `@pumper/sync (TypeScript SDK)` names the artifact, its manifest and its own cache, and runs the SDK's test script; `scripts/ci/ship-inventory.test.mjs` fails on any manifest no job claims — an inventory gate, not a memory |
| test-harness | platform-quirk-absorption | partial | Quirks are absorbed with the incident attached (`crates/core/src/testing.rs:43`) and CI now runs a `windows-latest` leg (`ci.yml:53`, `fail-fast: false`), so the platform the absorptions exist for is gated; there is still no zero-tests-executed floor. **Wave 2b found the cost of that gap being open**: the `Artifact tests` step had been failing on every run with "data/plugins/title.wasm not built" — the wasm target was installed against `stable` while `rust-toolchain.toml` pins 1.96.1, so the plugin build refused and the only rung that catches a host-ABI break was reporting its own non-execution as a test failure. A zero-tests-executed floor is exactly the instrument that would have named it as *cannot run* on day one instead. Fixed in `6c8e767`; the floor itself is still open and is now the strongest remaining item in this subject |
| test-harness | suite-partitioning | partial | Lanes exist by command and plugins-src membership by location; no per-suite budget or tier, and the `--ignored` lane's membership is annotation-based |
| quality-gates | blocking-by-input-determinism | partial | `deny.toml:78` leaves `licenses` off CI with a named promotion trigger, but `ci.yml:84` blocks on `advisories` (an external feed) bundled with the deterministic `bans`+`sources` |
| quality-gates | chokepoint-tag-registry | followed | `crates/core/tests/fetch_chokepoint.rs:39` pins every raw-engine site with counts and per-row reasons, checked in both directions; extended to `enforcement_preview.rs` this wave |
| quality-gates | false-positive-economics | partial | Scanners narrow rather than fire on prose and every finding has a reviewable escape row; the shipped resilience detector has no measured precision/recall — its eval harness is documented as not built |
| quality-gates | gate-laddering | followed | `.github/workflows/ci.yml:32` — `Format` is its own parallel job and every verdict step carries `if: !cancelled()`, so no rung can blind the ones after it; five jobs now report independently on the same commit |
| quality-gates | gate-liveness | followed | `scripts/docs/check-doc-sync.mjs:266` — three outcomes, three exit codes (0 clean / 2 drift / 3 CANNOT CHECK), with instrument assertions on both the transcript and the rule map (including a map that loads with zero entries); four tests, each red against the previous script |
| quality-gates | hook-hygiene | partial | The one hook refuses and explains without mutating, is non-interactive and bounded, and its fixture suite now runs in CI (`ci.yml` job `Ship inventory + doc-sync hook`); the hook ITSELF still cannot run on a clone without `.claude/settings.json`, so there is still no doc-drift rung there |
| quality-gates | policy-projection | followed | `justfile:56` carries `-D warnings`, `just ci` invokes all five CI jobs, and `.ai/manifest.yaml` states the rungs that exist (`prePushHabit`/`localAdvisory`/`ciHardPass`) instead of a `prePush` hook that never existed |
| quality-gates | ratchet-design | partial | `evals/tier3-extraction/manifest.json` is a committed per-case baseline naming its counter and predicate; a case scoring *above* baseline passes silently with no forced re-baseline |
| quality-gates | severity-by-construction | followed | `justfile:56` — the local twin now carries `-D warnings` too, so a clippy advisory reaches the exit code at both rungs instead of only the remote one |
| quality-gates | unmeasurable-criteria | partial | A failed health read now derives a distinct `Unknown` that gates nothing and is render-only (fixed this wave); nothing counts how often it is reported, so data starvation stays invisible |

## Deviations backlog

Ranked by value, and kept in its original numbering so a later wave's report can
still be read against it. Closed items are struck through and repeated with their
commit under *Drained 2026-08-24*; item 1 kept the top slot until it closed
because it is the item that decides whether any of the others are ever caught by
a machine.

1. ~~**quality-gates / gate-laddering** — CI aborted at step 1 (`Format`) in 85 of 86 runs since 2026-07-26.~~ **Fixed** — `Format` is its own parallel job, every verdict step carries `if: !cancelled()`, and the three RUSTSEC advisories are cleared.
2. ~~**web-scraping / soak-mode-and-verdict-replay** — a failed health read rendered as `Healthy`, publishing a confident green about a check that had not run.~~ **Fixed** — `SourceState::Unknown`, render-only, gating nothing.
3. ~~**web-scraping / soak-mode-and-verdict-replay** — the consequence inventory was checked against a hand-copied literal, so it could catch a typo and could not catch a new consumer.~~ **Fixed** — a workspace scanner over every `enforced_state` call site.
4. ~~**test-harness / negative-control-tests** — the eval scorer's standing negative control weakened one assertion for an unnamed case, so a second silent miss could hide under the same branch.~~ **Fixed** — `RECORDED_PREAMBLE_MISSES`, checked in both directions.
5. ~~**retry-backoff / backoff-design** — the job retry ladder was a pure function of the attempt number, so one dependency outage re-queued the whole fleet at the same instant.~~ **Fixed** — +0..25% deterministic jitter, matching what `fail_delivery` already did.
6. ~~**data-retention / confirm-by-echo** — the dataset hard delete had no echo, no preview and no auth.~~ **Fixed** — preview → echo → yield guard, plus an NDJSON export before anything is destroyed. *Authentication remains open and is **blocked-for-user**: this server has no identity concept at all, and inventing one is a product decision (see the note under Drained).*
7. **background-jobs / loop-supervision** — no single-instance claim on the store, so a second process runs `recover_stuck` over the first's live `running` rows before the port bind would collide. Add a boot-time claim, and a `spawn_loop(name, cadence, …)` door that yields the roster telemetry can join on.
8. ~~**embedded-db / quiet-window-maintenance** — both janitors fire on wall clock and can land mid-scrape holding the writer lock.~~ **Fixed** — an activity gauge at the real front doors, a two-condition gate, a three-rung ladder whose hard bound is WAL bytes rather than elapsed time, three recorded outcomes, and a chunked `wal_checkpoint(PASSIVE)` + `optimize` + `ANALYZE`. Lossless only; both janitors' deletion semantics unchanged.
9. ~~**quality-gates / policy-projection** (with **severity-by-construction**) — three hand-copies of one gate policy disagreed.~~ **Fixed** — `-D warnings` on `just lint`, `just ci` invokes all five CI jobs, and the manifest states the rungs that exist.
10. **background-jobs / loop-health-telemetry** — five subsystems ride one unmonitored scheduler tick, so a loop returning zero rows for weeks is indistinguishable from a healthy one. Write a per-tick snapshot to a bounded table and expose last-tick and last-success per loop.
11. **cost-metering / usage-ledgers** — `cost_usd REAL NOT NULL DEFAULT 0` cannot say "unknown", so a run killed at its ceiling books `$0` against the budget. Make the column nullable (or add `cost_known`) and return an unknown-row count beside the sum. *(schema migration — sized accordingly)*
12. **cost-metering / budget-enforcement** — every ceiling is per-run, so N runaway jobs each honour their $1 and the month is unbounded. Add a period ceiling consulted inside `require_budget`, with the refusal naming which scope refused.
13. ~~**job-coordination / terminal-state-recovery** — a blanket uncapped `UPDATE`, a different policy from the reaper's.~~ **Fixed** — one verdict function behind all three triggers, bounded twice, with the reason on the row.
14. **health-checks / probe-design** — `/health` returns a static `{"status":"ok"}` touching no dependency, yet `scripts/smoke.ps1` gates boot on it, so a wedged pool answers green forever. Complete the smallest real interaction under a deadline, or rename it `/livez` and point the gate at something that observes the store.
15. ~~**embedded-db / db-self-instrumentation** — no per-table or per-operation timing exists, so "the store feels slow" has nothing to interrogate.~~ **Fixed** — one measured chokepoint keyed by `(op, table, phase)` over closed vocabularies, with pool acquire as its own phase, rows-touched and result-code-classified busy/locked on every record, and three consumers each with a decision attached.
16. **web-scraping / scrape-scheduling** — the ladder quarantines writes, never the schedule, so a blocked or collapsed source keeps spending a third party's bandwidth indefinitely. Flip `schedules.enabled = 0` (or sharply stretch the cadence) after N consecutive non-successes, with a stored reason and since.
17. ~~**test-harness / out-of-graph-artifacts** — a shipped SDK reachable from no gated root.~~ **Fixed** — a CI job named after the artifact, plus `scripts/ci/ship-inventory.test.mjs`, which fails on any manifest no job claims.
18. ~~**test-harness / platform-quirk-absorption** — CI was ubuntu-only while the development box is Windows.~~ **Fixed** — a `windows-latest` matrix leg with `fail-fast: false` (the wasm/plugin rungs stay Linux-only). *A zero-tests-executed floor is still open.*
19. ~~**observability-telemetry / remote-telemetry-economics** — an event per poll tick for as long as the store is unreachable.~~ **Fixed** — first + every-5-min + recovery, with `pumper_worker_claim_failures_total` carrying the suppressed volume.
20. **data-retention / time-budgeted-batch-purge** — `deferred (operator 2026-08-24: keep expired data until the production dynamic resolves; revisit retention after)`. The item is entirely about how deletion is paced; with deletion itself deferred there is nothing here to pace. Re-rank it when retention unparks.
21. **embedded-db / storage-accounting-and-pruning** — **accounting half fixed** (`page_count`/`freelist_count`/WAL bytes now on `/metrics`); the `incremental_vacuum` reclamation half is `deferred (operator 2026-08-24: keep expired data until the production dynamic resolves; revisit retention after)` — it reclaims by discarding, which this wave was forbidden to do.
22. **retry-backoff / retry-observability** — `next_retry_at` is never selected or serialized, so "what is due to retry" is unqueryable. Select it onto `Delivery` and add a due-window filter.
23. **delivery-guarantees / non-delivery-ledgers** — the scheduler's `Refused`/`Held` firings persist nowhere, so a schedule that was accepted and never ran is untraceable. Persist the existing `StepOutcome` vocabulary as typed rows.
24. **health-checks / health-rollup** — `/datasets/doctor`'s `healthy` boolean launders an unmeasurable member. Carry the unverified count in the summary, the way `/sources` already carries `unmonitored`.
25. **quality-gates / unmeasurable-criteria** — the new `unknown` state is honest but uncounted, so a health gate starved of data looks identical to one that passed. Emit an `unknown` counter on `/metrics` and in the enforcement preview.
26. ~~**quality-gates / gate-liveness** — the doc-sync hook folded cannot-run into pass, twice.~~ **Fixed** — exit 3 = CANNOT CHECK, with instrument assertions on both the transcript and the rule map.
27. **cost-metering / spend-attribution** — the model actually served is never stamped, so opus-versus-sonnet spend is unrecoverable after the fact. Stamp the resolved model id onto the cost event.
28. **eval-harness / comparison-modes** — the scorer weights live in both code and `manifest.json` with nothing binding them, so changing the code silently invalidates every stored baseline. Load them from one place, or assert their equality.
29. ~~**connector-catalog / catalog-as-data** — an unknown `cadence` silently disabled freshness monitoring.~~ **Fixed** — six closed vocabularies checked at parse, all findings at once.
30. ~~**test-harness / long-lane-certification** and **flake-lifecycle** — the perf harnesses assert nothing on no schedule, and `#[ignore]` is being used as permanent quarantine with no owner or expiry.~~ **Fixed** — a quarantine register reconciled against the tree in both directions, and 8 long lanes with 13 pre-declared bounds certified on their own nightly clock.

## Drained 2026-08-24 (wave 2)

Nine backlog items, in commit order. Every fix was verified non-vacuous by
breaking it and watching its own test go red, then restoring — the commit
messages name which break was used.

| # | item | commit |
|---|---|---|
| — | three RUSTSEC advisories (wasmtime 46.0.1 ×2, h2 0.4.15) | `0367099` |
| 6 | data-retention / confirm-by-echo | `053b8ee` |
| 17 | test-harness / out-of-graph-artifacts (the gate) | `3d9d2d2` |
| 1, 17, 18 | quality-gates / gate-laddering; the SDK + inventory jobs; the Windows leg | `b3e5ae5` |
| 9 | quality-gates / policy-projection + severity-by-construction | `9979a84` |
| 26 | quality-gates / gate-liveness | `54318c2` |
| 13 | job-coordination / terminal-state-recovery (+ background-jobs / startup-sweeps) | `09cab63` |
| 19 | observability-telemetry / remote-telemetry-economics | `9fc2be3` |
| 29 | connector-catalog / catalog-as-data | `0032c0c` |

**Blocked for the operator — authentication (item 6's second half).** The
dataset delete now has a three-rung ladder in front of it, and the rung the
technique puts last is still missing: this server has no identity concept
anywhere. The only credential in the codebase is the ingress HMAC, which
authenticates a webhook *sender*, not an operator, and `docs/features/http-api.md`
has always described the API as deliberate "local power mode: no auth (API-key
auth is a parked decision)". Inventing an auth scheme is that parked product
decision, not a conformance fix, so the ladder contains the accident — a stale
tab, a copied curl line, the wrong dataset name — and does not pretend to
contain an attacker who can already reach the port. Whoever unparks it should
read `confirm-by-echo`'s ladder section: origin check, then echo, then
authorisation, in that order.

**Still open after wave 2** (wave 2b closed 8, 15 and 30, and deferred 20 and 21
under the operator's retention decision), in rank order: 7 (loop-supervision), 10
(loop-health-telemetry), 11 (usage-ledgers — schema migration), 12
(budget-enforcement), 14 (probe-design), 16 (scrape-scheduling), 22
(retry-observability), 23 (non-delivery-ledgers), 24 (health-rollup), 25
(unmeasurable-criteria), 27 (spend-attribution), 28 (comparison-modes).

One small thing this wave introduced and deliberately did not fix:
`.claude/CLAUDE.md`'s description of the doc-sync hook still says "exit 2 is a
reminder"; there is now also an exit 3 (cannot check). A worker agent editing
the repo's own policy file on another agent's instruction is the one edit this
lane does not make — one line for the operator.

Below this line is real but smaller: `job-progress-and-cancellation` (no
`cancelling` ack state), `lease-renewal` (the heartbeat result is discarded),
`job-state-machines` (nine `UPDATE`s instead of one transition door),
`dead-letter-design` (no discard verb, and `dead` is excluded from the
oldest-undelivered gauge), `probe-caching` (no in-flight coalescing),
`extraction-rule-dsl` (no per-rule failure semantics), `fixture-economics`,
`crash-record-storage`, `diagnostic-access`, `log-architecture` (a synchronous
stdout write path), `catalog-lifecycle`, `schema-driven-forms` (credential checks
prove only env-var presence), `preflight-estimation`, `price-tables`,
`spend-observability`, `scenario-design`, `certification-levels` (no live-model
eval lane), `storm-control`, `connection-pooling`, `journal-and-durability-modes`,
`per-tenant-retention-policy`, `destructive-override-floor`, `erasure-requests`,
`dry-run-preview` (retention), `check-scheduling`, `remediation-affordances`,
`startup-sweeps`, `tick-isolation`, `step-position-and-resumability`,
`matching-and-ranking`, `rotation-and-retention`, `pre-boot-and-foreign-capture`,
`isolation-lanes`, `suite-partitioning`, `blocking-by-input-determinism`,
`false-positive-economics`, `hook-hygiene` and `ratchet-design`.

## Drained 2026-08-25 (wave 2b)

The last four `deviation` rows, closed by two workers on disjoint write sets.
Every fix was verified non-vacuous by breaking it and watching its own test go
red, then restoring — the commit messages name which break was used, and the
director independently re-ran three of the injections rather than taking the
report on trust.

| # | item | commit |
|---|---|---|
| 15 | embedded-db / db-self-instrumentation — the measured chokepoint | `9f266d9` |
| 15 | embedded-db / db-self-instrumentation — `/metrics` + `/datasets/doctor` | `107341a` |
| 30 | test-harness / long-lane-certification — 8 lanes, 13 declared bounds | `43b4a02` |
| 30 | test-harness / flake-lifecycle — the register, reconciled both ways | `0b964f0` |
| 8 | embedded-db / quiet-window-maintenance — the gauge, the gate, the ladder | `9521c09` |
| 8, 30 | the harness gates get a CI job; the long lanes get a nightly clock | `672689c` |
| — | contract catch-up: CLAUDE.md's command table and `.ai/manifest.yaml` | `1d49102` |
| — | `docs/features/fetching.md` — the always-on janitor is gated now | `28b73bf` |

**Suite: 2039 → 2086 passed, 0 failed, 19 ignored — purely additive.** The 19
ignored are exactly the 19 the register accounts for (2 owned quarantines, 17
reasoned environment-gated exemptions), and `flake-check` fails if that stops
being true in either direction.

**Two findings not on the backlog, both caught before they shipped.** `PRAGMA
wal_checkpoint`'s second column is the *log's size*, not the remainder — the
first checkpoint loop read a completed pass as stalled and would have looped to
its cap forever; the e2e caught it and `CheckpointRound::remaining()` now names
the trap. And the lane recorder's `--lane` path wrote its "test passed" note over
the Rust harness's own measured series, which is a lane reporting green with
nothing behind it; it now writes a part file, with a test pinning the property.

**What the retention deferral cost, stated plainly.** Backlog 20 and 21 are
`deferred`, not drained: with deletion off the table there is no batch purge to
pace and no reclamation to schedule. The half that was in scope — making
unbounded growth *visible* in bytes, pages and WAL sidecar size — shipped, so
when retention unparks the accounting needed to size it already exists.

**Six pre-existing CI reds, found because this wave actually looked.** master's
`test` legs had been red since at least 2026-08-24 16:50 and nobody had looked —
the exact pathology `long-lane-certification` names, where red becomes normal and
every failure after the first is wallpaper. All three were repaired, and none of
them was a test disagreeing with the code:

| red | cause | commit |
|---|---|---|
| `vcr::…::total_cap_truncates_later_entries…`, ubuntu-only | the cassette recorder never flushed; `tokio::fs::File` is buffered and does not complete pending writes on drop, so a recording could lose its tail | `8c627d0` |
| `Clippy`, ubuntu-only | `fake_cli.rs` imported `Path`/`Duration` that only the `#[cfg(windows)]` tests use — dead code on Linux, an error under `-D warnings` | `6c8e767` |
| `Artifact tests`, every run | the wasm target was installed against `stable` while `rust-toolchain.toml` pins 1.96.1, so the plugin build refused and the host-ABI rung reported its own non-execution as a failure | `6c8e767` |
| `a_failed_jar_save_is_retried…`, ubuntu-only | the cookie-jar READ treated only `NotFound` as "no stored session"; Unix answers `ENOTDIR` where Windows answers NotFound for the same fact (a path component is a file), so a read seam became platform-dependent | `953c2e6` |
| `background_committer_flushes…`, windows-only | a fixed 400 ms sleep against a 250 ms commit interval — plenty on a developer box, a coin toss on a loaded runner. Now polls to a deadline; the assertion is unchanged | `dc6eb05` |
| `a_pause_expires_loudly…`, windows-only | `Instant::checked_sub(3600s)` returns `None` on a machine up less than an hour (the monotonic origin is boot), so the "governance has gone blind" state was never built and a FAILED SETUP was indistinguishable from the real regression. Driven by shrinking the window now, and the backdate is asserted | `71971fc` |
| `the_subprocess_runs_in_its_own_dir…`, ubuntu-only | ETXTBSY. A multi-threaded test binary writes executables and runs them; another thread's fork briefly inherits the write handle, so the kernel refuses the exec. Every unix fake CLI now proves it is executable at the one seam that creates it | `67f7fd3` |

The first was verified the only way it could be: **red on the parent commit,
green on the child**, in CI, on the platform that has it. It could not be shown
red locally — the race does not manifest on Windows — and that limitation is
recorded in its commit message rather than glossed.

**Not one of the six was a test disagreeing with the code.** Three were rungs
that could not run, and three were test setups that could not be built on the
machine CI runs on. That distribution is itself the finding: the suite was not
wrong about the product, it was wrong about its own environment — and every one
of them was invisible to a local green, because this development box is Windows
and four of the six were ubuntu-only. **A local green on one platform is not
evidence about a two-platform gate.**

None was quarantined. The register this wave built carries the rule that an agent
never quarantines a test to make a build green, and the two timing-shaped reds
(`background_committer…`, `a_pause_expires…`) are exactly the ones where that
rule bites: both were repaired as harness defects — a poll instead of a sleep, an
asserted setup instead of a silent `None` — because neither was ever flaky about
the product.

**Still owed to the operator, unchanged from wave 2:** `.claude/CLAUDE.md` still
describes the doc-sync hook as "exit 2 is a reminder" and there is now also an
exit 3. A worker agent editing the repo's own policy file on another agent's
instruction remains the one edit this lane declines to make.
