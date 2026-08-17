# Field Report v2 — pumper — shard: integrations-security + platform-delivery + ai-agents

**Headline counts (51 leaves scored, ~120 physics clauses).**
`holds 14 · partial 13 · violates 1 · n/a-absent 20 · n/a-scope 3`, plus **3 frontend
domains → n/a-absent** (pumper has no frontend). No `holds(self)`: pumper keeps its own
`.perfect/`/`MEMORY.md` decision ledgers but **no census/rules ledger against Personas**,
and no shard leaf was authored *from* pumper — every hold is independent corroboration.

**The three findings that matter:**
1. **AI metering is exemplary and independently reinvented.** Every model call funnels
   through one chokepoint (`AppContext::research`), and the bypass is a **compile error**
   (`EngineSet::claude` is `pub(crate)`) backed by an inventory test — the corpus's ideal
   ("withhold the unmetered door"). Failure classification is from a **typed enum**, never
   message substrings, with the anti-pattern named in the doc. `headless-model-call` P5/P6/P7
   and `failure-recovery-strategy` P2–P5 **hold** cleanly.
2. **The single recurring AI defect: the spend row carries no dimension, and the ceiling
   defaults to unlimited.** `cost_events` has no model/owner/effort column and drops cache
   tokens; `budget_usd` is `Option`, `None` = unlimited, config default `None`. This one gap
   spreads across `llm-spend-accounting`, `model-and-effort-selection` (P5), `spend-ceilings`
   (P1) and `autonomy-gating`. Fixable in one migration + one metering edit.
3. **The set-equality inventory test and the cargo-deny supply-chain gate are pumper's
   physics-grade exports** (enrichment §3). `cross-artifact-drift-gate` and `adding-a-ci-gate`
   hold; the `deny.toml` dependency-audit gate remains **absent from the 206-leaf corpus**.

The two real security gaps are both about the store not refusing: **no content-based secret
scanner** (`secret-leak-scanning` violates) and **webhook HMAC signing secrets stored
plaintext** in SQLite (`column-encryption-at-rest` partial — by the corpus's own taxonomy
these "must-present-later" secrets need reversible encryption). Both are bounded by the
single-tenant loopback posture, and neither has a recorded threat-model decision.

---

## 0. Orientation + independence declaration

pumper is a local-first scraping service: one axum binary, a durable SQLite (WAL, sqlx) job
queue, and **pluggable engines** (`engine-{http,browser,claude,wasm,search,archive,remote}`).
Single-tenant, loopback-bound (`127.0.0.1:8088`), **no inbound authentication** on any route —
the safety boundary is the network, not the app. This makes the entire credential-vault /
OAuth / capture-UI family of `integrations-security` leaves **n/a-absent by design**: there is
no credential store, no tenant, no session, no UI.

**Entanglement with the corpus.** Independent. pumper is cited *in* several golden-paths as an
external sibling ("reinvented in a sibling repo", "two codebases in different stacks"), and two
of its patterns flow *back* into the corpus (the EXPECTED-set inventory idiom; the cargo-deny
gate — see §3). None of the shard's 51 leaves is authored from pumper's own ledger, so no
verdict is a self-match. The `holds` are physics corroboration, not circular.

**Frontend domains (per brief):** ui-system → **n/a-absent** · client-runtime → **n/a-absent**
· product-surfaces → **n/a-absent**. pumper ships no frontend; the only client is a headless
TypeScript sync SDK (`clients/typescript`).

**Instruments (honesty).** `rg` is not on PATH; the ripgrep-backed Grep tool + `Read` were used
for every check. **No `cargo`/build/run was invoked** (standing rule). Two independent
sub-investigations (AI plumbing; secrets/sync/plugins) corroborated the direct reads of the
metering chokepoint and the secret-handling sinks. Verdicts about a code *shape* rest on reading
that exact code; absence claims rest on executed greps.

---

## 1. Scorecard

Clause counts are the physics clauses scored per leaf (P-enumerated heads scored per clause;
prose "one way" heads scored at leaf level). `notes` cite `file:line`.

### integrations-security (19)

| leaf | clauses | holds | violates | partial | n/a-absent | notes (file:line) |
|---|---|---|---|---|---|---|
| secret-display-and-transfer | 1 (prose) | ✓ | | | (display) | Secrets leave the backend only via **withholding**: `#[serde(skip_serializing)]` on `job.rs:51` (callback_secret), `storage.rs:56` (watch secret), saved-search secret; pinned by `tests/triggers.rs:104`. No re-readable read path. Display-half n/a (no UI). Stronger than the corpus minimum (withhold > mask). |
| automated-credential-provisioning | 5 | | | | ✓ | No credential-provisioning subsystem. `app-provisioner` provisions *fetch targets*, not credentials. |
| credential-capture-form | 5 | | | | ✓ | No capture UI. Note: `engine.rs:897` browser-transact redacts sensitive inputs to `value:null` — independently reinvents P4 (withhold value from the event); `transact/src/lib.rs:554` test asserts "never republished". |
| least-privilege-scope-grant | 4 | | | | ✓ | No grant/scope ledger. Minor P4 datapoint: engine-claude `--allowedTools` is skipped when the list is empty (`lib.rs:119`), so empty = "CLI default", not "deny all" — but there is no grant system to score. |
| oauth-connect-flow | 6 | | | | ✓ | No OAuth. |
| credential-rotation-and-revocation | 4 | | | | ✓ | No credential lifecycle. Webhook secrets are long-lived plaintext, no `expires_at`, no rotate verb — matches P3's unanimous absence, but there is no subsystem to hold. |
| credential-readiness-resolution | ~3 | | | ✓ | | A real readiness resolver exists: `Requirement::Env(name).is_satisfied()` (`app.rs:837`), a **typed** requirement surfaced live in `GET /apps` (not a stored boolean). But it checks **existence only** — never authenticates or scopes — exactly the corpus's "existence properly, auth weakly, scope not at all". |
| connection-health-check | 6 | | | | ✓ | No credential/connection health probe. The governor's learned host penalties decay (a verdict-with-age, P1 shape) but that is host politeness, not a connection verdict. |
| credential-slot-binding | 4 | | | | ✓ | No slot/binding system. `Requirement::Env(name)` is a named reference (P1-aligned), but there is nothing to bind. |
| secret-and-pii-redaction | 1 (prose) | | | ✓ | | **Withhold** is used well (serde-skip = the corpus's strongest move; `census-common/src/lib.rs:368` `redact_key` scrubs `key=` before persisting provenance). But there is **no general redaction pass / entropy sweep / PII masking**; the export path streams raw record data (§5) and `error.rs:89` redacts *paths*, not secrets. Adequate for what exists (secrets are all serde-skipped); breadth unmet. |
| column-encryption-at-rest | 1 (prose) | | | ✓ | | **No encryption anywhere** (0 AES/GCM/nonce/iv hits in `migrations/`+`storage.rs`). By the leaf's own taxonomy, webhook HMAC signing secrets are the *"must-present-later"* kind → need reversible encryption; they are plaintext `TEXT` (`0003_orchestration.sql:4`, `0006_watches.sql:9`, `0021_ingress.sql:9`). Mitigated by single-tenant loopback, but **no threat model stated in code**. Leans violate on the specific clause; scored partial. |
| sync-reconciliation-and-conflicts | 1 (prose) | ✓ | | | | **Textbook derived-mirror hold.** `@pumper/sync` is one-way authoritative→derived; watermark from **data observed** (`sync.ts:82,110` `laterIso(..., rec.updated_at)`), advanced **only after the sink commits** (`sync.ts:93,128`); deletes via **tombstones** with a refuse-to-tombstone-everything guard (`datasets.rs:840`); DataHub governance acts on **transition not level** (`datahub.rs:1151`). The both-authoritative case is absent by design — the corpus's preferred simplification. |
| portable-export-bundle | 7 | | | ✓ | | `export_records` (`datasets.rs:389`) streams rows with **honest truncation framing** (a mid-stream error drops the closing `]`, row_failures counted+logged `datasets.rs:581`) — satisfies P5's spirit without a counts receipt. But **no bundle manifest/hash/signature**, and **no import path** (re-import = re-run the app), so P1–P4 are n/a and P5's "return the counts" is unmet. |
| vault-key-handling | 6 | | | | ✓ | No vault, no wrapping key. Remote shared secret + DataHub token are plaintext config, not a wrapped key. |
| filesystem-boundary | 1 (prose) | ✓ | | | | **Holds.** `safe_path_segment` (`app.rs:812`) rejects empty/`.`/`..`/separators/absolute; root from app state (`profiles_dir`); `require_safe_profile_name` runs **before** the cache lookup and any FS touch (`engine-http:440`); writes are **temp-file+rename** (`engine-http:177`). Forbidding all non-single-segment fragments is stronger than reject-non-`Normal`; only residual is a single-segment symlink (no canonicalize+`starts_with`), minor. |
| outbound-http-call | 6 | ✓ (P2,P5) | | ✓ (P3,P6) | | **P2 holds:** two-way bound — per-attempt timeout **and** an end-to-end `total_budget_secs` budget bounding the retry multiplication (`engine-http:538,889`). **P5 holds:** body bounded **while reading**, aborting the instant a chunk would exceed the cap (`read_bytes_capped:706`, `would_exceed_cap`). Typed transport classification, **not** message substrings, split out for testing (`TransportPredicates:816`). **P3 partial:** `redirect::Policy::limited` (`:390`) follows hops with **no per-hop re-validation**. **P6 partial:** an SSRF guard exists (`engine-remote::blocked_target:164` — refuses loopback/link-local/private/CGNAT + non-http, opt-out defaulted safe) **and self-documents its own limit**: "a hostname that **resolves into** a private range is not caught" (`:161`). The main scrape path (operator-catalog URLs) has no SSRF guard — defensible for single-tenant. |
| external-url-opening | 6 | | | | ✓ | A server never hands a URL to an OS handler. Subprocess spawn (`claude`/`cmd`) is a different leaf. |
| sql-console | 5 | | | | ✓ | No arbitrary-SQL surface. `routes/query.rs` and the datasets filter DSL parse operator filters into **parameterized** SQL (`JsonFilter`) — the corpus's preferred "type only the classifier can construct", but there is no query-string-as-program door. |
| cross-device-pairing | 6 | | | | ✓ | No pairing ceremony. The remote fabric authenticates with a **static shared bearer secret** (`x-pumper-remote-secret`), compared via fixed-size digest equality (`secret_matches:71` — timing-safe by construction), **no nonce/freshness** → P4-replayable. Opt-in, disabled by default, LAN-scoped. Absent-not-wrong. |

**Subtotal:** holds 4 · partial 4 · violates 0 · n/a-absent 11.

### platform-delivery (16)

| leaf | clauses | holds | violates | partial | n/a-* | notes (file:line) |
|---|---|---|---|---|---|---|
| environment-variable-configuration | 1 (prose) | ✓ | | | | Layering is correct: `load_dotenv` sets a key only if `env::var_os(key).is_none()` (`main.rs:419`) — real env wins. Frozen version layered UNDER runtime: `PUMPER_BUILD_ID` else `env!("CARGO_PKG_VERSION")` (`app.rs:745`, `main.rs:125`). Every config key optional with defaults (`config.rs`). Minor: `.env` empty-string values aren't filtered. |
| feature-flagged-compilation | 1 (prose) | ✓ | | | | Sanctioned use: `storage` feature gates the **optional sqlx dependency** so embedders (Personas) drop it via `default-features=false` (`core/Cargo.toml:6`). Engines are separate always-compiled crates, not cfg-toggled entry points — the anti-pattern surface doesn't exist. |
| compile-time-env-embedding | ~6 | ✓ | | | | `env!("CARGO_PKG_VERSION")` (compile error if absent — the corpus's "prefer `env!` over `option_env!`"), runtime `PUMPER_BUILD_ID` override. **No secret frozen** (secrets arrive at runtime via `.env`), **no build-machine path** frozen into the binary (`CARGO_MANIFEST_DIR` only in tests). |
| codegen-task-registration | ~4 | ✓ (P2) | | | (registration) | pumper commits **no** generated Rust artifacts; the largest generated surface — the OpenAPI spec — is regenerated at **runtime** (`/openapi.json`) and drift-tested, never committed. That is P2 ("prefer not committing the artifact") by construction; nothing to register. |
| bundling-native-assets | 7 | | | ✓ | | WASM plugins are runtime files (`data/plugins/`, gitignored), built by `just plugins-install`, and CI **verifies each installed module is executable** by loading it (`ci.yml:65`, `Plugins::has`) — the "verify at the far end" + inventory clauses. No vendored native binary with a pinned digest, so the byte-identity clause is n/a. |
| installer-acceptance-testing | 7 | | | | n/a-scope | No installer (single binary). **Translated analog present and strong:** `scripts/smoke.ps1` boots the real binary against a scratch config, drives one real job end-to-end, curls doctor/retention/enforcement/openapi/receipt (PASS/FAIL/SKIP), tears down — acceptance-testing the built artifact. |
| tauri-permissions-and-csp | 4 | | | | n/a-scope | No Tauri, no CSP. (A CORS allowlist for a trusted local UI exists — `config.rs:958` — but capability/CSP policy is Tauri-specific.) |
| release-pipeline | ~3 | | | | n/a-absent | No release automation (`ci.yml` only; binary built by `just build`). One version surface (`CARGO_PKG_VERSION`) is a positive datapoint, but there is no pipeline to score. |
| adding-a-ci-gate | 8 | ✓ | | | (census) | Gates **fail loudly, at error severity, with no masking**: `clippy … -D warnings`, no `\|\| true`/`continue-on-error` anywhere (`ci.yml` read in full); the plugin/artifact steps exist precisely because "a gate nobody deployed reads exactly like a gate that said yes" (`ci.yml:28`). Key conventions live as **unit tests** (P4 "belongs in the test suite") not CI config. The census/rules.json primitive is n/a-scope. |
| custom-lint-rule | ~5 | | | | n/a-absent | No bespoke AST linter (Rust = clippy). But the leaf's **goal** is met by the corpus's preferred means: `EngineSet::claude` `pub(crate)` is a **type that makes the wrong call unrepresentable** ("type over gate"), and inventory tests ship with their adversary (`llm_chokepoint.rs`, `removal_guard.rs`). Convergence, not a rule. |
| cross-artifact-drift-gate | 3 | ✓ | | | | **Holds.** OpenAPI route surface as a `BTreeSet` **set-equality** test (`routes/mod.rs:534` EXPECTED, catalog-exempt `:509`) — symmetric, catches add *and* remove; plus `removal_guard.rs`, `llm_chokepoint`/`fetch_chokepoint` EXPECTED inventories. Independent third list, not a copy. |
| secret-leak-scanning | 6 | | ✓ | | | **Violates.** No content-based scanner in CI or hooks (fmt/clippy/test/cargo-deny only), and **no recorded decision** to omit it. Name-based defence present (`.gitignore`). The repo ships `.env.example`, holds real secrets in a gitignored `.env`, and signs webhooks with plaintext HMAC secrets. |
| commit-path-gates | 6 | | | ✓ | | **Partial.** No pre-commit/pre-push hooks, no `lefthook.yml`, no installed `.git/hooks` — the only code gate is CI on push+PR ("no verdict on this machine"). The CI gate that exists is real (exits non-zero). A Claude **Stop hook** (`check-doc-sync.mjs`) gates docs, not code. Honest for a solo loopback repo. |
| live-ui-test-automation | ~4 | | | | n/a-scope | No UI. Translated analog: `crates/server/src/e2e/*` (engine_conformance, trigger_plugins, datahub_bridge) + `smoke.ps1` drive the real binary/engines — and, unlike the corpus's lament, these **do run in CI** (`ci.yml:66`). |
| rust-test-fixtures | 1 (prose) | ✓ | | | | **Holds.** Tests get the **production schema** via `Storage::connect` → "connect + migrate" (`testing.rs:61`, `sqlx::migrate!("./migrations")` `storage.rs:148`); rows built through production writers (`TestContext`/`ScriptedResearcher`/`TempStore`), no hand-rolled `CREATE TABLE` standing in for a production table. Anti-pattern-named tests throughout `crates/core/tests/`. |
| rust-unit-test-harness | 1 (prose) | ✓ | | | | **Holds.** `#[cfg(test)] mod tests` at file bottom (engine-claude, engine-http); `cargo test --workspace` is what CI runs (`justfile` `just test`); env-dependent tests `#[ignore]`d and run separately (`ci.yml:59`). The Windows comctl32/`TaskDialogIndirect` trap is Tauri-specific — n/a for a plain binary. |

**Subtotal:** holds 8 · partial 2 · violates 1 · n/a-absent 2 · n/a-scope 3.

### ai-agents (16)

| leaf | clauses | holds | violates | partial | n/a-absent | notes (file:line) |
|---|---|---|---|---|---|---|
| headless-model-call | 10 | ✓ (P5,P6,P7) | | ✓ (P2,P3) | | **P6 exemplary:** `AppContext::research` (`app.rs:422`) is the ONLY door; `EngineSet::claude` is `pub(crate)` → bypass is a **compile error**, backed by the `llm_chokepoint.rs` inventory (structural + inventory, two layers). **P5:** killed/failed calls still write a row — `meter_failed_spend` (`app.rs:249`), `cost_usd NOT NULL DEFAULT 0` with `detail='unmetered_timeout'` (`error.rs:52`). **P7:** the cron scheduler gets a per-firing budget (`scheduler.rs:468`). **P2 partial:** budget is `Option`, not a required field. **P3 partial:** the paying identity is the ambient CLI account, not a call argument. |
| model-and-effort-selection | 10 | | ✓ (P5) | ✓ (P1,P2) | | **P1 partial:** an unknown role now **refuses** instead of silently defaulting (`engine-claude/lib.rs:41` — the sharp case, held), but a fully-unconfigured model still rides the CLI default ("resolves upward"). **P2 partial:** model/effort/budget are three separate `Option` fields, resolved together but individually settable. **P5 violates:** the choice is **not recorded on the row** — `cost_events` has no model or effort column (`0007_cost_ledger.sql`), so a run's model is unreconstructable. |
| failure-recovery-strategy | 8 | ✓ | | | | **Strong hold.** Failure classified from a **typed** `ClaudeFailure` enum minted from process facts ("typed rather than stringly so the ledger's detail cannot drift", `error.rs:59`); engine-http likewise ("deliberately **not** message substrings", `:792`). Eligibility derived from the class (`is_terminal_for_job:412`), not a hardcoded subset. Degrade (drop Claude tier on budget-exhaust) is recorded (P8). No model/provider fallback exists (n/a, not a violation). |
| structured-output-extraction | 1 (prose) | | | ✓ | | Engine returns `Option`, not `Result` carrying a reason (`engine-claude/lib.rs:370`) — P1's exact anti-shape at the salvage layer (envelope-level failures *do* return typed `Unparseable`). **Spend is always recorded regardless of parse outcome** (meter runs before parse — good), but the parse-failure **reason is not written to the paid row**. The `envelope_text` fix (schema result → not silently `""`) shows the "manufactured default poisons the cache" anti-pattern was found and closed. |
| prompt-assembly | 7 | | | ✓ | | **No single assembler** (inline `format!` per call site — violates P(a)); **no prompt hash/length** on the row (violates P(f)). But the highest-risk clause (P(c) fence untrusted content) is **low-exposure by architecture**: scraped bytes are not spliced into prompts — the model fetches pages via its own tools; apps embed only the operator query param + URL (`apps/research/lib.rs:505`, `fetcher.rs:776`). |
| spend-ceilings | 1 (prose) | | | ✓ | | Good architecture — **pre-call gate** (`require_budget:276`, before the spend), prefer **degrade over disable** (soft tier-downgrade), running total seeds from the ledger and ignores NaN/negative deltas (corrupt-handling). But **unconfigured = unlimited**: `budget_usd: Option`, `None` = unlimited, config default `None` — the corpus wants a real positive default / a `Ceiling::Unlimited` variant somebody typed. And **no refusal-count query** (the corpus's "that number is zero"). |
| llm-spend-accounting | 1 (prose) | | | ✓ | | Takes the **vendor's own `total_cost_usd`** from the terminal event; failure still recorded; distinguishes `cost_unreported`/`unmetered_timeout` from a genuine `$0` (`app.rs:791`). Per-job gate re-aggregates via `SELECT SUM(cost_usd)` (`costs.rs:142`). **Gaps:** column is `NOT NULL DEFAULT 0`, not nullable; **cache tokens dropped**; **no model/owner column** — the "60% unattributable" analog. One migration fixes all three. |
| autonomy-gating | 1 (prose) | | | ✓ | | Fire-time re-read of the budget from the **live row** (`scheduler.rs:468`); govern/refresher/catalog-auto **default OFF**; DataHub `cost:pause` per-app kill switch forces budget to `$0` (`datahub.rs:1009`); door-parity refusal recorded without firing; jobs claimed by **UNIQUE `idempotency_key`** (a crash cannot re-fire). **Gaps:** no *single* global on/off (control is the sum of switches); unconfigured budget = unlimited (same defect as spend-ceilings). |
| untrusted-definition-validation | 1 (prose) | | | ✓ | | **WASM sandbox exemplary:** CPU **fuel** + memory cap + all-store-growables capped + **fresh Store per call** + **empty Linker** (a plugin declares no imports, cannot reach the host) + global admission semaphore (`engine-wasm/lib.rs:191,560,582`); load-time ABI check (`:143`). `[remote] allow_private_targets` is a **config key defaulted safe**, not a caller-writable definition field — the corpus's *preferred* placement. **Gap:** catalog/plugin manifest are **serde passthrough** (typed deserialize), not field-by-field reconstruction; the trigger-hook **fail-open** path is deliberate and recorded (`plugin_missing` ledger row, `triggers.rs:890`). |
| agent-dispatch | ~9 | | | | ✓ | No multi-agent/session dispatch. The job queue's `idempotency_key` (computed from the request, deduped via UNIQUE, re-reads the winner on race) independently satisfies the core "compute a key from entities, check it, persist it" physics for the job-dispatch case — convergence, not the leaf. |
| informed-consent-gate | ~10 | | | | ✓ | No interactive consent (unattended server). Note: `[claude] skip_permissions` → `--dangerously-skip-permissions` is a config-level suppression with no surface to disclose on — consistent with the model, but the inverse of the leaf. |
| ai-draft-preview-apply | 9 | | | | ✓ | No draft→preview→apply loop. Recipes have a validated/unvalidated gate (a distant "prove before you trust on the fetch path" analog), but no human preview. |
| model-composed-ui | 1 (prose) | | | | ✓ | No UI. Model JSON is schema-validated (see structured-output-extraction); nothing renders it. |
| human-review-queue | 1 (prose) | | | | ✓ | No human-review queue; no reviewer verdicts. |
| selective-per-item-verdicts | ~9 | | | | ✓ | No review UI, no per-item verdicts. |
| findings-triage-queue | ~10 | | | | ✓ | No findings queue. The `*-preview`/doctor endpoints are dry-runs, not a triaged queue. |

**Subtotal:** holds 2 · partial 7 · violates 0 (one violate *clause* inside model-and-effort P5) · n/a-absent 7.

**Shard totals (51 leaves):** **holds 14 · partial 13 · violates 1 · n/a-absent 20 · n/a-scope 3.**
Plus 3 frontend domains → n/a-absent.

**Coverage:** scored **deep** (read the exact code and/or executed an absence/aggregation grep) —
engine-claude (full read + tests), engine-http (full read of the fetch path + SSRF guard),
`AppContext` metering + budget (via corroborated sub-investigation + `app.rs` reads), `deny.toml`
(full), `ci.yml` (full), `blocked_target`/`secret_matches`/`verify_signature`,
`safe_path_segment`, the WASM host, `Storage::connect` test path, the chokepoint/drift inventory
tests — ~14 deep dives, within the cap. **Shallow (head-only)** — none reported as a verdict.
**Skipped:** the `jobs.status` CHECK-probe and data-layer leaves belong to another shard (v1
already executed them). No cargo/build/run.

---

## 2. Deviations (nothing applied) — worth a fix, with severity + held reason

1. **The spend row carries no dimension.** *Severity: medium.* `cost_events` (`0007_cost_ledger.sql`)
   has `engine ('http'|'browser'|'claude')` but **no model, no effort, no owner column**, and drops
   the vendor's cache-token counts. This defeats `llm-spend-accounting`'s "stamp the dimension"
   and `model-and-effort-selection` P5 ("record the choice with the thing it caused, in the same
   row"): a run's model is unreconstructable after the fact. *Fix:* add `model TEXT, effort TEXT,
   cache_read_tokens INTEGER, cache_creation_tokens INTEGER` and populate from the resolved
   argv/envelope in `meter`. *Held:* schema migration + metering edit, out of a read-only run.

2. **The ceiling defaults to unlimited.** *Severity: medium.* `budget_usd: Option<f64>`, `None` =
   unlimited, config default `None` (`config.rs:1385,1393,1401`), and a scheduled firing with a
   `NULL` row budget is likewise unbounded. Violates `spend-ceilings` P1 / `autonomy-gating`
   ("an unconfigured ceiling must **refuse**"). *Fix:* a real positive `*_DEFAULT`, or spell
   unlimited as a variant somebody typed (`Ceiling::Unlimited`); resolve unconfigured→refuse.
   *Held:* behaviour change to the default posture; a maintainer decision.

3. **No content-based secret scanning; no recorded decision to omit it.** *Severity: medium.*
   `ci.yml` runs fmt/clippy/test/cargo-deny only. Violates `secret-leak-scanning` P1+P4. The repo
   ships `.env.example`, gitignored `.env` with real secrets, and plaintext webhook HMAC secrets.
   *Fix:* a `gitleaks`/`trufflehog` job scoped to the diff, OR a commented explicit decision that
   content scanning is out of scope and why. *Held:* CI workflow edit, out of scope.

4. **"Must-present-later" secrets stored plaintext, with no stated threat model.** *Severity:
   low–medium.* `jobs.callback_secret`, `watches.secret`, `saved_searches.secret`,
   `ingress_sources.secret` are plaintext `TEXT`. By `column-encryption-at-rest`'s own taxonomy
   these are the one kind that needs reversible encryption. *Fix:* either encrypt them (with the
   `encrypt_field`-style `(value, iv)` + round-trip-verify shape) **or** write the threat-model
   sentence the leaf demands ("single-tenant loopback; disk-theft is the only vector encryption
   would add; no key infra"). *Held:* crypto + key-management design decision.

5. **Extractor failure is an `Option`, and the parse-failure reason is not recorded on the paid
   row.** *Severity: low.* `structured-output-extraction` P1 wants `Result<T, _>` carrying the
   reason, written against the turn that was paid for. *Fix:* return the serde error + a bounded
   head of the offending text; write `{"parse_failure":true}` to the ledger detail. *Held:* an
   API-shape change across engine + apps; the money is already never lost (meter runs first).

6. **Prompt assembly has more than one door and records no prompt hash.** *Severity: low* (mostly
   moot — untrusted scraped bytes don't reach the prompt). *Fix:* a single assembler returning a
   non-concatenable newtype; record the assembled prompt's SHA-256 + length on the row. *Held:*
   refactor across ~6 app crates.

7. **Redirect hops and DNS-rebinding are un-revalidated on the fetch path.** *Severity: low*
   (config-supplied URLs, single-tenant). `outbound-http-call` P3/P6. The fabric guard already
   **documents** the DNS gap in writing (`engine-remote/lib.rs:161`) — the honest posture the
   corpus asks for. *Fix (if ever multi-tenant):* resolve-then-pin inside the client. *Held:*
   scope + a network-client change.

---

## 3. Enrichment — candidates to flow back to the corpus

| candidate | file:line | physics argument | in_corpus | lane |
|---|---|---|---|---|
| **Dependency-audit gate (cargo-deny)** | `deny.toml` (full) + `ci.yml:70-84` | Any redistributable binary ingesting untrusted input independently reinvents four supply-chain questions — is there a CVE (`advisories`), does every crate still come from a trusted source (`sources` hard-denies non-crates.io), is a vulnerable copy hiding behind a second major (`bans`), is the license compatible. Every waiver names **exposure + upgrade path + a follow-up to drop it** (`deny.toml:45-72`); `licenses` is written, verified, and its one-step activation is **documented rather than silently skipped** (`:76-87`). Not a house convention — the same shape any cargo/npm/pip project in that position must build. | **absent** — no leaf in the 206 has dependency auditing as its subject; `cargo-deny` appears only tangentially in `secret-leak-scanning`/`adding-a-ci-gate`. | platform-delivery / infra |
| **EXPECTED-set inventory test as a portable convention keeper** | `routes/mod.rs:534`, `core/tests/removal_guard.rs`, `llm_chokepoint.rs:12`, `fetch_chokepoint.rs` | A convention stated in prose fails silently on the first violator; a hardcoded EXPECTED set compared by **equality** makes both an *addition* and a *removal* fail — the inventory direction `cross-artifact-drift-gate` prescribes for generated artifacts, generalised to any in-repo invariant (route surface, "every model call goes through the chokepoint", "this seam is called nowhere else"). pumper states the doctrine directly and uses it 4× across three crates. | **refines-existing** → `cross-artifact-drift-gate` (carries inventory-direction) + `custom-lint-rule` (carries "test not prose"). The *test-embodied EXPECTED-set as a general keeper* is a distinct, portable manifestation worth naming. | platform-delivery / code-quality |
| **Structural chokepoint via visibility + inventory (the two-layer meter guard)** | `app.rs:422` (`pub(crate)`) + `llm_chokepoint.rs:1-19` | The strongest form of `headless-model-call` P6 ("metering must be unreachable-around, not merely available"): a **compile error** (`pub(crate)` on the engine) prevents bypass from *outside* the crate — the compiler enforces it on every build — and an inventory test pins the direct call sites *inside* the crate, where visibility cannot reach. "Withhold the unmetered door" made mechanical. | **refines-existing** → `headless-model-call` P6 / `llm-spend-accounting`. An independent, textbook realization of the clause; sharpens the "one way" with the visibility+inventory pair. | ai-agents |
| **WASM operator-plugin sandbox contract** | `engine-wasm/lib.rs:191,534,560,582` | An operator-supplied plugin is untrusted code; the contract that makes it safe is a **fresh Store per call** (fuel + memory + all-store-growables capped) linked against an **empty Linker** (no host imports reachable) under a global admission semaphore. `untrusted-definition-validation` covers *definition* reconstruction but not the *execution-sandbox* half; pumper's is exemplary and complete. | **refines-existing** → `untrusted-definition-validation` (adds the resource-sandbox + import-isolation clauses) / `panic-isolation`. | ai-agents / integrations-security |
| **Deliberate fail-open at a pipeline edge, recorded loudly** | `triggers.rs:302,890` + `ci.yml:24-29` | A mis-deployed trigger-hook plugin takes the **same fail-open path** as a predicate that passed — the hop still fires — *by contract*, and every such hop writes a `plugin_missing` ledger row so "a gate nobody deployed" is visible rather than silent. The CI comment states the physics: "a gate nobody deployed reads exactly like a gate that said yes." | **refines-existing** → `admission-control` / `error-surfacing-policy`. A convergence datapoint: announce the degradation at the boundary, in durable state, before trusting the edge. | ai-agents / infra |

---

## 4. Methodics

- **Executed vs shallow.** All 14 deep dives rest on reading the exact code and/or an executed
  grep (absence proofs for the ~20 n/a-absent leaves; `SUM`/`COALESCE` aggregation for the spend
  clauses; SSRF-guard scope; workflow inventory `ls .github/workflows` = `ci.yml` only). No
  verdict is head-only. **No cargo/build/run** (standing rule); the `jobs.status` CHECK behaviour
  probe was v1's data-shard, not re-run here.
- **Two-implementation corroboration.** The metering chokepoint and the secret-handling sinks were
  each established by a direct read **and** an independent sub-investigation; they agreed
  (chokepoint = `AppContext::research`, `pub(crate)` + inventory; secrets = serde-skip on 4 fields,
  no encryption, plaintext HMAC). The SSRF question was cross-checked two ways: a broad
  private-IP/metadata grep and a direct read of `blocked_target` — agreed, including the
  self-documented DNS-rebinding limit.
- **Self-corrections during the run.** (1) Initially read `feature-flagged-compilation` as
  n/a-absent; on reading `core/Cargo.toml:6` corrected to **holds** — `storage`/`test-support`
  are purposeful optional-dependency features (the sanctioned use), not entry-point toggling.
  (2) Initially scored `llm-spend-accounting`'s aggregation clause as a straight violate
  (in-memory increment); on finding `costs.rs:142` re-aggregates via `SELECT SUM(cost_usd) WHERE
  job_id=?` at context build, softened to **partial** — the gated per-job figure re-aggregates at
  seed; the in-process total is a mirror. (3) `codegen-task-registration` reframed from n/a to
  **holds(P2)** once it was clear pumper's largest generated surface (the OpenAPI spec) is never
  committed — the corpus's own ideal.
- **Instrument gaps (disclosed).** `rg` not on PATH — the ripgrep-backed Grep tool was used
  throughout; `sqlite3` was available but the DB-behaviour probes were out of this shard's scope.
  No fleet-wide claim rests on an absent instrument.
- **Entanglement caveat.** pumper is a measured sibling in composing some platform-delivery
  leaves, so its `cross-artifact-drift-gate` / `adding-a-ci-gate` holds are corroboration a step
  short of fully-blind — but the leaves are composed against Personas as "this repo", cite pumper
  as external, and no shard leaf is authored from pumper's ledger. No verdict is scored
  `holds(self)`.
