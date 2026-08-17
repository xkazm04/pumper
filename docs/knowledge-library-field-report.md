# Knowledge-Library Field Report — pumper

Run of the Knowledge-Library Field-Test Kit v1 against `pumper`, a sibling of the
Personas golden-path corpus. Read-only against source; the only file written is
this report. No commits, no source edits, no destructive applies.

**Headline:** 11 leaves scored across **29 physics clauses** — **7 leaves HOLD**
(swallowed-error-telemetry, cross-artifact-drift-gate, connection-pool-pragmas,
backfill-migration, conditional-write) or **n/a by honest absence**
(data-normalization-migration, ownership-verification), and **4 PARTIAL/violate**
(secret-leak-scanning, commit-path-gates, schema-inexpressible-invariant,
status-transition-rules). pumper's data layer is unusually strong on the
concurrency clauses — `conditional-write` and `backfill-migration` are
**exemplary** (single-statement CAS with a `(status, attempts)` fencing token;
keyset-paged backfills with anti-pattern-named tests). Its two real gaps are both
"the store does not refuse the write": the central `jobs.status` state machine
carries **no CHECK** (proven by execution — it accepts `'NONSENSE_STATE'`), and
there is **no secret scanner** at all. **Primary enrichment: a supply-chain /
dependency-audit gate** (`deny.toml` + the CI `cargo-deny` step) — genuinely
absent from the corpus, physics-grade, data/infra lane.

---

## 0. Orientation

pumper is a **local-first scraping service**: one Rust binary exposing an axum
HTTP API, a durable SQLite (WAL) job queue, and pluggable scraping engines.
Cargo workspace, edition 2021, `resolver = "2"`. Data/backend-oriented.

- **Size (tracked files by extension, `git ls-files`, executed):** 819 tracked
  files — 435 `.md`, **235 `.rs`**, 49 `.toml`, **40 `.sql`**, 24 `.json`,
  10 `.html`, 7 `.ts`, 6 `.jsonl`, 3 `.mjs`.
- **Crates:** `core` (traits + models + storage/job-queue + datasets + resilience
  + config), `engine-{http,browser,claude,wasm,search,archive,remote}`, `apps/*`
  (one per scraping use case), `server` (axum routes + worker pool + scheduler +
  webhooks + triggers + SSE + datahub).
- **Persistence:** SQLite via **sqlx** (not rusqlite/r2d2), one `SqlitePool`,
  40 forward-only `.sql` migrations run by the sqlx migrator.
- **Governance artifacts present:** `CLAUDE.md`, `.claude/CLAUDE.md` (binding
  policy), `context-map.json` (dual Vibeman/Personas, 46 contexts / 8 groups),
  `ONBOARDING.md`, `deny.toml`, `justfile` (canonical task runner),
  `.github/workflows/ci.yml`, a versioned Claude Stop hook (`.claude/settings.json`
  → doc-sync). **No lefthook, no installed git hooks** — the only enforcement
  boundary is CI.
- **Auth posture (deployment.md §Auth):** *no inbound authentication on any
  route*; single-tenant, loopback-bound (`127.0.0.1:8088`). The safety boundary
  is the network, not the application — which makes `ownership-verification` n/a.

**Instrument note (honesty):** the `rg` binary is **not** on PATH in this
environment. The Grep tool is ripgrep-backed and was used for all content
searches; `sqlite3` (3.50.6) is available and was used for the throwaway-DB
probe; `git ls-files` produced the file census. No cargo build/test/run was
invoked (per standing rules).

---

## 1. Scorecard

| leaf | clause (physics) | verdict | evidence (file:line, count, executed?) |
|---|---|---|---|
| **swallowed-error-telemetry** | one chokepoint so the call site can't decide | **holds** | `sentry_tracing::layer()` at `crates/server/src/main.rs:174` maps `error!`→Sentry event, `warn!`/`info!`→breadcrumb — the exact brainiac convergence the leaf names. Executed (read). |
| | user vs operator = a different door, not an `if` | **holds** | operator door = `tracing::error!`→Sentry (`main.rs:194-210`); user door = `crates/server/src/routes/error.rs`→HTTP response. Two typed doors. |
| | a swallowed failure leaves a structured, identifying trail | **holds** | structured kv fields on `error!` (`main.rs:196-202`). Sampled all 11 core `let _ =` sites — every one is a best-effort compensating action (`ROLLBACK`, `remove_file`) while the real error propagates via `?`; none is a silent loss. Executed (Grep, count=11 core / 106 workspace). |
| **secret-leak-scanning** | the *control* when the scanner is absent must be a recorded decision | **violates** | No secret scanner anywhere: `ci.yml` runs fmt/clippy/test + `cargo-deny` only; zero gitleaks/trufflehog/detect-secrets in CI or hooks (Grep, executed). No recorded decision to omit it. |
| | name-based AND content-based defence — you need both | **violates** | Name-based present (`.gitignore:21` `.env`, `*.local`); content-based defence **absent**. The leaf: these are not substitutes. |
| | redaction reach | **holds** | secrets are HMAC-only + `#[serde(skip_serializing)]` on `Job.callback_secret` (`crates/core/src/job.rs:51`); **0** `tracing::*!` sites interpolate secret/token/password/api_key (Grep regex, executed, 0 matches). |
| **commit-path-gates** | site the *blocking* gate at the last reversible moment | **partial** | No pre-commit/pre-push hook, no installed `.git/hooks/*`; the only gate is CI on push+PR (`.github/workflows/ci.yml`) = "no verdict on this machine". Honest for a solo loopback repo, but the last local reversible moment is ungated. |
| | never let an invocation swallow its own verdict | **holds** | `ci.yml` has **no** `\|\| true`, no `continue-on-error`, no `allow_failure`; `cargo clippy … -D warnings` (`:47`) gates on lints. Executed (read full file). |
| | make absence loud | **holds** | the plugin build/install/artifact-test steps exist precisely because "a gate nobody deployed reads exactly like a gate that said yes" (`ci.yml:24-29,55-68`) — fail-loud reasoning in the job comments. |
| | commit-message vocabulary = one enum, N consumers | **n/a** | no commitlint / changelog-classifier in this repo; no message gate to keep in sync. |
| **cross-artifact-drift-gate** | regenerate-and-compare, not diff; act on exit code | **holds** | `crates/server/src/routes/mod.rs:530-655` — the OpenAPI spec is generated from `openapi_router` and compared by **set-equality** against a hand-maintained `EXPECTED` route inventory (`:538-635`) in a `#[test]`. |
| | inventory direction — catch removals/orphans a diff misses | **holds** | `BTreeSet` equality is symmetric: adding *or* removing a route fails the test (`:536-537` comment states exactly this). Second instance: `crates/core/tests/removal_guard.rs` inventories misuse of the `detect_removed` seam. |
| | a gate over 2 artifacts from 1 source only proves the copy is current | **holds** | router+spec share `openapi_router`, but `EXPECTED` is an **independent third list** — an edit to a route (not the generator) fails it, so it is a real check, not a third copy. |
| **schema-inexpressible-invariant** | choose a keeper; prefer store-refuses where cheap; NOT NULL beside CHECK | **partial** | store-refuses used well where cheap: `resilience state` CHECK (`0020:27`), `triggers.source_kind` CHECK (`0002/0014`), `source_dataset <> target_dataset` cross-column CHECK, `jobs.idempotency_key` UNIQUE (`0011:4`). Only **5** CHECK lines across 40 migrations (Grep, executed). |
| | the central state vocabulary must have a keeper stronger than a comment | **violates** | `jobs.status` = `TEXT NOT NULL DEFAULT 'queued'` with **no CHECK** (`0001_init.sql:5`). **Executed probe** (throwaway sqlite3 DB): `INSERT … status='NONSENSE_STATE'` → **exit 0, row landed**; the same shape into the resilience `state` CHECK → **exit 19, CHECK constraint failed**. `webhook_deliveries.status` (`0010:11`) names its vocabulary only in a `--` comment. |
| **status-transition-rules** | states in schema (CHECK NOT NULL) | **partial** | resilience `SourceState`: states in schema (CHECK, executed-rejected garbage). `jobs.status`: **not** in schema (executed-accepted garbage). |
| | legal edges in a closed type | **partial** | `SourceState` has `next_state()` (`crates/core/src/resilience/detect.rs:833`) — a pure closed transition. `JobStatus` (`job.rs:8`) has only `as_str`/`parse`, **no `transition_to`**; job edges are enforced only by per-statement `WHERE status='running' AND attempts=?` CAS. |
| | one write door taking the enum; CAS with old state in WHERE; return affected-row count | **partial→holds on CAS** | no single door — `claim`/`complete`/`fail`/`cancel`/`reset`/`retry_bulk` each write a status literal (`storage.rs:300-476`). BUT every one enforces the from-state in `WHERE` and returns the row-count verdict — so illegal *edges* are blocked at the DB even though illegal *values* are not. |
| **connection-pool-pragmas** | one batch / one applier, no drift | **holds** | single prod pool config (`crates/core/src/storage.rs:176-184`), shared to `cache`/`datasets` via `pool()` clone; the only other `SqlitePoolOptions` in `src` is a read-only test copy (`tests/migrations.rs:306`). Executed (Grep, 1 prod site). |
| | verify the pragmas that fail silently (foreign_keys, journal_mode) | **n/a / partial** | FK-verification is **n/a**: the schema has **0** `REFERENCES`/`ON DELETE CASCADE`/`FOREIGN KEY` (Grep, executed, 0) — an unverified `foreign_keys` cannot silently disable a cascade that does not exist. `journal_mode(Wal)` is set via options but not read back (minor). |
| | always set a connection acquire timeout | **holds** | not set explicitly → sqlx `SqlitePoolOptions` default `acquire_timeout` (30 s) applies, so pool exhaustion is an error, not a hang. |
| | size the pool for the longest hold, with a comment; bound the WAL by size | **partial** | `max_connections(8)` with no comment naming the workload; **no `journal_size_limit`** (WAL bounded only by sqlx's default autocheckpoint). Minor optimisations. |
| **backfill-migration** | P1 return a receipt (population/handled/refused), not a scalar | **holds** | `search-backfill` returns `DatasetReport{indexed,purged}` + `doc_count` (`crates/server/src/bin/search-backfill.rs:81-94`). Executed (read). |
| | P2 derive "already done" from the destination; idempotent | **holds** | same `SearchDoc::from_dataset_record` builder as the live path, stable `<app>:<dataset>:<key>` ids, upsert not duplicate, "safe to run against a partially-populated index" (`:10-12`). |
| | P5 a zero must say WHICH zero it is | **holds** | `empty_scope_error` + tests `a_typod_scope_is_not_reported_as_success` / `an_empty_scope_is_not_reported_as_success` (`:228-236,395-476`) — a typo'd/empty scope errors instead of printing `0 … complete` exit 0. Exemplary — the exact P5 physics. |
| | P7 bound the read, not only the write | **holds** | keyset paging, `INDEX_CHUNK=500` doubles as page size "so nothing is read that isn't about to be written" (`:37-40,98-106`); the old `LIMIT 1_000_000` ceiling was removed, with a test (`a_dataset_larger_than_one_page_is_not_half_indexed`). |
| | P6 a per-row failure must not look like the end | **holds** | errors propagate via `?`/`anyhow::bail!`; no silent swallow that could terminate the drain early. |
| **data-normalization-migration** | guard on rows; close the door; report unmappable; close the vocabulary | **n/a** | migrations are **forward-only DDL** — 0 row-rewriting `UPDATE`/`INSERT…SELECT` in `crates/core/migrations/*.sql` (Grep, executed; only comments mention backfill). Row work lives in binaries, not migrations. Additive CHECK widening (`source_kind` +`external`, `0002`) needs no value remap. sqlx also provides the migration ledger this leaf notes personas lacks. |
| **conditional-write** | precondition in WHERE; affected-row count IS the return value; never Result<()> | **holds** | every job mutation: `complete`→`Result<bool>`, `fail`→`Result<Option<JobStatus>>`, `cancel`→`Result<Option<String>>`, `reset`→`Result<Option<Job>>` (`storage.rs:319-476`). Precondition in `WHERE … AND status=… AND attempts=?`. Never `Result<()>`. |
| | single-statement CAS, not BEGIN/SELECT/UPDATE/COMMIT | **holds** | `claim` = one `UPDATE … WHERE id=(SELECT … WHERE status='queued' … LIMIT 1) RETURNING` (`:299-311`). |
| | a claim needs a fence that re-arms | **holds** | `(status, attempts)` is a fencing token: a reclaimed job advances `attempts`, so a stale worker's write matches no row (documented `:314-318,419-424`). |
| | INSERT-race / dedup handled | **holds** | `enqueue_dedup` + `idempotency_key` UNIQUE index; on a lost insert race it re-reads the winner (`:246-259`). |
| **ownership-verification** | (entire subject) | **n/a** | single-tenant, unauthenticated, loopback-bound service (deployment.md §Auth, executed read) — no session/persona/tenant/owner rows. Clause (g) ("don't build a check that reads ownership off the wire for a single-tenant surface") is *satisfied by restraint*: pumper adds no `caller_*_id` params; app/dataset is organisational, not a security boundary. |

**Summary:** 29 physics clauses scored — **17 holds / 3 violates / 6 partial /
3 n/a** (leaf-level: 5 holds, 2 n/a, 4 partial-or-violate).

---

## 2. Deviations (APPLY lane — nothing applied)

1. **`jobs.status` has no CHECK — the central state machine is unconstrained.**
   *Site:* `crates/core/migrations/0001_init.sql:5`. *Proven:* throwaway sqlite3
   DB accepted `status='NONSENSE_STATE'` (exit 0) vs the resilience `state` CHECK
   rejecting the same (exit 19). *Fix:* add
   `CHECK(status IN ('queued','running','succeeded','failed','cancelled'))` beside
   the column, listing exactly the strings `JobStatus::as_str` emits (`job.rs:17`).
   *Severity:* medium — the CAS-in-`WHERE` blocks illegal *edges*, but a typo or a
   future second writer using raw SQL can still land an illegal *value* silently.
   *Behaviour change?* Adding a CHECK to a table with existing rows is a table
   rebuild in SQLite and could reject legacy rows. **Held:** it is a schema
   migration with a data-shape precondition, not a same-session edit — flag for the
   maintainer to run behind a `COUNT(*) WHERE status NOT IN (…)` guard first.

2. **`webhook_deliveries.status` keeps its vocabulary only in a comment.**
   *Site:* `crates/core/migrations/0010_webhook_deliveries.sql:11` (`-- 'pending' |
   'delivered' | 'failed'`, plus `'dead'` used in code at `storage.rs:1525`).
   *Fix:* same as (1) — a CHECK listing all four live values. *Severity:* low.
   **Held:** same migration/behaviour-change caveat; also verify `'dead'` is in the
   set (the comment omits it), which is itself evidence the comment already drifted.

3. **No content-based secret scanning; no recorded decision to omit it.**
   *Site:* `.github/workflows/ci.yml` (fmt/clippy/test + `cargo-deny` only).
   *Fix:* add a `gitleaks`/`trufflehog` job scoped to the diff, OR — following the
   corpus's own clause 1 — record an explicit, commented decision that content
   scanning is deliberately out of scope and why. *Severity:* medium — the repo
   ships `.env.example`, holds real secrets in a gitignored `.env`, and signs
   webhooks with per-delivery HMAC secrets; name-based `.gitignore` defence "protects
   by name; a leak arrives under a name nobody anticipated." **Held:** adding a CI
   job is a workflow edit, out of this read-only run's scope.

4. **No local pre-push gate.** *Site:* repo root (no `lefthook.yml`, no installed
   `.git/hooks`). *Fix:* a pre-push hook running `just ci` (or its fast subset) puts
   a verdict on the machine before work leaves the box. *Severity:* low for a solo
   loopback repo where CI-on-PR is the effective boundary. **Held:** tooling/config
   addition, out of scope.

5. **`connection-pool-pragmas` minor gaps:** no `journal_size_limit` (WAL size is
   uncapped between autocheckpoints) and `max_connections(8)` carries no comment
   naming the workload that forced it (`storage.rs:181-182`). *Severity:* low —
   optimisations, not correctness; the FK-verification clause is genuinely n/a
   (0 foreign keys in the schema). **Held:** behaviour-adjacent tuning.

---

## 3. Enrichment (BRING-BACK lane)

### A. Supply-chain / dependency-audit gate (PRIMARY, data/infra lane)
*Name:* **dependency-audit-gate** (working title). *Evidence (executed reads):*
`deny.toml` (whole file) + `.github/workflows/ci.yml:70-84` (`cargo-deny check
advisories bans sources`). The policy is unusually disciplined: every advisory
waiver carries a `reason` naming the **exposure**, the **upgrade path**, and a
**follow-up to drop the waiver** (`deny.toml:45-72`); `sources` hard-denies any
non-crates.io registry/git (`:155-163`); `licenses` is written, verified, and its
**one-step activation follow-up is documented rather than silently skipped**
(`:76-87`). *Physics argument:* any redistributable binary that ingests untrusted
input (this one crawls the web and runs operator WASM) independently reinvents the
four supply-chain questions — is there a CVE, does every crate still come from a
trusted source, is a vulnerable copy hiding behind a second major, is the license
compatible — and a waiver without a reasoned exposure is a silent risk the next
reader cannot audit. This is not a house convention: it is the same shape any
`cargo`/`npm`/`pip` project in that position must build. *in_corpus:* **no** — no
leaf in `index.json`'s 199 has dependency auditing as its subject; `cargo-deny`
appears only tangentially in `secret-leak-scanning` and `adding-a-ci-gate`. The
closest neighbours (`secret-leak-scanning`, `outbound-http-call`,
`rendering-untrusted-content`) are about different subjects.

### B. EXPECTED-set inventory test as a portable convention keeper (code-quality)
*Name:* **inventory-test-convention** (refines `cross-artifact-drift-gate`).
*Evidence:* `crates/server/src/routes/mod.rs:530-655` (the route surface as a
`BTreeSet` set-equality test); `crates/core/tests/removal_guard.rs` (a seam that
must be called nowhere else); `.claude/CLAUDE.md:35` states the doctrine directly:
*"Conventions are enforced with an inventory test (EXPECTED-diff idiom), not with a
sentence in a doc."* *Physics argument:* a convention stated in prose fails
silently on the first violator; encoding it as a hard-coded EXPECTED set compared
by equality makes both an *addition* and a *removal* fail — the same inventory
direction `cross-artifact-drift-gate` prescribes for generated artifacts,
generalised to any in-repo invariant (route surface, "all sites call helper X",
"this seam is called nowhere else"). *in_corpus:* **partially** —
`cross-artifact-drift-gate` carries the inventory-direction physics and
`custom-lint-rule` carries the convention-enforcement idea, but the *test-embodied
EXPECTED-set idiom as a general keeper* is a distinct, portable manifestation worth
naming. Flowing it back sharpens `cross-artifact-drift-gate`'s §"one way".

### C. Fail-open guarantee announced loudly at the boundary (data/ops)
*Name:* **fail-open-observability**. *Evidence:* `crates/server/src/main.rs:183-211`
(`log_contract_observability`) — when the catalog will not parse, every job silently
skips declared-contract enforcement (fail-open by design), so it logs `error!`
**once, before the first job runs**, naming that the fleet is checking zero contracts,
while delivery continues. The comment states the physics: *"a gate nobody deployed
reads exactly like a gate that said yes."* *Physics argument:* a configured safety
guarantee that degrades fail-open is invisible by construction — the only defence is
a loud, once-per-boot announcement at the boundary. *in_corpus:* **partially** —
`error-surfacing-policy`, `admission-control`, and `swallowed-error-telemetry` touch
fail-open, but none centres the "announce the degradation at the boundary before any
work runs" clause. A convergence datapoint, not a wholly new leaf.

### D. Pre-migration snapshot ritual (data)
*Name:* refines `destructive-schema-change` / `boot-migration-step`. *Evidence:*
`crates/core/src/storage.rs:185-194` — `backup_before_migrations` snapshots the DB
before the migrator advances the schema; a no-op for fresh/up-to-date/in-memory DBs
(so the test harness never writes a backup) via `backup::backup_decision`. *Physics
argument:* forward-only migrations that rewrite rows are irreversible; a
pre-migration snapshot is the only undo, and gating it on "not fresh / not in-memory"
is what keeps it from taxing the test path. *in_corpus:* **likely yes** —
`destructive-schema-change` and `schema-change` already discuss backups; recorded as
convergence evidence rather than a new leaf.

### E. Convergence-only datapoints (already in corpus, reinforce the clause)
- `conditional-write` / `job-claim-and-lease`: the `(status, attempts)` **fencing
  token** that makes a reclaimed job's stale worker-write match no row
  (`storage.rs:314-318,419-424`) — an independent, textbook reinvention.
- `backfill-migration` P5/P7: the typo-scope-is-not-success and read-is-paged
  discoveries, each shipped as a test named after its anti-pattern
  (`search-backfill.rs` tests). Strong convergence for P5 and P7.

---

## 4. Methodics compliance

- **clauses_scored:** 29 physics clauses across 11 leaves (leaf-level: 5 holds,
  4 partial/violate, 2 n/a).
- **executed vs read:** *Executed* — file-extension census (`git ls-files`);
  the CHECK-behaviour probe on a throwaway sqlite3 DB (proved `jobs.status`
  accepts garbage exit 0 vs resilience `state` rejects it exit 19); all content
  counts via the ripgrep-backed Grep tool (CHECK constraints = 5;
  `REFERENCES`/`CASCADE`/`FOREIGN KEY` = 0; prod `SqlitePoolOptions` = 1;
  secret-in-log interpolation = 0; core `let _ =` = 11). *Read* — the specific
  function bodies whose shape is the evidence (`storage.rs`, `search-backfill.rs`,
  `main.rs`, `job.rs`, `deny.toml`, `ci.yml`, `routes/mod.rs`). Verdicts about a
  code *shape* rest on reading that exact code; verdicts about *behaviour*
  (`jobs.status` accepting a value) were executed.
- **two-implementation counts & disagreements:** CHECK constraints — Grep count
  (5 lines) cross-checked by reading each hit; the 5 lines resolve to ~4 distinct
  protected columns (source_kind appears twice due to an additive rebuild), no
  disagreement. Status columns — Grep for `status … TEXT` (jobs, webhook_deliveries)
  cross-checked against the `SourceState` `state` column found separately; agreed.
  Foreign keys — Grep for `REFERENCES` and for `ON DELETE CASCADE` independently,
  both returned 0; agreed and consistent with the "verify FK pragma" clause being
  n/a. Pool config — Grep for `SqlitePoolOptions::new` and `SqliteConnectOptions::new`
  independently; both point to one prod site + one read-only test copy; agreed.
- **rg absent (disclosed, not fabricated):** the `rg` binary is not on PATH here;
  the Grep tool (ripgrep-backed) and `sqlite3` are present and were used. No
  fleet-wide claim rests on an absent instrument.
- **self-corrections during the run:** (1) an initial `grep -rn` over the repo
  root hung on the `target/` build tree and timed out — switched to the scoped,
  ripgrep-backed Grep tool for all subsequent counts. (2) A Grep regex with an
  unescaped `{` was rejected by ripgrep — reran with a corrected alternation. (3)
  Initial instinct was to treat `connection-pool-pragmas` clause (c) as a straight
  violation; on measuring 0 foreign keys in the schema, corrected it to **n/a** for
  the FK half (an unverified pragma cannot break a cascade that does not exist) —
  the leaf's own "report n/a honestly" discipline.
