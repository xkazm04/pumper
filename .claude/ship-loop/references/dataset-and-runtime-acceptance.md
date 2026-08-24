# Dataset value proof + runtime acceptance harness

(The pumper replacement for the SaaS original's "billing value proof + Playwright UAT". Same method: prove the thing the product exists to deliver, with automated assertions, driven through the real surface. Here the currency is **records**, not dollars, and the real surface is the **HTTP API of a running server**, not a browser.)

## Part A — Dataset value proof

The claim to prove: **every scheduled or requested job that costs time, bandwidth, or model spend returns records a consumer can actually use, and the service never reports success without producing value.**

### A1. Per-app value inventory
Build (at boot, from the app + dataset audit) a table in `state.md`:

| App | Engine tier | Dataset(s) | Key fields | Promised value ("a caller asks for X, receives Y") | Cost shape |
|---|---|---|---|---|---|

- **Promised value** must be honest and consumer-visible, and should match the app's `description()` and its row in `catalog/data-sources.toml`. If you can't write it convincingly for an app, that app is a product bug — flag it as a checkpoint decision (rewrite / re-tier / cut).
- **Cost shape** is `free` (http, cached), `slow` (browser), or `paid` (claude tier / tier-3 escalation). Paid apps get the strictest scrutiny in dimension 9.

### A2. Per-app assertions (automated, part of the runtime-acceptance suite)
For **every** row of the value inventory:

1. **Produces value:** the job reaches `succeeded` AND writes ≥1 dataset record (or returns a non-empty `result` for apps that don't persist). A `succeeded` job with an empty result is the pumper equivalent of charging without delivering — it is a 🔴, not a 🟡.
2. **Idempotence / change detection:** run the app twice against unchanged upstream data → the second run reports **zero** `changed` records. A second run that reports everything changed means the key or the comparison is wrong, and every downstream consumer's "what's new" is noise.
3. **Key correctness:** two genuinely different upstream records never collide on the key; the same upstream record across runs always maps to the same key. Prove with a targeted query against `/datasets/{app}/{dataset}` or the store directly.
4. **Failure ≠ success:** force the upstream to fail (unreachable host, 403, a timeout below the job's budget) → the job lands `failed` with a useful `error`, retries respect `max_attempts`, and **no partial/garbage records are persisted**. This is the assertion everyone skips; do not skip it.
5. **Paid apps only:** a run with `max_budget_usd` set below the expected cost is refused or clamped, not silently overspent; the reported `cost_usd` in `/jobs/{id}/costs` is non-zero and plausible; a repeat identical request is served from the research cache at zero cost.

### A3. Lifecycle journey (one end-to-end spec)
1. Fresh `data/` → boot (`just run`, or `just dev` for verbose logs) → migrations apply → `GET /health` ok, `GET /apps` lists the registry.
2. `POST /apps/{name}/jobs` → `202` + job id → `GET /jobs/{id}` polls to `succeeded` → `GET /datasets/{app}/{ds}` shows the records → `GET /datasets/{app}/{ds}/export` returns them.
3. Enqueue and **cancel** a queued job (`DELETE /jobs/{id}`) → it never runs.
4. Kill the process with a job in flight → restart → crash recovery re-queues it (do not fake this with a unit test; run it).
5. Create a schedule via the API, confirm it is listed and its next fire time is right, then disable it.
6. Register a webhook, run a job, confirm the delivery is signed and appears in `/webhooks/deliveries`, and that `replay` works.
7. `GET /events` (SSE) shows the transitions of a job run in another shell; `GET /metrics` counters move.

### A4. Config assertions
- Every key the code reads exists (or has a documented default) — assert by inspection recorded as evidence.
- The catalog's `cron` for an app matches the schedule actually registered; the catalog's `status = "live"` matches the registry. A mismatch is a trust bug at 🔴 (a consumer reading the catalog believes data is arriving that isn't).

## Part B — Runtime acceptance harness

### B1. Principles
- Journeys are **consumer stories driven through the real HTTP API** of a **release-profile server** (`cargo build --release -p pumper-server --bin pumper`, then run it — `--bin pumper` is required, since the package also ships `reindex` and `search-backfill` and has no `default-run`), against a `data/` directory reset to a known state per run.
- Prefer arriving at state through the API like a consumer would; touch the SQLite file directly only for expensive preconditions (e.g. "a dataset with a month of history"), and mark those journeys as seeded.
- Keep the journeys as a runnable script (bash + `curl`, or a small script under `scripts/`) checked into the repo — **not** as prose in the state dir. A journey that only exists in a markdown file rots. If the repo grows a proper integration-test harness, move them into the `#[ignore]`d set (`just test-ignored`) and record that in the profile.
- Name them `uat-<journey>.sh` and keep the port configurable (default `127.0.0.1:8088`).

### B2. Consumer personas (default set — trim per the ship bar)
- **New consumer:** a fresh agent on this machine reads `ONBOARDING.md` §4, hits `/apps`, enqueues one job, polls, gets a result. Everything they need must be discoverable from the API + that doc alone.
- **Scheduled operation:** an app fires on cron unattended overnight, writes records, reports changes; nobody is watching. Failures must be visible after the fact (`/jobs?status=failed`, `/metrics`, webhook).
- **Bulk consumer:** requests many jobs at once — per-app fairness holds (one busy app doesn't starve others), the global concurrency cap holds, priorities are respected.
- **Hostile upstream:** the target site 403s, rate-limits, redirects in a loop, returns a 50 MB body, or serves JS-only content. Expect graceful degradation (governor spacing, bounded reads, tier escalation), never a hang and never garbage persisted.
- **Recovering operator:** kills the process mid-run, restarts, retries a failed job (`POST /jobs/{id}/retry`), resets a stuck one (`POST /jobs/{id}/reset`). Nothing is lost or double-run.

### B3. Journey derivation
Cross the app × route inventory with the personas: every registered app appears in at least one journey; every route in the `EXPECTED` inventory is exercised by at least one journey or explicitly listed as untested with a reason. At boot, list the derived journeys in `backlog.md` (one item each) so the user prioritizes them like any other work. The runtime-acceptance depth chosen at boot decides whether the hostile-upstream journeys are ship-blocking or nice-to-have.

### B4. Determinism rules
- Reset `data/` (or point `config.toml` at a scratch dir) before each journey file; no ordering dependence between journeys.
- **Live upstreams are the point for correctness, and the enemy of determinism.** Split them: a *smoke* set that hits real sources (proves selectors still match — this is what catches the failure mode ONBOARDING §8 warns about) and a *deterministic* set that serves fixtures from a local file/HTTP stub (proves the plumbing). Record which is which in the journey header.
- Never let a journey spend model money implicitly. Claude-tier journeys must pass an explicit `max_budget_usd`, and are excluded from the fast gate.
- A flaky journey is a 🔴 finding on its own (either the journey or the service is racy) — fix, don't retry-mask. If the flake is genuinely upstream (a rate-limited public API), move it to the smoke set and say so.

### B5. Data sweep (feeds dimension 5)
After journeys pass: for each dataset touched, pull `/datasets/{app}/{ds}/changes`, `/history`, and `/duplicates` and eyeball them against the value inventory — near-duplicate clusters, a change count equal to the record count, or an empty history where records exist are all findings. File them to the backlog. Re-sweep after any change to a key, a parser, or the extraction rules.
