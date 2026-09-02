# pumper vs. `Portkey-AI/gateway` — structural peer comparison

- **Source**: `Portkey-AI/gateway`, clone `C:/t/portkey`, pinned `669825cbe89ee51569918b8f78a9db486fd69dd4`
- **Design record**: `librarian/sources/2026-09-02-portkey-gateway.md` (intake run `intake-portkey-0902`)
- **Why this peer, and why the study is short**: portkey is 78 provider adapters behind one typed interface with a price book, a fallback/load-balance tree, and a cost seam. pumper is pluggable scrape/fetch engines behind one interface with a cost ledger. The domains share nothing; the *architecture* is the same shape. That makes pumper the better test of whether any of portkey's decisions deserve to be stated as general rules — a technique that transfers into LLM-adjacent tracklight might just be LLM-shaped, while one that transfers here is structural. This study runs 13 points and only on that axis; the request-plane detail is in tracklight's copy.
- **Verdicts** come from the closed set `adopt` / `adapt` / `keep ours` / `different forces`.

**Verdict tally: 13 points — 2 `adopt`, 3 `adapt`, 6 `keep ours`, 2 `different forces`.**

**Headline: the design record's §10 row 12 is wrong and this study corrects it.** It reads *"pumper has a durable job queue where retries default on by design"* and files the comparison as `different forces`. `crates\server\src\routes\jobs.rs:156` says `max_attempts: body.max_attempts.unwrap_or(1).clamp(1, MAX_ATTEMPTS_CAP)` — one attempt, no retry, unless a caller asks. Same posture as portkey's `attempts ?? 0`. The row is a **catch**, and the convergence is the finding: two systems of opposite shape independently defaulted a retry ladder to off. See §4.

---

## 1. Engines behind one interface

**1.1 — Capability traits, not one Engine trait.**
Portkey: one adapter shape for 78 providers — `ParameterConfig` / `ProviderConfig` (`C:/t/portkey/src/providers/types.ts:19-42`), `ProviderAPIConfig` (`:45-81`), and a `RequestHandler` escape hatch for providers that do not fit (`:127-136`).
pumper: three narrow traits by capability, not one wide one — `HttpClient::fetch` (`C:\Users\kazda\kiro\pumper\crates\core\src\engine.rs:1179-1180`), `Browser::render` (`:1213-1214`), `Researcher::research` (`:1239-1240`), bundled in `EngineSet` (`:1253-1259`). Optional capabilities are default methods that refuse by name: `fetch_bytes` (`:1191-1196`) and `transact` (`:1232-1234`).
**Verdict: `keep ours`.** Portkey's escape hatch exists because one wide interface could not hold Bedrock's SigV4 and Vertex's service-account auth. pumper's default-refusing method is the same idea in a form that does not need an escape hatch, and the compiler enforces it. Three traits at N=3 engines is right-sized; the declarative param map is what N=78 buys and pumper is not near it.

**1.2 — The chokepoint the interface does not expose.**
Portkey's `providerOptions` is public everywhere; every caller can name any provider.
pumper makes `claude` a private field of `EngineSet` (`crates\core\src\engine.rs:1256`), with the reasoning written out at `:1243-1252`: every model call must go through `AppContext::research`, which adds the research cache, the budget governor and cost metering, and *"it happened — `connector-api-watch` summarized every doc diff off-ledger"*. Field privacy makes the chokepoint structural rather than conventional.
**Verdict: `keep ours` — inverse list.** This is the stronger design and it is one portkey does not have: `preRequestValidatorService.ts:22` is a seam the OSS tree never registers, so nothing enforces that a call passes it.

**1.3 — Selection is a cost-ordered escalation ladder, not a strategy tree.**
Portkey: each config node carries a strategy (`single | loadbalance | fallback | conditional`) *and* the execution policy for everything below it, merged child-wins at every hop across 26 keys (`C:/t/portkey/src/middlewares/requestValidator/schema/config.ts:12-74`; `src/handlers/handlerUtils.ts:476-560`, with `retry` and `cache` replaced wholesale rather than deep-merged at `:503-513`).
pumper: `Fetcher::fetch` (`crates\core\src\fetcher.rs:508`) walks fixed tiers in cost order — API recipe (`:520`) → archive (`:533`) → HTTP (`:626`) → browser (`:636`) → Claude research (`:773`, only under `FetchStrategy::AutoWithResearch`) — escalating on a `TierVerdict` of `Thin` / `Blocked` / `Error`, ending in `ladder_exhausted` (`:832`).
**Verdict: `different forces`.** Portkey's targets are interchangeable — two vendors serving the same call, so weights and fallback order are an operator's business decision. pumper's tiers are *not* interchangeable: they are strictly ordered by cost and capability, and "load-balance 80/20 across the browser and the HTTP tier" is not a sentence anyone wants. A flat, ordered ladder is the right shape and a tree would be expressive machinery for an unaskable question.

---

## 2. Router-vs-routed failure attribution

**2.1 — The distinction pumper has is transient-vs-terminal, not mine-vs-theirs.**
Portkey: a leaf failure synthesises a response carrying `x-portkey-gateway-exception: true`, and the fallback loop breaks on that header as if the request had succeeded — refusing to burn the remaining targets (`C:/t/portkey/src/handlers/handlerUtils.ts:800-826`, with the comment *"Add this header so that the fallback loop can be interrupted if its an exception"*; the break condition at `:679-690`). The force: a bug or a config error in the router reproduces identically on every candidate, so a loop that cannot tell the two apart turns one defect into N upstream calls per request.
pumper: `Error::is_terminal_for_job()` (`crates\core\src\error.rs:412-421`) returns true for exactly `BudgetExhausted | Transact | BadRequest | ReplayMiss | SourceDrift`, each with a written justification for why a retry re-fails identically. **`Error::Config(_)` is classified retryable** (`error.rs:819`). A malformed `[claude]` section therefore walks the whole tier ladder in `Fetcher::fetch` (`fetcher.rs:508-838`) and then rides the job-level backoff at `crates\core\src\storage.rs:466-491` — 10s, 20s, 40s — producing the same sentence every time.
**Verdict: `adopt`.** pumper has the vocabulary and the discipline; the axis is missing. This is the cheapest transfer in the run and it is the pumper proposal.

**2.2 — The one place pumper already made the distinction, and wrote down why.**
`Browser::transact`'s default refusal is typed `Error::Transact`, not `Error::Browser`, and the doc comment at `crates\core\src\engine.rs:1223-1231` is the reasoning verbatim: *"which engine sits behind the trait object is fixed for the life of the job, so every attempt reaches this identical refusal before touching a browser. As an `Error::Browser` it was retryable, and a job that reached an engine without flow support burned its whole backoff ladder producing the same sentence four times."*
**Verdict: `keep ours` — and it is the argument for 2.1.** pumper found this failure once, in one capability, and fixed it there. The general form is that a fact about *pumper's own configuration* is not a fact about the engine, and 2.1 is that sentence applied to the rest of the enum.

**2.3 — The escalation ladder's un-skip.**
`fetcher.rs:752-767` re-tries the HTTP tier when the browser engine fails, even if the router had skipped it — *"http tier un-skipped: browser engine failed, retrying the tier the router had skipped"*.
**Verdict: `adapt`.** This is exactly the amplification portkey's header prevents, in miniature: a browser failure that is *pumper's* (a missing Chrome profile, a bad `[browser]` config) currently un-skips and re-spends a tier the router had already ruled out. The un-skip is right for an engine failure and wrong for a router failure, and the two are indistinguishable at `:752` today. Same fix as 2.1, same line of code.

---

## 3. Per-engine framing

**3.1 — Framing is per-provider there and shared here.**
Portkey: the frame delimiter is a `(provider, endpoint)` lookup — `\n\n` default, `\r\n\r\n` for Anthropic `/complete` and Vertex's Google publishers, `\n` for Cohere's non-chat endpoints and DeepInfra, `\r\n` for Google (`C:/t/portkey/src/utils.ts:14-45`) — and Bedrock is not SSE at all, read by a hand-rolled binary reader over length-prefixed frames with a 4-byte prelude, a headers block and a CRC (`src/handlers/streamHandler.ts:38-130`). Chunk transforms own a mutable `streamState` threaded across the stream (`:69`).
pumper: one buffered-response model across all three engines. `HttpClient::fetch` returns `HttpResponse { body: String }` (`crates\core\src\engine.rs:359-368`); `Browser::render` returns a materialized `RenderedPage { html: String }` (`:1090`); `engine-claude` parses one JSON envelope after the CLI exits (`crates\engine-claude\src\lib.rs:353`, `:388`, `:397`). SSE exists only for *job status* (`crates\server\src\routes\events.rs:8,33,63,104,162`), which is one producer to one surface.
**Verdict: `different forces` — for now, with a named trigger.** A scrape produces a document, and a half-document is not a smaller result. The framing table earns nothing while every engine's output is a settled artifact. The trigger that changes it is stated in `crates\core\src\engine.rs:1185-1190`: `fetch_bytes` is documented as the *"engine-traits#2-LITE seam"* with *"the full streaming binary-body design stays deferred"* and the body buffered in memory. The day a ZIP or a PDF is streamed to disk, framing becomes per-engine and portkey's `streamHandler.ts:38-130` is the ~90-line dependency-free reference implementation to read first.

**3.2 — The delimiter as a parameter is the transferable half.**
The portable claim is not the table; it is that a stream reader takes its delimiter as a parameter rather than assuming the spec, because mis-framing does not error — it stalls or emits truncated JSON.
**Verdict: `adapt`, when 3.1's trigger fires.** Recorded here so the decision is inherited rather than rediscovered.

---

## 4. Retries: default on or off, for a durable queue vs. a synchronous gateway

**4.1 — Both default to off, and the design record says otherwise.**
Portkey: `attempts ?? 0`, with `onStatusCodes` empty unless attempts > 0 (`C:/t/portkey/src/handlers/services/requestContext.ts:148-155`) — the taxonomy is inert until an operator opts in, because a component that fans in every caller is the fleet's amplifier by construction.
pumper: `max_attempts: body.max_attempts.unwrap_or(1).clamp(1, MAX_ATTEMPTS_CAP)` (`crates\server\src\routes\jobs.rs:156`), and the same default at `crates\server\src\routes\schedules.rs:226` and `crates\server\src\routes\triggers.rs:266`.
**Verdict: `keep ours` — and it is a catch, not a contrast.** Durability and retry are separate properties: the queue guarantees the job is not *lost*, not that it is *re-run*. A scrape job that failed on a source that changed its schema re-fails identically, which is why `SourceDrift` is terminal (`crates\core\src\error.rs:249-263`) and why the default is 1. Two systems of opposite shape, same default, for the same reason.

**4.2 — The stated wait against a total-time budget — pumper already implements portkey's hardest retry decision.**
Portkey: a stated `Retry-After` zeroes the remaining ladder, but a stated delay `>=` the 60-second whole-request budget, or `>` what remains of it, **ends the ladder** rather than truncating the wait, spends zero further attempts and reports its own terminal state (`C:/t/portkey/src/globals.ts:5`; `src/handlers/retryHandler.ts:104-146`; `src/handlers/handlerUtils.ts:1283-1288`).
pumper: `capped_retry_sleep` (`crates\engine-http\src\lib.rs:925-930`) returns `None` — stop now — when the sleep plus a minimum attempt will not fit what remains of the deadline, and the comment states the rule the corpus is missing: *"Truncating the sleep instead would be worse than failing — it would retry earlier than the server asked, which is the one thing politeness must never do."* The caller turns that into a distinct `budget_exhausted` error (`:566-597`), and `attempt_timeout` (`:935-940`) clamps each attempt to the remaining budget too.
**Verdict: `keep ours` — the strongest inverse-list entry in this study.** pumper reached portkey's answer independently, spelled the reason better, and applied it in two places instead of one. This is a **second sighting** of the technique the intake run wants to land in `backoff-design`, and it should be cited as a field application rather than proposed as a direction.

**4.3 — Ordered header spellings.**
Portkey reads three, with two unit systems: `['retry-after-ms', 'x-ms-retry-after-ms', 'retry-after']` (`C:/t/portkey/src/globals.ts:7`), the last `*1000` at `src/handlers/retryHandler.ts:118`.
pumper reads one — `response.headers().get("retry-after")` (`crates\engine-http\src\lib.rs:998-1001`) — but reads it *properly*: both RFC 7231 forms, delta-seconds and HTTP-date, with the date converted from now, clamped to 600s, and a past or malformed date yielding `None` rather than zero (`:994-1016`).
**Verdict: `adapt`, small.** The two millisecond spellings are an Azure/vendor concern that a general web scraper meets rarely; the *rule* — a stated-delay reader needs an ordered accept-list, not one header name — is worth one line in `retry_after()` if a target host is ever seen sending one. Note that pumper's use is stronger in direction: `retry_delay` takes `max(backoff, retry_after)` (`:987`), so the stated wait is a floor and never shortens the ladder, whereas portkey lets it *zero* the ladder.

**4.4 — Jitter.**
Portkey passes `randomize: false` (`src/handlers/retryHandler.ts:169`) — no jitter at all, on its fleet's correlator.
pumper jitters both ladders deterministically from a seed, with no `rand` dependency: the fetch ladder via `lcg_fraction` (`crates\engine-http\src\lib.rs:984-991`) and the job ladder with up to +25% at `crates\core\src\storage.rs:491`.
**Verdict: `keep ours` — inverse list.** The source is wrong; pumper is right, twice, and deterministically so the tests can assert it.

---

## 5. Config validation

**5.1 — The checker is the contract, and pumper's is the deeper one.**
Portkey: a ~170-line zod schema expressing a recursive strategy tree with four cross-field `.refine()` invariants carrying human-readable messages (`C:/t/portkey/src/middlewares/requestValidator/schema/config.ts`) — the whole contract in one screen. The design record calls it the best small example of "the checker is the contract" a recent run produced.
pumper: `Config` is 27 nested sections over ~188 public fields (`crates\core\src\config.rs:10-37`), and `validate()` (`:742`, running to ~`:1088`) holds **29** cross-field refusals, each with a comment naming the failure mode it prevents and each with a message an operator can act on — `stale_after_secs` vs `heartbeat_secs` (*"otherwise every healthy job is reaped as hung"*, `:749-756`), `job_timeout_secs` vs `stale_after_secs` (*"otherwise the reaper races the job timeout"*, `:761-770`), `concurrency == 0` (*"a worker with 0 slots claims no jobs"*, `:774-778`), and the `http.max_body_bytes == 0` case (`:859-861`) which is refused rather than reinterpreted precisely because one tier down `[browser] max_html_bytes = 0` means the opposite. `Config::load()` calls it at `:715`, reached from `crates\server\src\main.rs:216`.
**Verdict: `keep ours` — decisively, inverse list.** Same doctrine, four times the coverage, and the comments carry the forces. If a technique is landed on "the checker is the contract", pumper's `validate()` is the better instance to cite.

**5.2 — The one asymmetry: a missing file skips validation entirely.**
`Config::load()` (`crates\core\src\config.rs:706-722`) warns and returns `Config::default()` when the file is absent (`:719-720`) — defaults are presumed coherent and never run through the 29 checks. Portkey's equivalent is the opposite failure: `src/middlewares/adminAuth/index.ts:8-19` **throws at startup** when a required secret is missing, refusing to boot rather than defaulting.
**Verdict: `adapt`, one line.** Running `validate()` on the default path costs nothing and would catch the day a default drifts out of the band its own rule enforces — which is the exact class of defect `validate()` exists to catch, exempted only for the configuration nobody wrote.

---

## 6. The cost ledger

**6.1 — What is recorded, and what is priced.**
Portkey: `pricing_config` and a `status`-bearing model roster travel with the *credential* (`C:/t/portkey/conf.example.json:38-44`), and the resolved pricing rides on the log object (`src/handlers/services/logsService.ts:37-40`).
pumper: `CostLedger::record` writes `CostEvent { job_id, app, engine, url, cost_usd, detail, created_at }` (`crates\core\src\costs.rs:20`, `:102`), aggregated by job (`:140`) and grouped by `(app, engine)` (`:151`). There is **no price table**: `cost_usd` is the Claude CLI's own reported `total_cost_usd`, read verbatim (`crates\engine-claude\src\lib.rs:353`, `:388`, `:397`), and HTTP and browser calls are recorded at `$0` (`costs.rs:3-4`).
**Verdict: `keep ours`.** Portkey needs a price book because it holds the credential and the caller does not; pumper's spender reports its own spend, and a locally-derived estimate over a vendor's authoritative figure would be a second number that can only disagree with the first. `cost_source` as a concept does not need inventing when there is only one source.

**6.2 — The axis the ledger is keyed on.**
pumper keys by `engine` — the tier name (`"claude"`, `"http"`, `"browser"`, `"archive"`) — not by provider or model. Portkey keys everything by `(provider, model)` and, at the integration, by credential.
**Verdict: `adapt`, narrowly and only if it becomes true.** Today `engine` and "who charged us" are the same fact, because exactly one tier costs money. The moment a second paid tier appears — a paid search API, a commercial proxy, a hosted browser — `(app, engine)` stops separating them and the summary at `costs.rs:151` reports one bucket for two vendors. The trigger is a second non-zero `cost_usd` producer, not a schema aspiration.

---

## 7. HTTP cache

**7.1 — Cache identity: raw request here, transformed request there.**
Portkey: the key is SHA-256 over the *provider-transformed* body plus the endpoint (`C:/t/portkey/src/middlewares/cache/index.ts:14-26`; `src/handlers/services/cacheService.ts:88-95`), so two different gateway-level requests that transform identically share a hit — cache identity sits at the layer where the request is canonical.
pumper: `HttpCache::key()` is SHA-256 over method + url + body + sorted headers + proxy (`crates\core\src\cache.rs:68-91`) — the raw request, headers sorted for determinism and not stripped.
**Verdict: `different forces`.** pumper's cached artifact is the origin's own bytes; there is no transform between the caller and the wire, so "canonical" and "raw" are the same layer. Portkey's transform exists because 78 providers spell one request 78 ways. The one place pumper *does* have a transform layer — `ResearchCache`, keyed over prompt, system prompt, role, model, effort, max turns and JSON schema (`cache.rs:479-502`) — already keys on the semantic inputs rather than the rendered CLI invocation, which is portkey's rule applied where it applies.

**7.2 — Cacheability is an enumerated property, not a per-call guess.**
Portkey: `putInCache` returns early on `stream`, and 16 endpoint kinds are excluded by an explicit non-cacheable list (`src/middlewares/cache/index.ts:69-72`; `src/handlers/services/cacheService.ts:22-40`).
pumper: `HttpEngine::cacheable()` (`crates\engine-http\src\lib.rs:538-543`) enumerates four exclusions in one predicate — non-GET, a body present, `no_cache`, and any profiled request — with the reason for the fourth written at `:534-537`: *"the shared `http_cache` is keyed by method+url+body only, so caching a logged-in body would serve it to anonymous callers."* `ResearchCache` adds a fifth by construction: `resume_session` bypasses it entirely (`crates\core\src\cache.rs:459`).
**Verdict: `keep ours`.** Same doctrine — one predicate, all the exclusions, each with its reason — and pumper's fourth exclusion is a security property portkey's list does not contain an equivalent of.

---

## Tests to initiate

Two, both paired, both naming the instrument and the number.

1. **The router-vs-routed fixture.** New case under `crates\server\src\e2e\` (49 files today), using the trait-level fake seam in `crates\core\src\testing.rs` — `engines_with(...)`, which `dead_engines()` (`:325`) calls with three `Dead` engines (`:77-80`; those panic rather than return, so this case needs a counting fake beside them) — with an engine wired to return `Error::Config("missing key")`. Pair: *today, the job walks every tier in `Fetcher::fetch` and then burns `max_attempts` at `crates\core\src\storage.rs:466`, producing the identical message each time* / *after §2.1, the ladder stops at the first tier and the job is terminal on attempt 1*. Instrument: the fake engine's call count and the job row's `attempts`. Number that moves: **engine invocations per router-caused failure**, from `tiers × max_attempts` to 1. Run it against `Error::Config` first and `Error::Plugin` second — the plugin path is the one where a pumper-side misconfiguration is most likely (`docs/features/trigger-plugins.md` documents the fail-open unknown-plugin path).

2. **Default-config validation.** New case in `crates\core\src\config.rs`'s test module (which already exercises `validate()` at `:1791-1823`). Pair: *today, `Config::load()` with no file at the path returns `Config::default()` without calling `validate()` (`:718-721`)* / *after §5.2, `Config::default().validate()` is asserted green as a standing invariant*. Instrument: the test itself. Number that moves: **config paths that reach the worker unvalidated**, from 1 to 0. This is the cheapest item in either study and it guards the 29 checks against their own defaults drifting.

---

## Features, ranked — with why the scope admits each

`scope.does` reads: *"durable SQLite job queue behind an HTTP API; pluggable scrape and fetch engines"* and *"research cache, remote fetch fabric, cost ledger, tier memory"*.

1. **A router-vs-routed axis on `Error`** (§2.1, §2.2, §2.3) — *"pluggable scrape and fetch engines"* names the plural, and a plural of engines behind one fallback ladder is precisely the configuration where the router's own failures masquerade as every engine's. The scope clause that admits the ladder admits the attribution. → proposal `2026-09-02-multi-provider-gateway-plane.md`.
2. **`validate()` on the default path** (§5.2) — *"durable SQLite job queue"*; the 29 checks exist to keep the worker from silently claiming nothing, and the default config is the one path that skips them. One line, and it is the first test above.
3. **A second axis on the cost ledger** (§6.2) — *"cost ledger"*, named in the scope. Deferred behind its trigger: a second paid tier. Recorded so it is inherited, not rediscovered.
4. **An ordered accept-list for `Retry-After` spellings** (§4.3) — *"remote fetch fabric"*. One line in `crates\engine-http\src\lib.rs:998`, worth doing when a target host is observed sending a millisecond spelling and not before.
5. **Per-engine framing** (§3.1, §3.2) — *"pluggable scrape and fetch engines"*, but gated: the trigger is the streaming binary-body design that `crates\core\src\engine.rs:1185-1190` explicitly defers. Until then this is machinery for a stream that does not exist.

**Only one proposal is filed**, against `software-engineering/multi-provider-gateway-plane`. The other three subjects this run wants to place — `retry-backoff`, `credential-vault`, `browser-credential-boundary` — already govern contexts in `.ai/registry-map.json` (`http-engine`, `us-federal-grants`, and `engine-contracts` / `browser-engine` / `http-engine` / `remote-engine` respectively). Those are coverage questions for `/conform`, not direction gaps, and proposing them here would be asking the owner to adopt something they already have.

---

## The inverse list — what pumper does better

1. **`capped_retry_sleep`** (`crates\engine-http\src\lib.rs:917-930`) — the stated-wait-vs-budget collision, resolved, with the reason written out and applied in two places (`:566-597`, `:935-940`). Portkey solves it once; pumper solves it better and jitters on top. This is the fleet's reference implementation and tracklight's proposal cites it rather than the source.
2. **Deterministic jitter on both ladders** (`crates\engine-http\src\lib.rs:984-991`; `crates\core\src\storage.rs:491`) vs. portkey's `randomize: false` on its fleet's correlator.
3. **The error enum's per-variant terminality reasoning** (`crates\core\src\error.rs:154-270`) — each terminal variant carries a paragraph explaining why a retry re-fails identically, and `SourceDrift` (`:236-263`) exists because *"the source changed its schema"* and *"the source was down"* are different events with different remedies. Portkey's taxonomy is an operator-supplied list of integers.
4. **`validate()` with 29 cross-field refusals**, each naming the invisible failure it prevents (`crates\core\src\config.rs:742-1088`) — against portkey's four `.refine()`s.
5. **The metered chokepoint enforced by field privacy** (`crates\core\src\engine.rs:1243-1256`), with the incident that motivated it named in the comment. Portkey's equivalent seam is never registered in the OSS tree, so nothing enforces it.
6. **`Error::Transact` as a pre-flight refusal distinct from an in-flight failure** (`crates\core\src\engine.rs:1223-1231`) — the router-vs-routed distinction, already made correctly in the one place it was found to matter.
7. **The profiled-request cache exclusion** (`crates\engine-http\src\lib.rs:534-543`) — a logged-in body must never be served to an anonymous caller. Portkey's 16-kind exclusion list has no equivalent security clause.
8. **Nine blocking CI rungs including `flake-check` and `harness-test`** (`.ai/manifest.yaml` `controls.ciHardPass`), with the harness suites existing specifically to prove the gates can still go red — *"a gate never observed going red is indistinguishable from a gate that cannot."* Portkey's tree carries no equivalent.
9. **Long lanes as certification rather than gate** (`controls.scheduledCertification`), with the distinction argued rather than assumed: a long lane judges behaviour over time, which is not a property of any single change.
