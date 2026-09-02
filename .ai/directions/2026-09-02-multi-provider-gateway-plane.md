---
subject: software-engineering/multi-provider-gateway-plane
project: pumper
raised_by: intake intake-portkey-0902 (peer comparison)
source: librarian/sources/2026-09-02-portkey-gateway.md
stage: the tier escalation ladder in crates/core/src/fetcher.rs and the job retry decision in crates/server/src/worker.rs
size: 3 files / ~80 lines / S
status: proposed
---

## Why the scope implies it

`scope.does` reads *"pluggable scrape and fetch engines"* — plural, behind one interface, with a fallback ladder wiring them together. That is the exact configuration in which one class of failure becomes expensive: a defect or misconfiguration in **pumper itself** reproduces identically on every engine, and a ladder that cannot tell it apart from an engine's own failure converts one defect into N engine invocations per job, and then into `max_attempts` × N across the job's backoff.

pumper has the vocabulary for this and has drawn the axis one notch off. `Error::is_terminal_for_job()` (`crates\core\src\error.rs:412-421`) separates transient from deterministic — `BudgetExhausted | Transact | BadRequest | ReplayMiss | SourceDrift` are terminal, each with a written justification for why attempt #2 re-fails in the identical place. **`Error::Config(_)` is on the retryable side** (`error.rs:819`), as are `Error::Plugin` (`:821`) and `Error::Parse` (`:818`). A malformed `[claude]` section therefore walks every tier of `Fetcher::fetch` (`crates\core\src\fetcher.rs:508-838`) to `ladder_exhausted` (`:832`), and then the job-level backoff at `crates\core\src\storage.rs:466-491` re-runs the whole thing at 10s, 20s, 40s — producing the same sentence each time, and reading in the job log exactly like the source being down.

pumper has already met this failure once, in one capability, and fixed it correctly there. `Browser::transact`'s default refusal is typed `Error::Transact` rather than `Error::Browser`, and the comment at `crates\core\src\engine.rs:1223-1231` is the general argument in miniature: *"which engine sits behind the trait object is fixed for the life of the job, so every attempt reaches this identical refusal before touching a browser. As an `Error::Browser` it was retryable, and a job that reached an engine without flow support burned its whole backoff ladder producing the same sentence four times."* The proposal is that sentence applied to the rest of the enum.

The peer solves the same problem at the same seam and with the same reasoning, which is what makes this structural rather than domain-specific. When a leaf handler throws, portkey synthesises a response carrying `x-portkey-gateway-exception: true`, and the fallback loop breaks on that header as if the request had succeeded — refusing to burn the remaining targets (`C:/t/portkey/src/handlers/handlerUtils.ts:800-826`, comment: *"Add this header so that the fallback loop can be interrupted if its an exception"*; the break condition at `:679-690`). Portkey needed a header because its two failure kinds are separated by a process boundary and a status code cannot carry the distinction — a gateway 500 and an upstream 500 are the same number. pumper needs no header: it is one process with one typed error enum, so the distinction is a variant classification. That the same decision arrives in a TypeScript LLM proxy and a Rust scraper is the evidence that it belongs in the corpus rather than in either tree.

There is a third site in pumper where the axis is currently absent and costs money: `fetcher.rs:752-767` un-skips the HTTP tier when the browser engine fails — *"http tier un-skipped: browser engine failed, retrying the tier the router had skipped"*. That is the right behaviour when the browser engine genuinely failed, and the wrong one when the failure was pumper's own `[browser]` configuration, because the router's original skip decision was correct and is now being overturned by evidence that says nothing about the tier.

## What the first context contains

A **failure-origin axis** on the existing error type, and its three consumers. One concept, three call sites, no new module.

**The axis.** A predicate beside `is_terminal_for_job` in `crates\core\src\error.rs` answering *"would every engine produce this?"* — true for the variants that describe pumper's own state rather than an upstream's: `Config`, `Plugin` where the failure is a missing or unloadable plugin rather than a plugin's own verdict, and the pre-flight refusals `Transact` and `ReplayMiss` that `is_terminal_for_job` already names. It is a **second, independent** axis, not a re-partition of the first: `BudgetExhausted` is terminal *and* ours; `Http` is transient *and* theirs; `SourceDrift` is terminal and *theirs*, and that is exactly the cell that proves the two axes are not the same question. Each variant's classification carries its reason in the doc comment, in the style `error.rs:236-263` already sets.

**Consumer one — the ladder.** `Fetcher::fetch` (`fetcher.rs:508-838`) stops escalating when a tier fails with an ours-error instead of walking to `ladder_exhausted` (`:832`). One tier tried, one failure reported, with the tier named.

**Consumer two — the un-skip.** `fetcher.rs:752-767` un-skips only on a theirs-error. A `[browser]` misconfiguration stops overturning a correct routing decision.

**Consumer three — the operator's row.** The job-level branch at `crates\server\src\worker.rs:1003-1038` already forks on `is_terminal_for_job()`; the origin rides into the recorded error text so a job row says which of the two happened, the way the fetch ladder's `budget_exhausted` (`crates\engine-http\src\lib.rs:942-945`) already distinguishes *"the origin was slow"* from *"we gave up on our own clock"* without anyone reading a log.

**What it must NOT absorb.** Not `is_terminal_for_job` — that axis stays exactly as it is, with its five variants and its reasoning intact; this is orthogonal and collapsing them loses the cells that matter. Not the tier ordering, the `TierVerdict` escalation rules, or the governor. Not portkey's in-band header: pumper is one process and does not need a carrier, and importing one would be the mechanism without the force. Not the resilience/quarantine ladder in `crates\core\src\config.rs`'s `[resilience]` block, which judges *sources* over time — a different subject, and the drift between "this source is unhealthy" and "our config is broken" is precisely what this axis keeps apart.

## The measurable

**Engine invocations per router-caused failure: currently `tiers × max_attempts`, target 1.**

Measured by a new case under `crates\server\src\e2e\` (49 files today), built on the trait-level fake seam in `crates\core\src\testing.rs` — `engines_with(http, browser, claude)`, the constructor `dead_engines()` (`:325`) already calls with three `Dead` engines (`:77-80`, which panic rather than return, so this case needs a counting fake alongside them) — with one engine wired to return `Error::Config("missing key")` and a job enqueued with `max_attempts` above the default 1 (`crates\server\src\routes\jobs.rs:156`). Assert the fake engines' call count and the job row's `attempts`. Today the count is every tier in `Fetcher::fetch` multiplied by the attempt count; after, it is one tier, one attempt, one terminal row.

**Second number: the un-skip's cost.** In the same harness, a browser engine failing with `Error::Config` currently causes `fetcher.rs:752-767` to re-run the HTTP tier the router had skipped. Assert that the HTTP tier is invoked zero times for an ours-error and still invoked for a theirs-error — the paired assertion is what keeps the fix from becoming a regression in the case the un-skip was written for.

**Third, for the operator:** the share of failed job rows whose recorded error names its origin. Today 0 for the retryable variants; the target is 100% of ours-errors, which is what turns a repeating scheduled-run failure into one that can be read rather than investigated.

## What would make this wrong

**If ours-errors are rare in the run history.** The whole argument is amplification, and amplification of a failure that never occurs is worth nothing. `Error::Config` reaching the fetch ladder at runtime may be a boot-time-only problem in practice, since `Config::validate()` (`crates\core\src\config.rs:742`) catches 29 misconfigurations before the server starts. The falsifying evidence is the job-failure history: if no failed job in the retained record carries a `config:` or `plugin:` error text, this is machinery for a case `validate()` already closed, and the honest verdict is `deferred` with "an ours-error is observed reaching the ladder" as the return condition. **This should be checked before the proposal is accepted**, and it is a single query against the jobs table.

**If the classification is ambiguous at the edges.** `Error::Plugin { plugin, kind, message }` (`error.rs:154-270`) covers both "the plugin could not be loaded" (ours) and "the plugin returned a verdict" (theirs, and legitimately retryable if the input changes). If the variant cannot be split without a new field, the axis is being drawn across a boundary the type system does not have, and forcing it produces a predicate that lies in one direction — worse than no predicate. The `PluginFailure` kind enum is where that is settled, and if it cannot be, `Plugin` stays unclassified and the axis covers `Config` alone.

**If it slows the ladder's real job.** `Fetcher::fetch` exists to get a document by any available means, and a predicate that stops the ladder early on a misclassified error turns a recoverable fetch into a failed one. The failure direction matters: a theirs-error wrongly classified as ours costs a document; an ours-error wrongly classified as theirs costs only what happens today. The predicate must therefore default to *theirs* for anything not explicitly named, and if the first implementation defaults the other way it should be rejected on that basis alone.
