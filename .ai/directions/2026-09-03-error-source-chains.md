---
subject: software-engineering/error-handling
project: pumper
raised_by: intake intake-kube-0903 (peer comparison)
source: librarian/sources/2026-09-03-kube-rs.md
stage: crates/core/src/error.rs and the ~186 construction sites across crates/ that flatten a cause into a format! string
size: 4 files / ~120 lines / M
status: accepted
---

## Why the scope implies it

`scope.does` names *"pluggable scrape and fetch engines"* and *"research cache, remote fetch fabric, cost ledger"* — six engine crates, a tiered fetcher, a plugin host and a store, every one of which raises a failure that crosses a crate boundary before anyone acts on it. pumper's whole reliability argument is that a failure is **classified**, not read: `is_terminal_for_job()` decides whether the retry ladder runs (`crates\core\src\error.rs:412-421`), `is_router_failure()` decides whether the tier ladder continues (`:453-493`, consumed at `crates\core\src\fetcher.rs:1174`), `is_store_contention()` decides which instrument bucket a store op lands in (`:518-526`). Each of those is a decision made on the *variant*, and each is exemplary.

The variant is not the only thing a caller might need, and it is the only thing that survives. Fourteen of the eighteen variants carry a `String` (`error.rs:155-263`), and **186 sites across `crates/` construct one with `format!`** — `crates\core\src\catalog.rs:330` and `crates\core\src\config.rs:713,716` are the shape: `.map_err(|e| Error::Config(format!("{}: {e}", path.display())))`. At that line the cause is a `toml::de::Error` with a span, a line and a column. One line later it is prose, and every consumer downstream — the job row, the receipt, the trigger ledger, the doctor — has the same prose and no way back.

The tree already knows this is the wrong direction and has said so twice, in its own words, one layer down. `PluginFailure` exists because *"the observatory buckets replays into ok/trap/empty/schema_invalid … and they used to do it by matching substrings of the host's own `format!` messages. Rewording one message silently reclassified every row it produced, with no test anywhere failing"* (`error.rs:96-100`). `SourceDrift` exists because *"the only way to classify was to match substrings of a sentence anybody was free to reword, which is the same anti-pattern `PluginFailure` exists to kill"* (`:244-247`). And `is_store_contention` refuses SQLite's prose in favour of the extended result code, with the reason written out (`:504-511`). Three times the tree has found that a *classification* smuggled into a message was a defect, and fixed it by adding a typed field. The cause itself is the same smuggling, unfixed.

The peer makes the opposite choice and makes it uniformly: `#[source]` on every non-leaf variant of every enum — `watcher::Error`'s five (`C:/t/kube/kube-runtime/src/watcher.rs:23,27,31,35`), `controller::Error`'s three (`kube-runtime/src/controller/mod.rs:74,79,83`), `finalizer::Error`'s four (`kube-runtime/src/finalizer.rs:28,32,36,40`) — so the chain, not the sentence, is the taxonomy. It goes further and makes the *caller's* error a type parameter (`controller/mod.rs:62-83`), warning that this deliberately makes `anyhow` awkward (`finalizer.rs:16-20`). That second half does not transfer; every pumper app is in-tree. The first half transfers exactly.

## What the first context contains

A `source` on the four variants where a typed cause exists and is currently discarded, and the three consumers that can then ask for it. One concept, no new module, and **no change to any variant's `Display` output** — the human sentence is good and stays byte-identical.

**The shape.** `Error::Http`, `Error::Config`, `Error::Parse` and `Error::Browser` gain a second, optional field holding the cause as `Option<Box<dyn std::error::Error + Send + Sync>>`, tagged `#[source]`, with `#[error("http: {0}")]` unchanged so the rendered line does not move. Constructors mirror the two that already exist for `Claude` and `Plugin` (`:274-305`): `Error::http(message)` keeps today's behaviour, `Error::http_from(message, cause)` attaches. The four are chosen because each has a typed cause at every one of its raise sites — `reqwest::Error`, `toml::de::Error`, `serde_json::Error`/`scraper` parse failures, and the browser driver's own error respectively — and because together they cover the majority of the 186 sites.

`Error::App` is deliberately **excluded**. Its causes are app prose by construction, and giving it a source invites apps to attach `anyhow` chains that mean nothing to any consumer here.

**Consumer one — the job row's operator answer.** `worker.rs:1003-1038` persists `e.to_string()` for both the terminal and the retryable branch. The row keeps that sentence and gains a `cause_kind` — the `type_name` of the deepest source, not its message — so the two failures an operator has to tell apart (*the origin refused* vs *our own client could not build a request*) are distinguishable in a query, the way `is_terminal_for_job` already made *terminal* queryable.

**Consumer two — the classifiers, made cheaper rather than changed.** `is_store_contention` currently reaches through `Error::Storage(sqlx::Error::Database(db))` to read a code (`:518-526`). With a source chain the same shape generalises: a `reqwest::Error` reached through `Error::Http`'s source answers `is_timeout()` / `is_connect()` directly, so `crates\engine-http\src\lib.rs:858-870`'s carefully-reasoned per-class comment block becomes a match on the cause rather than a comment about what the message probably means.

**Consumer three — the receipt.** `GET /jobs/{id}/receipt` gains the chain's depth-1 rendering. This is the one place a human is deliberately reading an error rather than a machine classifying it, and it is where a lost span/line/column costs most.

**What it must NOT absorb.** Not `is_terminal_for_job` or `is_router_failure` — both stay exactly as they are, matching on the variant, and neither may start consulting a source, because a classification that depends on a cause the compiler does not check is the drift both predicates were written to end. Not the `Display` strings. Not `anyhow` at any boundary the two predicates read; `Error::Other(#[from] anyhow::Error)` stays the escape hatch it is. Not the 186 sites in one pass — the first context converts the ones inside `crates/core` and `crates/engine-http`, which is where the classifiers live, and leaves `crates/apps/**` for the pattern to spread into.

## The measurable

**Construction sites that discard a typed cause: today 186, target 0 for the four converted variants.**

Measured by a test in `crates/core/tests/` that greps `crates/core` and `crates/engine-http` for `Error::{Http,Config,Parse,Browser}(format!` and asserts zero — the same EXPECTED-diff idiom `expected_terminal` already uses (`error.rs:876-900`), so a new site fails the build rather than being noticed later. The starting count is the number in the title of this section, taken today.

**Second number — the classification that becomes possible.** `crates\engine-http\src\lib.rs:855-870` documents five reqwest failure classes and their retry verdicts in a comment, because the enum cannot hold them. After, a test asserts that a fabricated `reqwest` connect error, timeout error and decode error each classify from the source without any string in the assertion. Today: 0 of 5 classified structurally. Target: 5.

**Third number, for the operator.** Share of failed job rows whose recorded error names a cause type. Today 0. This is the number that turns a repeating scheduled failure from "read the log" into "group by cause".

## What would make this wrong

**If the cause is already in the message and that is enough.** Every one of the 186 sites interpolates `{e}`, so the cause's *Display* is not lost — only its type and its structured fields. If a survey of the retained job-failure history shows that no consumer would branch differently given the type, this is craft for its own sake, and the honest verdict is `declined`. The check is cheap: take the 20 most frequent distinct `jobs.error` prefixes and ask, for each, whether a decision anywhere in the tree would change with the cause type in hand. If fewer than three would, stop.

**If `Send + Sync + 'static` does not hold at the raise sites.** `Error` is `Clone`-adjacent in places and crosses `tokio::spawn` boundaries everywhere; a boxed `dyn Error` is not `Clone`. `Error` does not currently derive `Clone` (only `ClaudeSpend` and the failure-class enums do), so this should hold — but if any consumer clones an `Error`, the box must become an `Arc<dyn …>` and that is a different, slightly worse shape. Verify before writing the first variant.

**If it grows the enum's size on the hot path.** `Error` is returned from every fallible function in the workspace, including the fetch chokepoint. Adding an `Option<Box<…>>` to four variants adds one pointer to the largest variant, which is already a `String` + a struct. If `size_of::<Error>()` moves by more than one word, the boxing should cover the whole payload rather than sitting beside it.

**If it re-opens the classification door it was meant to close.** The risk is not technical, it is cultural: once a chain exists, the next classifier will be tempted to walk it. `is_router_failure`'s doc already states why the exhaustive variant match is the right shape — *"a `_ =>` arm would let 'ours' be reached by accident"* (`:446-452`) — and that argument applies with more force to a chain, whose depth and content no signature constrains. If the first review of this change proposes a predicate that reads a source, that is the signal the boundary was not stated clearly enough, and the fix belongs in `error.rs`'s module docs before the code lands.
