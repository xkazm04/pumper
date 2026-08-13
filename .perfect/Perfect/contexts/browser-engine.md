---
name: browser-engine
type: perfect/context
group: Scraping Engines
category: lib
opportunity: 4
last_proposed: 2026-08-13
cooldown_until: round 20
directions: ["[[render-cancel-safe]]", "[[render-has-a-budget]]", "[[browser-refusals-terminal]]"]
alias_of_old_map: "[[fetch-engines]] (round-3 pass covered this file)"
---

## Current state

Scouted "very thorough" 2026-08-13 (round 18), end to end: `crates/engine-browser/` in full,
the `Browser` trait in `crates/core/src/engine.rs`, every call site, the config keys, and the
coupled docs. The crate is three files and one public type: `BrowserEngine` (`lib.rs:180`),
1315 lines.

**The banked r17 anchor was CONFIRMED and sharpened twice.** `render` is indeed the only
required method of the trait (`core/src/engine.rs:1106-1107`; `transact` has a default body at
`:1125-1127`) and `just ci` = `cargo test --workspace` with no `--ignored`. But the crate now
has **23 tests, not 4**: 19 un-ignored unit tests were added in-crate (`lib.rs:933-1315`) and
they pass — every one of them over a **pure helper or constructor**. Not one line inside the
body of `async fn render` (`lib.rs:390-674`) executes in CI. The second sharpening: one
un-ignored CI test *does* construct a real `BrowserEngine` and call a trait method —
`crates/server/src/e2e/engine_conformance.rs:302` — but it reaches exactly one line (`lib.rs:686`,
`req.validate()?`) before returning. So `transact`'s pre-flight door has coverage; `render` has
literally zero statements covered.

**Precedent for the offline harness exists and is copyable**: `crates/engine-http/tests/profiles.rs:36-48`
spawns an in-process `axum::Router` on `127.0.0.1:0` and its test is NOT ignored — it runs in CI
today. Cost is one dev-dependency. What that pattern removes is the *network* dependency, not the
*Chrome* dependency: `render` cannot be entered without `ChromeBrowser::launch` succeeding
(`lib.rs:260`). Open question for a future round: does the CI runner image carry a Chrome the
crate can auto-detect.

**Findings, severity-ordered** (all `file:line`-backed in the scout brief; the two severest were
Director-verified independently):
1. **Cancellation leaks a Chrome tab + two detached tokio tasks, permanently** (HIGH) — the
   worker `break`s out of its select and drops the pinned future (`worker.rs:673-675`, `:692-706`);
   `page.close()` (`lib.rs:641`) and the two `.abort()` pairs live only on success-shaped paths.
   Up to ~200 zombie tabs before the recycle relaunch. → [[render-cancel-safe]]
2. **`extra_wait_ms` / `wait_ms` are uncapped `u64` millis and there is no total render budget**
   (MED-HIGH) — four such jobs wedge the whole browser tier; only 1 of 6 awaits is timed. →
   [[render-has-a-budget]]
3. **Deterministic refusals classified retryable** (MED) — `Error::Profile` is not in
   `is_terminal_for_job` (`error.rs:314-316`), and `docs/features/apps.md:59` claims otherwise.
   → [[browser-refusals-terminal]]
4. No timeouts on `goto` / `content()` / `evaluate` / `find_element` (folded into #2).
5. Network-capture `select!` ends on the *request* stream's end (LOW — dead path, see below).
6. `holders.order` can retain a key `holders.live` dropped (LOW, self-healing FIFO).
7. Every LRU eviction/recycle emits chromiumoxide's "Browser was not closed manually" WARN —
   the log line that would matter is pre-desensitized (LOW).
8. One `Mutex::expect("capture sink poisoned")` reachable from the caller's thread (LOW, contained
   by `catch_unwind`).

**Dead surfaces (grep-proven).** The whole API-X-ray path (~170 lines) has no production producer:
`capture_network` is never set true anywhere, `.xray(` has zero call sites, and the repo already
admits it at `crates/server/src/routes/recipes.rs:10`. `load_all_resources` is set exactly once, in
an `#[ignore]`d test. **`FetchRequest.actions` has zero producers workspace-wide**, so
`execute_actions` is reachable in production only through `transact`. `RenderRequest.evaluate` has
one producer and it is internal. Net: on the production fetch path `RenderRequest` is effectively
`{url, wait_for_selector, profile}` — six of its nine fields are unreachable except through
`transact`.

**Docs-vs-code:** `apps.md:59` false for one of four refusals (→ #3); `README.md:244` +
`ONBOARDING.md:559` list running-job cancellation as "Still open" when it shipped;
`fetching.md`'s `[browser]` config list omits `max_html_bytes`; its `RenderRequest`/`RenderedPage`
field lists are stale; `config.toml:141-150` omits four keys `fetching.md` documents; and
`lib.rs:1046-1047` claims a crash-recovery test lives in `tests/render.rs` — **it does not exist**.

## Direction history

- 2026-08-13 (round 18) — **first proposal pass on the 46-map. 3 accepted / 3 rejected.**
  Slate was all-robustness by judgment, not lapse: the two non-robustness candidates both failed
  the taste filter (below).
  - ACCEPTED [[render-cancel-safe]] · robustness · M — the leak class, Director-verified twice.
  - ACCEPTED [[render-has-a-budget]] · robustness · M — a caller-supplied number can wedge the
    tier for every app on the box.
  - ACCEPTED [[browser-refusals-terminal]] · robustness · S — one contract line from what r17's
    conformance battery already killed, and the doc claims it is fixed.
  - **REJECTED-deferred (banked as this context's next anchor): `render-harness-offline`** — copy
    `engine-http/tests/profiles.rs`'s in-process axum fixture into `tests/render.rs` so the four
    ignored tests become hermetic and deterministic, and gate them on Chrome detection instead of
    a blanket `#[ignore]`. Real, and the precedent is exact. Deferred because the payoff is
    developer-facing and the tests **still cannot run in CI without Chrome on the runner** — which
    is an unanswered question, not an assumption. The two accepted directions deliver the operator
    payoff from the same file this round. Resolve the runner-Chrome question first next time.
  - **REJECTED: `xray-path-honest`** — delete or document the ~170-line dead `capture_network`
    path. No user moment: the repo already documents it as awaiting a discovery caller
    (`routes/recipes.rs:10`), so the "honesty" half is done and the rest is deletion of a moonshot
    seam. Cosmetic churn by the taste filter (config.md § User taste).
  - **REJECTED: transact evidence screenshot** (fill the always-`null` `screenshot_path`,
    `lib.rs:775`) — genuine user value for debugging a failed flow, but unverifiable in CI (needs
    Chrome), it belongs to `browser-transact` rather than here, and `apps.md:71` already declares
    it an honest gap rather than a stub. Bank it for a round that also solves the Chrome-in-CI
    question.

## Shipped

- (inherited — see [[fetch-engines]] and [[browser-transact]])
- **round 18 — 3/3 shipped, merged to master, gate 1746/0, smoke 36/36:**
  - [[render-cancel-safe]] → `efca07c` — a cancelled or timed-out render no longer leaks its tab or
    its two CDP tasks. The builder found the leak surface was **four** exits, not the two the scout
    named. Residual, documented: the drop-path close is best-effort (a shutting-down runtime may
    never poll the detached task); the task aborts are unconditional.
  - [[render-has-a-budget]] → `ee4f4f4` — `[browser] render_budget_secs` (180) bounds a whole render;
    six untimed CDP awaits wrapped; caller waits clamped, not rejected, with a capture reserve.
  - [[browser-refusals-terminal]] → `f145ad2` — a typo'd profile name fails once instead of burning
    four attempts, on **both** engine seams and at the door. Director `eefdd3b` closed the third seam
    (`engine-http`) that the new conformance battery had pinned as a visible gap.
- **Director follow-ups this round:** `eefdd3b` (the `engine-http` seam — class closed, EXPECTED row
  flipped to `true`), `e1e18db` (smoke check: an unsafe profile name is a 422 at the door, live).
- **Next anchor (banked, in priority order):** `render-harness-offline` (resolve does-CI-have-Chrome
  FIRST) · `RenderedPage.budget_truncated` so the settle-wait truncation is in-band rather than a
  `warn!` (one `..Default::default()` at `crates/core/tests/eval_tier3_extraction.rs:363`) · the dead
  `capture_network` X-ray path, still ~170 lines with zero producers.
