---
slug: http-fetch-has-a-deadline
type: perfect/direction
context: "[[http-engine]]"
lens: robustness
status: accepted
size: M
proposed: 2026-08-13
accepted: 2026-08-13
shipped: —
commit: —
---

## What & why

One `HttpEngine::fetch()` has no bound on its own wall clock. `timeout_secs` is bound
per attempt (`build_client` at `lib.rs:252`, overridden per request at `:352-355`) and the
whole thing sits **inside** `for attempt in 0..=retries` (`:397`). Against a host that
answers `429 Retry-After: 600`, `retry_delay` takes `floor = max(backoff, retry_after)`
and adds up to 25% jitter (`:602-610`), with `MAX_SECS = 600` (`:624`) — so three sleeps
of up to 750 s each. **One fetch can run ~37.5 minutes.** `[worker] job_timeout_secs`
defaults to 900 s (`config.rs:1014`), so a single hostile URL eats an entire job's budget
and kills every other unit of work that job had queued — and the operator sees `TimedOut`
with nothing naming the URL that did it. Even the benign case (black-holed host) is
~127 s, against a `config.toml` comment that promises "a hung host should free its worker
slot in ~30s, not sit for 300s" (`config.toml:36-40`).

The repo has already solved this exactly one layer up and wrote down why:
`[remote] timeout_secs` is documented "**end to end** … enforced as a deadline around the
whole node attempt, because the HTTP engine underneath applies a request timeout per
*retry attempt* (`[http] retries`), which used to multiply this budget by four"
(`config.rs:297-302`). Round 18 had to route *around* this engine. This direction fixes it
at the source, so every caller inherits the bound instead of re-deriving it.

## Evidence

- `crates/engine-http/src/lib.rs:397-403` — per-attempt timeout inside the retry loop; sleep before `governor.acquire`
- `crates/engine-http/src/lib.rs:602-610` — `retry_delay` floor = `max(backoff, retry_after)` + 25% jitter
- `crates/engine-http/src/lib.rs:624` — `MAX_SECS = 600` per sleep
- `crates/core/src/config.rs:297-302` — the `[remote]` deadline shape, and the prose naming this engine as the cause
- `crates/core/src/config.rs:1014` — `job_timeout_secs` default 900 s
- `config.toml:36-40` — the documented promise the code contradicts
- Grep `Instant|deadline` in `crates/engine-http/src/` → **zero hits**
- `crates/core/src/fetcher.rs:840` — the tiered fetcher wraps no timeout around the http tier; it only *measures*
- Rider: `crates/engine-http/src/lib.rs:584-586` + `crates/core/src/config.rs:1142-1149` — `[http] max_body_bytes = 0` rejects **every** non-empty body ("exceeds max_body_bytes cap of 0 bytes"); `Config::validate` guards the `[remote]` (`:803-809`) and `[ingress]` (`:867-873`) twins but not this one, while `[browser] max_html_bytes` documents `0 = disables the cap` (`:1219`) — same number, opposite meaning, one tier down

## Acceptance criteria

1. A single `fetch()` / `fetch_bytes()` is bounded end to end by a stated budget. Compute a
   deadline once at the top of `send`, cap **each** retry sleep at the remaining budget, and
   stop starting a new attempt that cannot finish inside it.
2. **Which knob carries the budget is yours to decide, and the hazard is real either way.**
   Option A: redefine `[http] timeout_secs` as end-to-end (matches `config.toml`'s existing
   promise and `[remote]`'s wording — but silently shortens every per-attempt timeout for
   existing deployments). Option B: add a separate `[http] total_budget_secs` (no behavior
   change without opt-in — but leaves `config.toml:36-40` a lie unless you also fix that
   comment). Pick one, say why in a doc comment, and make `config.toml` and
   `docs/features/fetching.md` agree with whichever you picked.
3. The exhausted-budget error names the URL and the elapsed time, and is classified so a
   caller can tell "the server was slow" from "we gave up on our own clock".
4. A test executes the retry loop **more than once** — this crate's most cost-bearing
   algorithm currently has none (`retry_delay` is tested exhaustively as a pure function;
   nothing exercises the loop). Use `tokio::time::pause()` against a local axum server that
   counts hits, and assert total wall clock against a `Retry-After` host.
5. Rider: `Config::validate` rejects `[http] max_body_bytes = 0` (or adopts the browser
   tier's `0 = unbounded` semantics — your call, but the two tiers must not keep opposite
   meanings for the same number silently). One test either way.
6. `docs/features/fetching.md` and `config.toml` state the real bound.

## Risks / non-goals

- Do **not** change governor/politeness semantics. The sleep-then-acquire ordering at
  `:400-406` is deliberate and the two ceilings (`retry` 600 s vs `penalty_cap` 300 s) are
  out of scope — note the divergence in the doc if you like, don't unify it.
- Non-goal: cancellation plumbing from the worker. Job-level cancel already works by
  dropping the future (`worker.rs:690-706`); this is about the engine bounding itself.
- Shortening a live deployment's effective timeout is the real risk. If you pick Option A,
  the doc comment must say what changes for someone upgrading.

## Build record

(filled during build)
