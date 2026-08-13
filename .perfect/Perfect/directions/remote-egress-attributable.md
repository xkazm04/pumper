---
slug: remote-egress-attributable
type: perfect/direction
context: "[[remote-engine]]"
lens: feature
status: shipped
size: M
proposed: 2026-08-13
accepted: 2026-08-13
shipped: 2026-08-13
commit: d128ae7
---

## What & why

The fabric's entire product claim is "this fetch left from a different IP/geography". **Nothing
in the product can confirm it happened.** No field on `HttpResponse`, `TierTrace`, `FetchOutcome`,
`Provenance`, the SSE job events, or `/receipt` names the serving node; `engine` is the literal
string `"http"` whether the fetch went out locally or through a peer in another country. The
total observability of the fabric is **one `warn!` line, coordinator-side, on the failure path
only** — success is completely silent, and the serving node logs nothing at all.

The operational consequence: a misconfigured secret makes every peer answer 401 → warn → silent
local fallback, forever, and the only symptom is a log line that reads identically whether one
fetch or a million fell back. An operator cannot answer *how much* egress actually went remote,
*which* node is failing, or whether the fabric is doing anything.

The trust consequence is sharper: nothing binds the returned envelope to the requested URL. The
coordinator deserializes whatever the peer sent and the tiered fetcher mints the outcome with the
**requested** URL and the peer's body. A buggy or hostile peer can return arbitrary content for
any URL and it is stored, indexed and attributed with no detectable trace.

`docs/features/fetching.md:245` already names this as a known gap — this direction closes it, on
the same seam r17's `archive-provenance-visible` used for the archive tier.

## Evidence

- `HttpResponse` = `{status, headers, body, final_url, cache_hit}` (`crates/core/src/engine.rs:264-274`);
  `TierTrace` (`crates/core/src/fetcher.rs:201-227`) and `FetchOutcome` (`:229-257`) have no node field.
- `crates/core/src/fetcher.rs:761` — `engine` is `"http"` for local and peer-served alike.
- `crates/server/src/routes/receipt.rs:70-82` — groups only `by_engine`.
- Observability grep over `crates/engine-remote/src/lib.rs` + `crates/server/src/routes/remote.rs`
  for `warn!|info!|debug!|error!|counter|histogram|span` → **exactly one hit**, `lib.rs:185`.
  `/metrics` (`crates/server/src/routes/meta.rs:53`) has zero remote series.
- `crates/engine-remote/src/lib.rs:169-171` — deserialize and return; nothing compares anything
  to `req.url`. `crates/core/src/fetcher.rs:1106` then mints `url: req.url.clone()`.
- `ProxyResponse.final_url` and the peer's headers are decoded, converted, and dropped —
  `try_http_tier` (`crates/core/src/fetcher.rs:716-812`) reads only `status`, `body`, `cache_hit`.
- The documented gap: `docs/features/fetching.md:245`.
- Precedent for the shape: r17's `FETCHED_VIA_HEADER` / archive snapshot provenance, and this
  crate's own `REMOTE_SECRET_HEADER` constant (`crates/engine-remote/src/lib.rs:42`).

## Acceptance criteria

1. A peer-served fetch is **attributable end to end**: the node that served it is visible on the
   fetch trace and reaches at least one operator-facing surface that already exists (the job
   receipt and/or the fetch trace — do not invent a new endpoint). A local-egress fetch is
   distinguishable from a peer-served one without reading logs.
2. **Do not add a field to `crates/core/src/engine.rs`** — that file belongs to the sibling lot
   this wave. Carry the attribution on the response headers under a reserved key (the
   `FETCHED_VIA_HEADER` precedent) and read it at the fetcher seam, or propose an alternative in
   your report and wait for a Director answer.
3. The envelope is **bound to the request**: the peer echoes the URL it was asked for, and the
   coordinator refuses a mismatched envelope (falling back to local like any other node failure)
   rather than storing it. Test it with a peer that answers for the wrong URL.
4. The serving side logs what it did on someone else's behalf — at minimum the target URL and
   the outcome — so a node whose IP gets banned can reconstruct what it fetched for peers.
5. Success is countable, not just failure: remote-served vs fallen-back is a number an operator
   can read. Prefer extending the existing `/metrics` producer over a new surface; if that is
   more than a small change, report it and ship the trace/receipt half.
6. `docs/features/fetching.md:245` is updated — the known-gap sentence either goes away or names
   precisely what still remains.

## Risks / non-goals

- **Non-goal:** cross-node governor/cache sharing. Out of v1 by the module doc; stays out.
- **Non-goal:** failover and cooldown — that is [[remote-failover-not-leakback]], same file.
- Risk: a reserved response header must not collide with a real target-site header. Namespace it
  and strip it from anything that echoes headers onward.
- Risk: logging target URLs on the serving side is a privacy surface; log at the level the repo
  already uses for fetches, not above it.

## Build record

**Shipped `d128ae7` · verdict KEEP.**

Attribution rides a reserved response header (`x-pumper-remote-node`), following `FETCHED_VIA_HEADER`
rather than forking a parallel system — the header map is the only channel that survives an engine
boundary. **Constraint honored: `crates/core/src/engine.rs` untouched** (it was Lot B's); constants
live in `core::fetcher` next to their only reader. Read **only** where the fabric is wired, so an
origin echoing the header on an ordinary live fetch cannot forge "a peer served this" — the archive
tier's own forgery rule, with a test.

Reaches three surfaces that already existed: the tier trace's `detail` (**on losing entries too** —
"the http tier came back blocked" reads very differently once you know whose IP it came back blocked
at), the job receipt's new `cost.egress = [{node, calls}]`, and the serving node's logs (target,
status, bytes, duration of every fetch made on someone else's behalf).

**Envelope binding:** the peer echoes the URL it was **asked** for — deliberately not `final_url`,
which legitimately differs after a redirect — and the coordinator refuses a mismatch, falling back
like any other node failure. An envelope with **no** echo fails closed too, or the binding would be
opt-in for the very peer being checked. The echo is stripped before the response flows onward, and any
node marker arriving from the wire is overwritten.

**Criterion 5 half-shipped, and correctly so:** `Fetcher::egress_counters()` was built, tested and made
public, but `/metrics` was **not** wired, because the read is a `state.engines.fetch` field access that
the raw-engine inventory flags by design — and the builder declined both workarounds (rephrasing the
expression to dodge the scanner; making the counters a process-global). Both refusals were right.
**The Director wired it in `19f1707`** with the reviewed inventory row plus
`a_silent_fallback_to_local_egress_is_not_invisible_to_a_dashboard`, and `just smoke` now live-checks
the series (36/36).

**Also corrected because this change depends on it:** the receipt's `unknown` text claimed "free tiers
do not write ledger rows". They do — `AppContext::fetch` meters every fetch as a $0.00 row, which is
exactly why per-job `cost.egress` is possible at all.

**Refuted:** criterion 1's "visible on the fetch trace" as a **typed field** is not reachable from this
write set — `TierTrace`/`FetchOutcome` are built by struct literal in four files the lot did not own.
`TierTrace.detail` used instead (the archive tier's precedent), and the typed-field path named in the
doc rather than silently dropped. `fetching.md:245` now states precisely what remains: no dataset
revision records the node, and the fact travels as a trail marker rather than a typed field.

**Not verified:** the serving-side log line is not asserted (no capture harness); `fetch_cost_detail` →
`cost_events.detail` → receipt is proven in unit form, not by driving a real job through a peer.
