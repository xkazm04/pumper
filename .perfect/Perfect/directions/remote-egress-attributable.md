---
slug: remote-egress-attributable
type: perfect/direction
context: "[[remote-engine]]"
lens: feature
status: accepted
size: M
proposed: 2026-08-13
accepted: 2026-08-13
shipped: —
commit: —
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

(filled during build)
