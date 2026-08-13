---
name: remote-engine
type: perfect/context
group: Scraping Engines
category: lib
opportunity: 4
last_proposed: 2026-08-13
cooldown_until: round 20
directions: ["[[remote-failover-not-leakback]]", "[[remote-egress-attributable]]", "[[remote-fabric-deployable]]"]
---

## Current state

Scouted "very thorough" 2026-08-13 (round 18): `crates/engine-remote/` (510 lines, one file),
the serving route `crates/server/src/routes/remote.rs`, the wiring in `state.rs` /
`fetcher.rs`, `RemoteConfig`, and every coupled doc. The crate is essentially **one moonshot
commit** (`52fd982` M17) plus r17's conformance fix — the least-swept engine in the repo.

**Both banked anchors re-verified. Confirmed, sharpened, and one correction.**

- r17's anchor — every peer failure swallowed into a `warn!` + silent local fallback, and
  nothing names the serving node — **CONFIRMED exactly**, and it is broader than stated: no node
  field on `HttpResponse`, `TierTrace`, `FetchOutcome`, `Provenance`, the SSE job events, or
  `/receipt`, and `engine` is the literal `"http"` either way (`fetcher.rs:761`).
- **Correction:** the anchor's implied "nothing proves a peer-served fetch works" is **wrong**.
  `crates/server/src/e2e/fetch_proxy.rs:277` binds a real listener on `127.0.0.1:0`, points a real
  `RemoteEngine` at it, and asserts the node's stack served the page — **it runs in CI and passes.**
  The happy path is proven end to end. What is unproven is every *failure* path and the trust gap.
- r11's anchor #1 (profile leak) — **CONFIRMED and sharpened.** The peer opens a missing
  `cookies.json` and takes the `Err(e) if NotFound => CookieStore::default()` branch
  (`engine-http/src/lib.rs:89`) with **no warning at all** — the one `warn!` on that function covers
  only an *unreadable* jar. r11 recorded it as a warn; it is silent. Director-verified.
- r11's anchor #2 (one node then local) — **CONFIRMED**, and the cost is bigger than r11 knew:
  `[remote] timeout_secs` is documented "end to end" but applied **per attempt** inside the HTTP
  engine's `for attempt in 0..=retries` loop (`retries: 3`), and `502` — exactly what the peer
  returns when its own fetch fails — is in `retryable_statuses`. ~244 s of proxy attempts before a
  ~124 s local ladder. Director-verified.
- r11's anchor #3 (`max_body_bytes` sizes only the transport) — **CONFIRMED**; folded into
  [[remote-failover-not-leakback]] as a secondary criterion.

**Findings, severity-ordered:** (1) profile leak = silent wrong data, *pinned as correct by two
passing tests*; (2) the secret-holder can drive the peer's whole unauthenticated API, no URL/host
policy anywhere; (3) `[remote]` cannot be used without violating `docs/deployment.md`'s stated
safety precondition and nothing says so; (4) per-attempt timeout + retryable 502 = ~6 min for one
HTTP-tier attempt; (5) no failover, no cooldown — a dead peer leaks a deterministic 1/N of egress
out the coordinator's own IP *and* re-pins the host to the browser tier via the learned router;
(6) zero observability (one `warn!`, zero metrics, zero serving-side logs, no doctor check);
(7) nothing binds the envelope to the requested URL; (8) the coordinator's governor and cache go
blind for remote-served fetches; (9) remote covers only the fetcher's HTTP tier — 18 raw
`ctx.engines.http` call sites still egress locally; (10) inner body cap unenforced; (11) no audit
trail of work done for peers.

**Dead surfaces (grep-proven):** `ProxyResponse.final_url` and every peer response header are
decoded, converted, and dropped — `try_http_tier` reads only `status`/`body`/`cache_hit`.
`FETCH_PROXY_PATH` and `ProxyResponse` are `pub` with no consumer outside the crate; the
TypeScript SDK does not know the route exists. **No dead config keys** — all five `RemoteConfig`
fields have readers.

**Docs-vs-code:** "end to end" timeout (false); the `2×` in the transport cap is undocumented;
"**Nodes are tried** by round-robin" (exactly one is); "exactly as polite as a local one" (false
for the coordinator's own governor); "**degrades throughput, never correctness**" (false — the
profile leak is a correctness failure); no `[remote]` section in `docs/deployment.md`; the fabric
is invisible in `ONBOARDING.md`; and **`crates/engine-remote/**` has no `feature-doc-map.json`
entry at all** — the only engine crate without one, so r17's doc-sync hook can never see this crate.

**Well-engineered, do not re-flag** (scout's non-findings, all argued): `fetch_bytes` always local
and pinned by a conformance test; the SHA-256 secret compare; `Relaxed` round-robin; no peer→peer
recursion (the route uses the *plain* `HttpEngine`, `state.rs:309`); no archive-provenance forgery
path; the tier router still learns from remote fetches (only the governor half is lost).

## Direction history

- 2026-08-12 (round 11): scouted (medium); candidates banked, not slated (pool cap). NOT covered.
  All three anchors re-verified in r18 above — #1 and #2 sharpened, #3 folded in.
- 2026-08-13 (round 18) — **first proposal pass. 3 accepted / 3 rejected.**
  - ACCEPTED [[remote-failover-not-leakback]] · robustness · M — a dead peer must not leak a
    deterministic 1/N of egress out the IP the fabric exists to avoid, at 4× the attempt cost.
  - ACCEPTED [[remote-egress-attributable]] · feature · M — closes the gap
    `docs/features/fetching.md:245` already documents, on r17's `FETCHED_VIA_HEADER` seam, plus
    binds the envelope to the requested URL.
  - ACCEPTED [[remote-fabric-deployable]] · robustness · M — the profile leak (silent wrong data,
    pinned by two passing tests) + a target policy + the missing deployment contract.
  - **REJECTED as a standalone: `remote-doc-truth`** — the five false module-doc claims. Not a
    direction: each accepted direction corrects the doc lines it falsifies, in the same commit.
    A doc-only pass would land the sentences without the behavior.
  - **REJECTED-deferred (banked): `remote-scope-honesty`** — remote egress covers only the tiered
    fetcher's HTTP tier; 18 raw `ctx.engines.http` call sites across apps, the crawler frontier and
    the browser/Claude tiers still egress from the coordinator. Real and undocumented, but the
    honest doc line rides along in the accepted directions' doc edits, and the actual fix (routing
    app fetches through the fabric) is `app-runtime`/fetch-chokepoint work, sized L, not this
    context's. Bank as the next anchor here **and** as a cross-reference on `app-runtime`.
  - **REJECTED-deferred (banked): per-node identities + secret rotation** — one cluster-wide
    `String` secret, no per-node identity, and `Config::validate` permits `http://` node URLs so it
    travels in cleartext by design. A real design question, but it is a bigger piece than one
    builder session and it edges the parked auth decision; [[remote-fabric-deployable]] takes the
    blast-radius half that does not.

## Shipped

- (none on this map)
- round 18: pending (see the three accepted directions above)
