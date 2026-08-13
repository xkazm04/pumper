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

- **round 18 — 3/3 shipped, merged to master, gate 1746/0, smoke 36/36. First shipped work on this
  context; the crate went from one moonshot commit to swept.**
  - [[remote-fabric-deployable]] → `6def9cc` — profiled fetches stay on the coordinator
    (`must_serve_locally`), the peer refuses a profile it has no jar for, and `blocked_target` refuses
    loopback/private/link-local/CGNAT targets and non-http(s) schemes by default. `deployment.md`
    finally states the network precondition the fabric requires.
  - [[remote-failover-not-leakback]] → `53b45ae` — failover across remaining peers with a per-node
    cooldown; the peer's own-failure status moved 502 → 422 so the coordinator's transport stops
    retrying a deterministic failure four times, guarded by a test against the shipped
    `retryable_statuses`.
  - [[remote-egress-attributable]] → `d128ae7` — `x-pumper-remote-node` carries the serving node to
    the tier trace and the job receipt's `cost.egress`; the peer echoes the URL it was asked for and
    a mismatched (or unecho'd) envelope is refused.
- **Director follow-ups this round:** `96a4ef1` — wired `pumper_remote_egress_fetches` onto `/metrics`
  with the reviewed `EXPECTED_RAW_ENGINE_CALLS` row the builder correctly refused to game, and added
  `crates/engine-remote/**` + `crates/engine-archive/**` + `routes/remote.rs` to the doc-sync map;
  `e1e18db` — smoke live-checks the egress series.
- **Next anchor (banked, in priority order):** `remote-scope-honesty` (18 raw `ctx.engines.http` call
  sites never go through the fabric — cross-referenced on `app-runtime`) · the **DNS-name SSRF hole**,
  open and documented: `blocked_target` is pure, so a hostname that *resolves* private is not caught;
  closing it needs resolve-then-pin inside the HTTP engine and still races rebinding · per-node
  identities + secret rotation (one cluster-wide secret, `http://` node URLs permitted by `validate`).
