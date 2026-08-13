---
slug: remote-fabric-deployable
type: perfect/direction
context: "[[remote-engine]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-13
accepted: 2026-08-13
shipped: 2026-08-13
commit: 6def9cc
---

## What & why

Two things make the fabric unsafe to stand up exactly as documented, and neither is written
down anywhere.

**One: a profile-scoped fetch dispatched to a peer silently returns logged-out content.** The
coordinator serializes `profile` onto the wire; the serving route clamps caps and never looks at
it; the peer's HTTP engine opens `<profiles_dir>/<name>/cookies.json`, gets `NotFound`, and
**starts an empty jar with no warning at all** (the one `warn!` on that function covers only an
*unreadable* jar, not a missing one — Director-verified). The peer returns HTTP 200 with public
or login-wall HTML, the coordinator has no way to tell, and it flows through extraction into
stored dataset revisions as real records. Turning `[remote]` on is enough to corrupt every
profiled dataset. **Both the unit test and the e2e assert that the profile travels — the leak is
pinned as the contract.**

**Two: the peer will fetch anything, including its own loopback API.** The inner request is a
fully-deserialized `HttpRequest` with arbitrary method, headers and body, and there is no URL
scheme/host/private-IP check anywhere. Meanwhile every other route on that node is
unauthenticated by design — `docs/deployment.md:133-140` says the whole safety argument is
binding to `127.0.0.1`. But a peer must be reachable at a routable address, so **using `[remote]`
at all requires violating that precondition, and nothing in `config.toml`, `RemoteConfig`, the
route's module doc, or `docs/deployment.md` says so.** `docs/deployment.md` has no `[remote]`
section; `ONBOARDING.md` does not mention the fabric at all.

This is **not** the API-key-auth direction the user parked, and not r8's rejected local-API
egress hardening: `/fetch-proxy` is the one route deliberately exposed to the network, and the
fix is a target policy plus an honest deployment contract, not authentication.

## Evidence

- Profile leak: `crates/core/src/fetcher.rs:726` sets `req.profile`;
  `crates/engine-remote/src/lib.rs:142` serializes it whole; `crates/server/src/routes/remote.rs:86-93`
  clamps only caps; `crates/engine-http/src/lib.rs:89` —
  `Err(e) if e.kind() == NotFound => CookieStore::default()`, **silent**. **Director-verified.**
- Pinned as correct: `crates/server/src/e2e/fetch_proxy.rs:141`
  (`assert_eq!(seen[0].profile.as_deref(), Some("acme"))`) and
  `crates/engine-remote/src/lib.rs:358`. Both pass in CI.
- No target policy: grep over `crates/engine-http/src/lib.rs`,
  `crates/server/src/routes/remote.rs`, `crates/engine-remote/src/lib.rs` for
  `is_loopback|127\.0\.0\.1|169\.254|private_ip|ssrf|allowlist|denylist|blocked_host`
  → **zero matches**.
- Arbitrary method/headers/body on the wire: `crates/core/src/engine.rs:177-243` (fully `Deserialize`).
- The unstated precondition: `config.toml:125` (`http://10.0.0.2:8088`) vs
  `docs/deployment.md:135-140`. `grep -n "remote" docs/deployment.md` → one unrelated hit at `:137`.
- `crates/engine-remote/**` has **no entry in `scripts/docs/feature-doc-map.json`** — the only
  engine crate without one, so the r17 doc-sync hook can never flag drift here. (Director-verified:
  the map has `engine-http/browser/claude/wasm/search` and no remote.)

## Acceptance criteria

1. A profile-scoped fetch can no longer be served logged-out and stored as real data. Choose the
   lever and justify it: keep profiled fetches local, or have the serving side refuse a profile
   it does not have (so the coordinator falls back), or have the peer report that the profile was
   absent so the coordinator can reject the response. **Silently succeeding is the one option that
   is off the table.**
2. The two tests that currently pin the leak are rewritten to pin the new contract, and a test
   named after the anti-pattern proves a missing-profile peer response is not stored as real data.
3. The serving side refuses target URLs pointing at loopback / link-local / private ranges by
   default, with an explicit `[remote]` opt-out for operators who genuinely proxy a LAN. Extract
   the predicate as a named pure function with its own tests (`.claude/CLAUDE.md` doctrine).
   **Check the existing e2e fixtures before you land this** — they proxy through `127.0.0.1`
   *nodes*; make sure the guard applies to the *target* and that the suite still passes for the
   right reason, not because the guard is inert.
4. `docs/deployment.md` gains a `[remote]` section that states the network precondition in plain
   words: enabling the fabric requires binding off loopback, which exposes every other route, and
   names the network-level control an operator must add (firewall / VPN / reverse proxy). No
   hand-waving, no in-app auth.
5. `scripts/docs/feature-doc-map.json` gains a `crates/engine-remote/**` entry pointing at the doc
   that actually covers the fabric, so the hook can see this crate at all. **This file is Class C
   (Director-only) this wave — report the exact row you need; do not edit it.**
6. The module docs that this falsifies are corrected — notably
   `crates/engine-remote/src/lib.rs:21-23` ("degrades throughput, never correctness": the profile
   leak is a correctness failure and must stop being denied in a doc comment).

## Risks / non-goals

- **Non-goal:** authentication on the other routes. The user parked API-key auth explicitly
  (config.md § User taste, 2026-07-13) and it stays parked. This direction is a target policy and
  an honest deployment contract only.
- **Non-goal:** per-node identities / secret rotation. Bank it; it is a bigger design.
- Risk: refusing private targets by default could break a legitimate LAN scraping deployment —
  hence the opt-out key, defaulted safe.
- Risk: making the peer refuse unknown profiles turns a silent-wrong-data case into a fallback,
  which costs a double fetch. That trade is correct; say so in the doc.

## Build record

**Shipped `6def9cc` · verdict KEEP.**

**Lever chosen and argued (criterion 1):** profiled fetches **stay on the coordinator**
(`must_serve_locally`), the doctrine `fetch_bytes` already follows. A profile is a cookie jar on one
node's disk and nothing replicates it, so no configuration exists in which a peer can serve one
correctly; peer-refuses-then-fall-back would pay a wasted round trip on *every* profiled fetch forever
to reach a node that can never legitimately answer. The serving side refuses too (`absent_profile` →
422), explicitly as defence-in-depth against an older or hostile coordinator. Layered on
`require_existing_profile` rather than forking a parallel concept.

**`blocked_target`** is a pure predicate (`blocked_v4`/`blocked_v6`/`blocked_name`) covering loopback,
`0.0.0.0/8`, RFC-1918, link-local incl. `169.254.169.254`, CGNAT, `fc00::/7`, `fe80::/10`,
broadcast/multicast and non-http(s) schemes. It judges the parsed `Ipv4Addr`, so every WHATWG spelling
(`127.1`, `2130706433`, `0x7f.0.0.1`, `[::ffff:127.0.0.1]`) is caught by construction rather than by a
blocklist of strings. `allow_private_targets` relaxes addresses only; the scheme refusal is not
opt-outable.

**Criterion 3's trap was checked as instructed:** the guard reads the **target**, so the e2e's loopback
*node* addresses still work, and `a_node_refuses_to_fetch_its_own_loopback_api_for_a_peer` proves it is
live rather than inert.

Both tests that pinned the leak rewritten. 10 new tests across the two crates, all anti-pattern-named.
`docs/deployment.md` gains a `[remote]` section with a control table (firewall / private overlay /
authenticating reverse proxy) — **no in-app auth invented**, as the non-goal required.

**Refuted:** `crates/engine-remote/**` was **not** the only engine crate missing from
`feature-doc-map.json` — `crates/engine-archive/**` is equally absent, and that one is not hypothetical
(the archive tier shipped a provenance direction with no map coverage). Director-verified; both added
in `96a4ef1`, along with `routes/remote.rs` so the fabric's *serving* half points at the doc that
actually describes its behavior.

**Not closed, stated:** the DNS-name SSRF hole — `http://internal.corp/` resolving into a private range
is not caught, because the predicate is pure. Documented as a known limit, not papered over.
