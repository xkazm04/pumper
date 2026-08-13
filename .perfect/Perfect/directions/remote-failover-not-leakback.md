---
slug: remote-failover-not-leakback
type: perfect/direction
context: "[[remote-engine]]"
lens: robustness
status: accepted
size: M
proposed: 2026-08-13
accepted: 2026-08-13
shipped: —
commit: —
---

## What & why

The remote fetch fabric exists so traffic leaves from somewhere other than the coordinator's
IP. With one dead peer out of three, **a deterministic third of all egress silently leaves
from exactly the IP the operator deployed the fabric to stop using** — and each of those
fetches first pays four full proxy attempts with exponential backoff before falling back.
`fetch` picks one node, tries it once, and on any error goes straight to local. There is no
health, no cooldown, no attempt at the remaining peers.

The cost half is worse than it looks. `[remote] timeout_secs` is documented "per proxy call,
**end to end**" and is in fact applied **per attempt** inside the HTTP engine's
`for attempt in 0..=retries` loop with `retries: 3` — and `502`, precisely what a peer returns
when its own fetch fails, is in `retryable_statuses`. So a deterministic peer-side failure
costs ~244 s of proxy attempts before the ~124 s local ladder even starts: ~6 minutes for one
HTTP-tier attempt, against a 900 s job timeout.

On a host that blocks the coordinator, that leaked third comes back thin/blocked, feeds the
learned tier router three strikes, and **permanently pins the whole host to the browser tier** —
so a dead peer silently escalates every future fetch of that host to a costlier engine.

## Evidence

- `crates/engine-remote/src/lib.rs:178-188` — `pick_node()` once; any `Err` → `warn!` → `self.local`.
- `crates/engine-remote/src/lib.rs:131-137` — bare `fetch_add % len`, no health/cooldown/eviction.
- `crates/engine-http/src/lib.rs:341-344` — `req.timeout_secs` → `builder.timeout(...)`, applied
  **per attempt**, inside the loop at `:386`. `retries: 3` (`crates/core/src/config.rs:1145`),
  `retryable_statuses: [429,502,503,504]` (`:1148`). **Director-verified.**
- `crates/server/src/routes/remote.rs:97-102` — the peer's own fetch failure is a **502**.
- The doc lines this falsifies: `crates/engine-remote/src/lib.rs:95` and
  `crates/core/src/config.rs:297` ("end to end"); `crates/engine-remote/src/lib.rs:19`
  ("**Nodes are tried** by simple round-robin" — plural implies failover; exactly one is tried);
  `:21-23` ("degrades throughput, **never correctness**" — a leaked third changes what the
  target returns, and permanently re-pins the tier).
- No test covers a failing node in a multi-node set: the round-robin test
  (`crates/engine-remote/src/lib.rs:362-385`) uses a transport that always succeeds.

## Acceptance criteria

1. A fetch tries the **remaining** healthy peers before falling back to local. Local is the
   last resort, not the second.
2. A node that fails is put on a cooldown and skipped while it lasts, so the next N fetches do
   not re-discover the same dead peer. Cooldown length is config-driven or justified as a constant.
3. The proxy hop does not multiply attempts: a peer-side failure costs **one** attempt per node,
   not four. Fix the amplification at its cause (the proxy call's retry/timeout setup) rather
   than by shortening the timeout, and make `timeout_secs` mean what the doc says — end to end
   for the proxy call — or change the doc to match the code. State which you did and why.
4. Tests, using the existing scripted-transport harness, that pin: node A dead ⇒ node B served
   it; all nodes dead ⇒ local served it; a dead node is skipped on the next fetch while its
   cooldown holds; a peer-side 502 is not retried four times.
5. The three false module-doc claims above are corrected in the same commit.
6. Secondary (fold in if it costs little, otherwise report and bank): the coordinator does not
   enforce `max_body_bytes` on the decoded inner body — only on the transport, at `2×cap+slack`
   (`lib.rs:157-161`, nothing checks `parsed.body.len()` after `:169`), so per-node config drift
   silently doubles the operator's stated cap and pays for the same page twice.

## Risks / non-goals

- **Non-goal:** cluster-wide governor state / shared host-weather. Explicitly out of v1 scope by
  the module doc, and it stays out.
- **Non-goal:** node attribution in the output — that is [[remote-egress-attributable]], same
  file, sibling direction. Keep the commits separate.
- Risk: trying every peer before local turns a total-cluster outage into N× latency. Bound the
  total attempt budget, don't just loop.

## Build record

(filled during build)
