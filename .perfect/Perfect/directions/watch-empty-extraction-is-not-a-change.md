---
slug: watch-empty-extraction-is-not-a-change
type: perfect/direction
context: "[[page-monitor]]"
lens: robustness
status: accepted
size: S
proposed: 2026-08-14
accepted: 2026-08-14
shipped: —
commit: —
---

## What & why

**The alerting app fires false alerts.** `watch` is the fleet's Visualping — its entire product is
"tell me when this page changed". It takes whatever the fetch ladder returns and fingerprints it
with **no emptiness check**:

```rust
let markdown = outcome.markdown.clone().or_else(|| outcome.text.clone()).unwrap_or_default();
```

Its two siblings both guard this exact case, and both say why in a comment. `readable`
(`:135-142`) returns `Err`: *"A successful fetch that yields no readable content is a failed
extraction, not an empty-but-valid result — don't report it as OK."* `connector-api-watch`
(`:260-265`) does the same. The escalation that would normally rescue a thin body is best-effort —
the ladder returns the last tier's output however thin (`crates/core/src/fetcher.rs:60-64`) — so an
interstitial, a transient JS-render failure or an empty 200 reaches this line routinely.

The chain that follows is the damage: the record becomes `{chars: 0, content_sha256: e3b0c442…,
excerpt: "", title: null}`, the upsert returns **`Changed`**, and every webhook subscribed to that
watch fires. The human opens the diff and reads *the entire page has vanished*. The next healthy
run flips it straight back — a second alert, equally false. **Two false alarms per incident, on the
one app whose whole value is that its alarms mean something.** A monitor that cries wolf is worse
than no monitor: the user's rational response is to stop trusting it.

## Evidence

- `crates/apps/watch/src/lib.rs:125-129` — the unguarded `unwrap_or_default()`.
- `crates/apps/readable/src/lib.rs:135-142` — the sibling's `extracted_nothing` guard **and its
  comment**, which is the argument for this direction in the repo's own words.
- `crates/apps/connector-api-watch/src/lib.rs:260-265` — the second sibling guarding the same case.
- `crates/core/src/fetcher.rs:60-64` — escalation on `min_content_chars` is best-effort; the ladder
  returns the last tier's body however thin.
- `crates/apps/watch/src/lib.rs:147-158` — the upsert whose `Changed` return drives the alert.
- `:228-230` — the suite **asserts `hex_sha256(b"")`**: it pins the hash of the exact body the app
  should have refused.
- `grep -n "fn extracted_nothing" crates/` — establish where it lives today before writing a new
  one. **Layer on it; do not fork a second predicate.**

## Acceptance criteria

1. `watch` refuses an empty/whitespace-only extraction rather than fingerprinting it, matching
   `readable`'s behavior and message shape. No `pages` record is written and no revision is
   appended on that path — the alert must not fire.
2. The predicate is **shared, not re-implemented**. Find `readable`'s `extracted_nothing`, lift it
   to the obvious shared home if it is app-local, and have both call sites use the one function.
   Three copies of this predicate in three apps is the outcome to avoid; if you conclude lifting is
   wrong, say why in your report rather than forking silently.
3. A test proves the empty body produces **no** `Changed` revision — the acceptance is the absent
   alert, not the returned `Err`. Drive it through `run()` with a `TestContext`, not by calling the
   predicate.
4. `:228-230` keeps passing unchanged — `hex_sha256("")` is still correct arithmetic, it is just no
   longer reachable from a stored record. Do not delete a test to make the fix fit.
5. A genuinely-empty page that a site really does serve is still distinguishable from a failure in
   the error message (name the URL, engine and status, as `readable` does).
6. `docs/features/apps.md` is **Director-owned this round**. Report your doc text; do not edit it.

## Risks / non-goals

- **Non-goal:** `readable`. It was scouted this round and is **clean** — declaration, emission,
  guard and tests all agree. Read it as the reference and leave it alone apart from the shared
  predicate.
- **Non-goal:** a `min_chars` param or any policy knob. The defect is that zero is treated as a
  measurement; that needs a guard, not a setting.
- **Risk:** a site that legitimately serves a near-empty page would now fail its watch instead of
  reporting "0 chars". That is the correct trade — `readable` already made it, and a failed job is
  visible where a false alert is actively misleading.

## Build record

(filled during build)
