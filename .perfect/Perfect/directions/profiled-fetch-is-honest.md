---
slug: profiled-fetch-is-honest
type: perfect/direction
context: "[[http-engine]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-13
accepted: 2026-08-13
shipped: 2026-08-13
commit: 2e46fd0 + 3c806b1
---

## What & why

Three ways a named login profile silently stops being a login, all in one file, all
invisible to the user.

**1. A missing jar starts empty, with no signal at all.** `ProfileJar::load` maps
`NotFound => CookieStore::default()` (`lib.rs:89`) — no warn, no metric, no error. A
mistyped `profile: "acme_portl"`, or a box moved without `data/profiles/`, fetches the
login wall with a `200`; it clears `min_content_chars`, the tiered fetcher records
`TierVerdict::Ok` (`fetcher.rs:891-921`), and the extractor writes a dataset revision of
the login page as real data. Worse, `create_dir_all` runs **before** the open (`:81-83`),
so the typo *materialises* `data/profiles/acme_portl/` and it then shows up in
`GET /profiles` (`runtime.rs:278`) as a real profile — indistinguishable from one that is
legitimately not logged in yet. **This is already written down as a known gap**:
`docs/features/fetching.md:189` says "engine-http maps a missing `cookies.json` to an
empty store with no warning … Nothing downstream can tell, so it is extracted and stored
as a real dataset revision." Round 18 fixed this one layer up in three places
(`engine-browser` transact, `engine-remote`'s `must_serve_locally`, the `/fetch-proxy`
door). `engine-http` has **zero** callers of `require_existing_profile` — measured: the
guard's only production reader workspace-wide is `engine-browser/src/lib.rs:999`.

**2. A failed jar write loses the login permanently, and retries never happen.**
`flush_loop` clears the dirty flag *before* saving (`:139-143`): `if self.dirty.swap(false)
{ if let Err(e) = self.save() { warn!(…) } }`. On a transient save failure — a Windows
sharing violation, the exact case `error.rs:299-330` documents as the reason `Error::Profile`
is retryable — the flag is already `false`, so the cookie is **never written**. The user
stays logged in for the life of the process and is silently logged out after restart.
Also: no `fsync` before the rename (`:116-118`), so a power loss can leave a zero-length
`cookies.json`, which the next load reads as "corrupt" and replaces with an empty jar.

**3. An empty jar can clobber a real one.** `jar_for` returns the cached `Arc` without
re-checking disk (`:307-309`), and `save()` renames over the path unconditionally with no
read-modify-write and no mtime check (`:105-121`). Start the server while the file is
missing → in-memory jar is empty → an operator restores `cookies.json` from backup → the
next profiled response `touch`es the jar and the debounced flush **overwrites the restored
session with the empty one**, logging `debug!("cookie jar saved")`.

## Evidence

- `crates/engine-http/src/lib.rs:89` — `NotFound => CookieStore::default()`, no signal
- `crates/engine-http/src/lib.rs:81-83` — `create_dir_all` materialises a typo'd profile
- `crates/engine-http/src/lib.rs:307-315` — cached `Arc`, never re-reads disk; `debug!` is identical for a 50-cookie jar and an empty one
- `crates/engine-http/src/lib.rs:105-121` — unconditional `rename`, no fsync
- `crates/engine-http/src/lib.rs:139-143` — dirty cleared before save; `Err` arm does not re-arm
- `crates/core/src/engine.rs:616` — `require_existing_profile` exists; readers measured = 1 (`engine-browser/src/lib.rs:999`), zero in engine-http
- `docs/features/fetching.md:189` — the gap, documented and unfixed at this layer
- `crates/core/src/fetcher.rs:330-361` — `FetchOutcome` has a `snapshot` field for exactly this "the body isn't what you think" class, and no channel for "this ran anonymously"
- `crates/server/src/e2e/engine_conformance.rs:354-417` — the profile probe tests only an *unsafe name*; an **absent** jar is never probed

## Acceptance criteria

1. A profiled fetch whose jar is absent or empty is no longer indistinguishable from a
   logged-in one. **Two levers, and the choice matters — pick with reasoning:** (a) refuse
   at the seam like `require_existing_profile` does for transact, or (b) keep
   create-on-first-use (the browser render tier deliberately relies on it —
   `engine.rs:604-615`) and make the degradation *observable* by carrying it out of the
   engine. If you take (b), the r17 `FETCHED_VIA_HEADER` precedent (`engine.rs:124`) is the
   established shape for engine→caller provenance on this path.
2. Whichever lever: `GET /profiles` must not keep inventing profiles from typos. Directory
   creation moves to first successful save, or the typo is refused before it touches disk.
3. A failed jar save is retried, not dropped. The dirty flag survives an `Err`.
4. `save()` cannot destroy a non-empty on-disk jar with an empty in-memory one.
5. Tests for all three: absent-jar behavior, save-failure re-arm, and the clobber sequence.
   The existing `crates/engine-http/tests/profiles.rs` is the right home and already has the
   live-axum + "restart" idiom to copy.
6. `docs/features/fetching.md:189` stops describing this as a live gap, and the engine
   conformance battery's profile probe covers the **absent** jar, not only the unsafe name.

## Risks / non-goals

- **Do not make `Error::Profile` terminal.** `error.rs:322-330` argues correctly that the
  variant has a genuinely transient producer (jar IO). r18 hit this and chose a different
  lever for the same reason. If you need a terminal signal, use a different variant.
- Create-on-first-use is load-bearing for the browser login flow — breaking it to fix the
  http tier is a regression, not a fix. Verify `engine.rs:604-615` before choosing lever (a).
- Non-goal: the multi-process tmp-file collision (`with_extension("json.tmp")` is a fixed
  path per profile). Real, but two pumper processes sharing one `profiles_dir` is out of
  scope — note it if you touch that code.
- Non-goal: flush-on-shutdown. There is no hook today (measured: zero in `crates/server/`);
  that is its own direction.

## Build record

**Shipped `2e46fd0` (engine half) + `3c806b1` (fetcher half). Director verdict: KEEP.** All six
criteria met.

**Criterion 1 — took lever (b), observable degradation, and the deciding argument was one this
note listed as a *risk* rather than a reason**: `docs/features/fetching.md:230` documents
establishing a login by "driving a login POST on the HTTP tier", which **is** a profiled fetch
with no jar. Refusing at the seam would have broken the tier's own onboarding path. So: a WARN at
load, plus a reserved `x-pumper-anonymous-profile` header on the `FETCHED_VIA_HEADER` pattern,
written **both ways round on every profiled response** (so an origin cannot forge it) and read
only when the caller asked for a profile.

A subtlety neither the brief nor the scout named: **`sent_anonymous` is captured *before* `send`**,
because a login response's own `Set-Cookie` is applied to the jar by reqwest during the call and
would otherwise mask the fact that *this* request carried nothing.

Criterion 2: `create_dir_all` moved from jar *load* to the first *save* that has a cookie —
verified against `GET /profiles`, which enumerates directories, so a typo genuinely stops
appearing there. Criterion 3: the dirty flag survives an `Err` and the write is retried, bounded
at `MAX_SAVE_RETRIES = 5` so a permanently unwritable path cannot become a warn-per-second
forever. Criterion 4: `save_decision` is a three-way enum (`Write` / `NothingToPersist` /
`WouldClobber`) whose doc **states the cost of the rule honestly** — a genuine logout no longer
erases the stored jar, so a dead cookie survives until the next login overwrites it; the cheaper
failure, since the site rejects a dead cookie whereas a clobbered session has no recovery.

Criterion 5: three live tests in `tests/profiles.rs` — absent jar, save-failure re-arm (via a
blocking file later removed), and the clobber-a-restored-backup sequence. Criterion 6:
`fetching.md:189` no longer describes this as a live gap, and the conformance battery's profile
probe now covers the **absent** jar (`an_absent_profile_is_marked_on_the_response_and_never_invents_a_profile`),
not only the unsafe name.

`3c806b1` lifts the marker into the escalation trail (so it reaches `cost_events.detail` and the
job receipt) and into `TierTrace.detail` **including the winning entry** — the winning fetch is
the one about to be stored as a revision. Bonus simplification: the two hand-rolled `detail` match
arms (winning / losing) collapsed into one `http_tier_detail` renderer, so a third note could not
drift between two places. `Error::Profile` correctly left non-terminal, per this note's risk.

**Left open, builder-disclosed and now recorded in `fetching.md`'s Known gaps**: no `fsync` before
the jar rename (named in this note's evidence, not in its criteria); the fixed-path `json.tmp`
collision between two processes sharing one `profiles_dir`; and flush-on-shutdown. All three were
explicit non-goals. Also: `ANONYMOUS_PROFILE_HEADER` and its helpers are reachable as
`pumper_core::engine::…` but not from the crate root, because `crates/core/src/lib.rs` was outside
the write set — consistent with how `fetcher::REMOTE_NODE_HEADER` is consumed today, so judged a
non-gap rather than silently worked around.
