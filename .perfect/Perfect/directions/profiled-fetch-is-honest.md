---
slug: profiled-fetch-is-honest
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

(filled during build)
