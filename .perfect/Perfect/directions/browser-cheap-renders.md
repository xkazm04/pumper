---
slug: browser-cheap-renders
type: perfect/direction
context: "[[Fetch Engines (HTTP / Browser / Claude)]]"
lens: optimization
status: shipped
size: M
proposed: 2026-07-13
accepted: 2026-07-13
shipped: 2026-07-13
commit: 8d3eda5
---

## What & why
Every render downloads all subresources — no CDP request interception exists; browser-tier fetches are slower/heavier than scraping needs (only the DOM matters). Add config-gated resource blocking (image/font/media blocked by default, per-request opt-out), memory flags, and periodic browser recycle after N renders.

## Evidence
- No interception/blocking anywhere: crates/engine-browser/src/lib.rs (scout-confirmed)
- No memory flags: lib.rs:41 (single stealth flag only)

## Acceptance criteria
- [ ] CDP request interception blocking image/font/media/stylesheet? (choose set; stylesheet often needed for selector waits — justify) — config `[browser] block_resources` default on, RenderRequest opt-out.
- [ ] Memory flags (--disable-dev-shm-usage; JS heap cap) added to launch args.
- [ ] Recycle: relaunch browser after `[browser] recycle_after_renders` (default e.g. 200; 0 disables) — coordinate with [[browser-resilience]]'s relaunchable holder.
- [ ] Live verification: render a page with images, confirm blocked requests (CDP events or timing), report honestly.
- [ ] docs/features/fetching.md updated.

## Risks / non-goals
- Risk: blocking stylesheets can break selector-based waits on CSS-dependent sites — default set must be conservative (images/fonts/media only).

## Build record
(pending)
