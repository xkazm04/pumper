---
slug: openapi-spec
type: perfect/direction
context: "[[HTTP API & Routes]]"
lens: wildcard
status: shipped
size: M
proposed: 2026-07-13
accepted: 2026-07-13
shipped: 2026-07-13
commit: 343341a
---

## What & why
~40 routes documented only in markdown. Generate an OpenAPI spec (utoipa) served at /openapi.json (+ optional Swagger UI) — client codegen, and a machine-readable tool surface pumper's CLI-agent audience can ingest directly. Deferred T7 item; doubles as the contract check keeping docs/features/http-api.md honest.

## Evidence
- No spec/utoipa anywhere: routes.rs:18-66 (scout-confirmed); harness-learnings.md:29 (OpenAPI deferred)

## Acceptance criteria
- [ ] utoipa annotations covering all routes in routes.rs with request/response schemas.
- [ ] GET /openapi.json serves the spec; spec validates (swagger-cli or utoipa's own test).
- [ ] Response shapes match reality for the paginated dual-mode endpoints (documented as oneOf).
- [ ] docs/features/http-api.md points to /openapi.json as the canonical surface.

## Risks / non-goals
- Risk: annotation drift — mitigate with a test asserting every registered route appears in the spec.
- Non-goal: generated clients committed to the repo.

## Build record
(pending)
