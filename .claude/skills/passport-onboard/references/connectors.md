# Connector knowledge (Personas Vault)

The Personas app manages the user's service credentials in its **Vault**
(catalog UI: `src/features/vault/sub_catalog`, credentials:
`src/features/vault/sub_credentials`; the service-type registry is
`src/lib/connectors/connectorMeta.tsx`). This skill only ever handles
connector **metadata** — `{id, name, service_type}` — supplied by the dispatch
context block or by asking the user. Credential VALUES stay in the Vault.

**The Vault is the source of truth — not `.env`.** For Sentry, GitHub, and
LLM-tracking, the Personas app reads telemetry through the credential bound on
the project's `dev_projects` slot, never through the target repo's env files.
A dimension that ends with "add SENTRY_DSN to your .env" is NOT done — it is
homework. The onboarding itself must end with the credential **bound in the
app** (see "Binding closes the loop" below). Env vars remain only as the
*runtime* delivery channel for the target app's own process (local dev
convenience + host-dashboard secrets), never as the integration contract with
Personas.

## Service types by passport dimension

| Dimension | Catalog service types (choose by stack fit) |
|---|---|
| Hosting | `vercel`, `netlify`, `cloudflare`, `fly_io`, `railway`, `digitalocean`, `aws`, `firebase`, `kubernetes` |
| Database | `supabase`, `neon`, `postgres_proxy` (any PostgreSQL), `convex`, `upstash` (Redis), `firebase` |
| Auth | `supabase`, `firebase`, plus code-level libs the probe detects (Clerk, Auth.js, Auth0, Better Auth, Lucia, WorkOS, Stytch, Kinde) — a code lib needs no Vault connector, only env vars |
| CI / VCS | `github`, `gitlab`, `azure_devops`, `circleci` |
| Observability (errors/logs/metrics/tracing) | `sentry`, `datadog`, `betterstack`, `pagerduty`, `uptime_robot` |
| LLM tracking | `langfuse`, `helicone`, `langsmith`, `tracklight` (LightTrack) |
| Analytics (occasionally offered under observability) | `posthog`, `mixpanel`, `amplitude`, `segment` |

## The offer flow

1. **An existing connector fits** (context block lists a matching
   service_type, or `list_credentials` shows one): offer it BY NAME — "Reuse
   *my-sentry* (Sentry) and bind it to this project". This is almost always
   the recommended option; the user already trusts that service. On accept,
   BIND it (below) — don't leave it as a report bullet.
2. **No existing connector fits**: offer "Add a <type> connector in Personas →
   Vault first, then bind it". The code-side wiring can still proceed in the
   same run; the binding becomes the ONE named user follow-up (create
   credential → bind slot), not a list of env vars to hand-maintain.
3. **Standalone mode** (no context block): ask — "Which monitoring service do
   you use?" with the catalog's realistic candidates as options + Other.

## Binding closes the loop (`dev_projects` slots)

Three slots on `dev_projects` make the passport read telemetry from the Vault:
`monitoring_credential_id` (+ optional `monitoring_project_slug` for
stats-by-project) lights errors/logs/metrics/tracing,
`llm_tracking_credential_id` lights LLM tracking, and `pr_credential_id`
powers CI/auto-PR. **A connector-flavored dimension is Done only when its slot
is bound.** How to bind, in order of preference:

1. **App reachable** (test-automation harness on :17320, or dispatched with
   IPC access): call it yourself —
   `dev_tools_update_project({ id, monitoringCredentialId, prCredentialId, llmTrackingCredentialId })`
   (camelCase args; pass only the slots you're setting). If the target repo
   isn't a dev project yet, register it first with
   `dev_tools_create_project({ name, rootPath, description, techStack, githubUrl })`.
   Verify by re-listing (`dev_tools_list_projects`) and checking the ids stuck.
2. **App not reachable**: name the exact binding as the user follow-up —
   "bind *my-sentry* on the wall's Error-tracking cell popover" — one action,
   not an env-var checklist.

Only credential **ids** move here; values never leave the Vault's encrypted
store.

## Env-var naming conventions (runtime wiring only)

Env vars are how the TARGET APP's own process reads a service at runtime —
they are NOT how Personas integrates (that's the slot binding above). Use the
service's canonical names — never invent novel ones:
`SENTRY_DSN`, `DATADOG_API_KEY`, `LOGTAIL_SOURCE_TOKEN` (Better Stack),
`LANGFUSE_PUBLIC_KEY`/`LANGFUSE_SECRET_KEY`/`LANGFUSE_HOST`,
`HELICONE_API_KEY`, `LANGCHAIN_API_KEY` (LangSmith), `DATABASE_URL`
(postgres/neon/supabase), `SUPABASE_URL`/`SUPABASE_ANON_KEY`,
`UPSTASH_REDIS_REST_URL`/`UPSTASH_REDIS_REST_TOKEN`, `VERCEL_TOKEN` (CI
deploy). Per environment, the NAME stays the same — the VALUE differs per
deployment target; document that in `.env.example` comments
(`# set per environment: local .env / CI secrets / host dashboard`).
