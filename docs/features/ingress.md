# Inbound event ingress

Pumper's pipelines historically started from cron or a manual enqueue — every
event the system knew about was one it produced itself. Inbound ingress opens
the other direction: `POST /ingest/{source}` accepts an HMAC-signed external
webhook, stamps it onto the event bus as an `external` event (visible on
`GET /events` and its replay ring like any job transition), and lets triggers
match it. GitHub push → re-crawl the docs; a partner system's "new client" →
run a research job — the reactive DAG now reacts to anything that can POST.

> **Security posture — read this first.** This is pumper's first write surface
> designed for non-localhost callers. It ships **disabled**
> (`[ingress] enabled = false`); every source has its own mandatory HMAC
> secret; bodies are size-capped (`max_body_bytes`, default 256 KiB); each
> source is token-bucket rate-limited (`rate_limit_per_min`, default 60);
> timestamped signatures are bound to a skew window (`max_skew_secs`, default
> 300 s). If you expose the port beyond localhost, expose only what you mean
> to.

## Ingress sources

A source is a named credential for one external caller:

| Route | What it does |
|---|---|
| `GET /ingress/sources` | List sources (secrets are never returned) |
| `POST /ingress/sources` | Create — `{name, secret?}`; a missing secret is generated and returned **once** |
| `POST /ingress/sources/{id}/enabled` | Enable / disable one source |
| `DELETE /ingress/sources/{id}` | Delete |

The CRUD surface works even while `[ingress] enabled = false`, so sources can
be staged before the switch is flipped.

## Signing

`POST /ingest/{id}` requires `x-pumper-signature: sha256=<hex>` computed with
the source's secret. Two bases are accepted (verification is constant-time):

- **Pumper scheme** — when `x-pumper-timestamp` is present:
  `HMAC-SHA256(secret, "{ts}.{delivery_id}." + body)` with the delivery id
  from `x-pumper-delivery-id` (may be empty). This is byte-for-byte the scheme
  pumper's *outbound* webhooks sign with, so one pumper can ingest another's
  deliveries directly. The timestamp must be within `max_skew_secs` of now.
- **Bare scheme** — no timestamp header: `HMAC-SHA256(secret, body)`. This is
  exactly GitHub's `x-hub-signature-256` computation, and that header is
  accepted as an alias for `x-pumper-signature`.

### Event ids and the replay posture

The event id scopes trigger idempotency (`trig:{trigger_id}:{event_id}`), so
**what the event id is derived from decides what counts as a replay.** Every
derivation is deterministic — a UUID-shaped id is never minted at random:

| Sender supplies | Event id |
|---|---|
| `x-pumper-delivery-id` that parses as a UUID | that UUID, verbatim |
| `x-pumper-delivery-id` that does not | `SHA-256("{source_id}:{delivery_id}")`, first 16 bytes |
| no delivery id | `SHA-256(source_id ++ "\0body\0" ++ body)`, first 16 bytes |

(Truncated SHA-256 wearing the UUID shape — **not** UUIDv5: no version or
variant bits are stamped. Earlier revisions of this page claimed UUIDv5.)

Consequences, stated plainly:

- A **redelivery** (same delivery id) re-verifies but cannot double-fire.
- An **exact replay with no delivery id** — the case the bare/GitHub scheme
  cannot otherwise detect, since it carries neither timestamp nor nonce, so a
  captured body verifies forever — now derives the same event id and therefore
  dedupes: `202` with `triggers_fired: 0` and no new job. Byte-identical
  re-sends from a legitimate sender collapse the same way; that is the correct
  reading of "same event", but if your sender legitimately emits identical
  bodies for distinct events, give it a delivery id.
- A **timestamped (pumper-scheme) capture** is additionally bounded by
  `max_skew_secs`, checked *before* the MAC.
- Nothing about this changes senders that supply delivery ids.

A `202` response returns `{event_id, seq, triggers_fired}`; `seq` is the
event's position in the `/events` replay ring.

### Refusal gates, in the order they are applied

`409` ingress disabled → `404` unknown source → `403` source disabled →
`429` rate limit → `413` body over `max_body_bytes` → `401` missing/invalid
signature or stale timestamp → `400` body is not JSON. The rate limit sits
*above* the crypto deliberately: a flood of bad signatures must burn the same
bucket as good traffic.

## External triggers

Triggers gain a third source kind:

```json
POST /triggers
{
  "source_kind": "external",
  "source_app": "<ingress source id, or '*'>",
  "filters": ["$.ref:eq:refs/heads/main"],
  "target_app": "crawl",
  "params": { "url": "https://acme.dev/docs" }
}
```

- `source_app` filters by ingress source (`'*'` = any source).
- `filters` are `$.path:op:value` specs — the exact `?filter=` grammar the
  dataset query surface uses (`eq`, `contains`, `gte`, `lte`, `numgte`) —
  ANDed against the inbound JSON payload. Omitted = match every event from
  the source.
- The fired job's `params._trigger` carries
  `{source_kind: "external", source_id, source_name, event_id, payload, depth, chain}` —
  the payload is inlined because ingress bodies are size-capped at the door.
- Cycle/depth guards, priority, `budget_usd` and `max_attempts` behave exactly
  as for dataset/job triggers (see `triggers.md`).

## Worked example: GitHub push → re-crawl the docs

1. Enable ingress in `config.toml` and restart:

   ```toml
   [ingress]
   enabled = true
   ```

2. Create a source and keep the returned secret:

   ```bash
   curl -s -X POST localhost:8088/ingress/sources \
     -H 'content-type: application/json' \
     -d '{"name": "github-acme-docs"}'
   # -> { "source": { "id": "1c2d...", ... }, "secret": "8a41...e2" }
   ```

3. Create the trigger — only pushes to `main` should re-crawl:

   ```bash
   curl -s -X POST localhost:8088/triggers \
     -H 'content-type: application/json' \
     -d '{
       "source_kind": "external",
       "source_app": "1c2d...",
       "filters": ["$.ref:eq:refs/heads/main"],
       "target_app": "crawl",
       "params": { "url": "https://acme.dev/docs", "max_pages": 200 }
     }'
   ```

4. In the GitHub repo: **Settings → Webhooks → Add webhook** with
   - Payload URL: `https://<your-host>/ingest/1c2d...`
   - Content type: `application/json`
   - Secret: the secret from step 2.

   GitHub signs each delivery as `x-hub-signature-256: sha256=HMAC(secret, body)`
   — pumper verifies that header directly, no relay needed.

5. Push to `main`. The delivery lands as an `external` event on `GET /events`,
   the trigger matches `$.ref`, and a `crawl` job is enqueued with the push
   payload in `params._trigger.payload`. Pushes to other branches emit the
   event but fire nothing.

## Config reference

```toml
[ingress]
enabled = false          # master switch; POST /ingest/{id} returns 409 while off
max_body_bytes = 262144  # hard cap per inbound body (bytes)
rate_limit_per_min = 60  # per-source token bucket (also the burst size)
max_skew_secs = 300      # clock-skew window for timestamped signatures
```
