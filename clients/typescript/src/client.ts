// Low-level, stateless Pumper client: thin typed wrappers over the dataset read
// surface (`docs/features/http-api.md`). No watermark, no persistence — that is
// `sync.ts`. Use this directly for one-off reads (a filtered export, a page of
// changes); use `createPumperSync` for continuous mirroring.

import { getJson, streamNdjson, type HttpOptions } from "./http.js";
import type {
  DatasetRef,
  PumperEvent,
  PumperEventPage,
  PumperRecord,
  RevisionPage,
} from "./types.js";

const DEFAULT_BASE_URL = "http://127.0.0.1:8088";

export interface PumperClientConfig extends HttpOptions {
  /** Defaults to `$PUMPER_URL` then `http://127.0.0.1:8088`. Trailing slashes trimmed. */
  baseUrl?: string;
}

function resolveBaseUrl(explicit?: string): string {
  const env = typeof process !== "undefined" ? process.env?.PUMPER_URL : undefined;
  return (explicit ?? env ?? DEFAULT_BASE_URL).replace(/\/+$/, "");
}

export class PumperClient {
  readonly baseUrl: string;
  private readonly http: HttpOptions;

  constructor(cfg: PumperClientConfig = {}) {
    this.baseUrl = resolveBaseUrl(cfg.baseUrl);
    this.http = { fetch: cfg.fetch, timeoutMs: cfg.timeoutMs ?? 30_000, maxBytes: cfg.maxBytes };
  }

  private dataset(ds: DatasetRef): string {
    return `${this.baseUrl}/datasets/${encodeURIComponent(ds.app)}/${encodeURIComponent(ds.name)}`;
  }

  /** Stream a full (optionally filtered) snapshot as canonical records. Constant
   *  memory, no row cap — filters are pushed into SQL server-side, so only
   *  matching rows cross the wire.
   *
   *  Explicitly requests `trust=all&removed=include`: a snapshot mirror must see
   *  every record (each carries its own `trust` stamp for the consumer to branch
   *  on) *and* every tombstone, so `PumperSync.snapshot()`'s `rec.removed_at`
   *  check has something to observe. `removed=include` matters because the
   *  server's default flipped to `exclude` — without it a cold-start snapshot
   *  would silently never see a previously-removed key and could never tombstone
   *  it through the sink. See docs/features/datasets.md § Tombstones. */
  exportRecords<T = unknown>(
    ds: DatasetRef,
    filter: string[] = [],
    signal?: AbortSignal,
  ): AsyncGenerator<PumperRecord<T>> {
    const q = new URLSearchParams({ format: "ndjson", trust: "all", removed: "include" });
    for (const f of filter) q.append("filter", f);
    const url = `${this.dataset(ds)}/export?${q.toString()}`;
    return streamNdjson<PumperRecord<T>>(url, { ...this.http, signal });
  }

  /** One keyset page of the change feed (newest-first). `since` is an exclusive
   *  RFC3339 lower bound; `cursor` (even empty) selects the paged response shape
   *  and walks the full feed past the legacy 1000-row clamp.
   *
   *  Explicitly requests `trust=stable` (the server default) — an incremental
   *  mirror should apply only revisions Pumper stands behind; a consumer that
   *  wants everything can pass `trust: "all"`. The change feed has no `removed=`
   *  knob of its own — `removed` revisions are part of the feed's lifecycle
   *  vocabulary (`new`/`changed`/`removed`), not a filterable population. */
  changesPage<T = unknown>(
    ds: DatasetRef,
    since: string | null,
    cursor: string,
    limit = 1000,
    trust: string = "stable",
  ): Promise<RevisionPage<T>> {
    const q = new URLSearchParams();
    if (since) q.set("since", since);
    q.set("cursor", cursor);
    q.set("limit", String(limit));
    q.set("trust", trust);
    const url = `${this.dataset(ds)}/changes?${q.toString()}`;
    return getJson<RevisionPage<T>>(url, this.http);
  }

  /** One page of the durable event log past `after` (N05).
   *
   *  This is the cursor half of the API, and it is a different thing from
   *  `changesPage`: the change feed answers "what records changed in this
   *  dataset", the event log answers "what happened on this server", including
   *  kinds a dataset has no opinion about (`job.failed`, `external`,
   *  `transaction.submitted`). Ascending by `seq`, so a consumer walks forward
   *  from whatever it last stored.
   *
   *  Deliberately NOT an SSE subscription: `@pumper/sync` is a batch mirror with
   *  no long-lived connection anywhere in it, and a durable cursor gives the
   *  same at-least-once guarantee without one. See `subscribe` below. */
  eventsPage<T = unknown>(
    after = 0,
    opts: { kind?: string; app?: string; limit?: number } = {},
  ): Promise<PumperEventPage<T>> {
    const q = new URLSearchParams({ after: String(after) });
    if (opts.kind) q.set("kind", opts.kind);
    if (opts.app) q.set("app", opts.app);
    if (opts.limit) q.set("limit", String(opts.limit));
    return getJson<PumperEventPage<T>>(`${this.baseUrl}/events/log?${q.toString()}`, this.http);
  }

  /** Walk the event log forward from `cursor`, yielding every event until the
   *  server says you are caught up (`next_after === null`).
   *
   *  A generator rather than a callback loop so the consumer owns the cursor:
   *  persist `event.seq` after you have durably handled the event, and pass it
   *  back as `cursor` on the next call. Nothing here retries or remembers —
   *  at-least-once is the server's cursor plus your commit, not client state.
   *
   *  This terminates; it does not tail. Poll it on your own interval (the
   *  `next_after: null` page is the signal to back off). */
  async *subscribe<T = unknown>(
    opts: { cursor?: number; kind?: string; app?: string; limit?: number } = {},
  ): AsyncGenerator<PumperEvent<T>> {
    let after = opts.cursor ?? 0;
    for (;;) {
      const page = await this.eventsPage<T>(after, opts);
      for (const event of page.events) {
        after = event.seq;
        yield event;
      }
      if (page.next_after === null) return;
      after = page.next_after;
    }
  }
}
