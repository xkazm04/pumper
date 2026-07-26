// Low-level, stateless Pumper client: thin typed wrappers over the dataset read
// surface (`docs/features/http-api.md`). No watermark, no persistence — that is
// `sync.ts`. Use this directly for one-off reads (a filtered export, a page of
// changes); use `createPumperSync` for continuous mirroring.

import { getJson, streamNdjson, type HttpOptions } from "./http.js";
import type { DatasetRef, PumperRecord, RevisionPage } from "./types.js";

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
   *  matching rows cross the wire. */
  exportRecords<T = unknown>(
    ds: DatasetRef,
    filter: string[] = [],
    signal?: AbortSignal,
  ): AsyncGenerator<PumperRecord<T>> {
    const q = new URLSearchParams({ format: "ndjson" });
    for (const f of filter) q.append("filter", f);
    const url = `${this.dataset(ds)}/export?${q.toString()}`;
    return streamNdjson<PumperRecord<T>>(url, { ...this.http, signal });
  }

  /** One keyset page of the change feed (newest-first). `since` is an exclusive
   *  RFC3339 lower bound; `cursor` (even empty) selects the paged response shape
   *  and walks the full feed past the legacy 1000-row clamp. */
  changesPage<T = unknown>(
    ds: DatasetRef,
    since: string | null,
    cursor: string,
    limit = 1000,
  ): Promise<RevisionPage<T>> {
    const q = new URLSearchParams();
    if (since) q.set("since", since);
    q.set("cursor", cursor);
    q.set("limit", String(limit));
    const url = `${this.dataset(ds)}/changes?${q.toString()}`;
    return getJson<RevisionPage<T>>(url, this.http);
  }
}
