// The CLI's transport, typed off the generated document.
//
// Separate from `@pumper/sync` on purpose: that package is a *mirror* — a
// watermark loop with a persistence boundary — and a CLI that pulled it in
// would inherit a contract it does not want and would drag the SDK's version
// into every `npx pumper`. What the two DO share is the artifact both are typed
// from, `clients/openapi.json`, so their shapes cannot disagree even though
// neither imports the other.

import type { components } from "./generated.js";

export type Schemas = components["schemas"];

export const DEFAULT_BASE_URL = "http://127.0.0.1:8088";

/** Codes whose condition can clear on its own (`crates/server/src/routes/error.rs`).
 *
 *  `budget_exhausted` is deliberately NOT here, and neither is any 4xx: a spend
 *  ceiling a client retries around is not a ceiling, and retrying a refusal
 *  turns one clear error into several slow ones. */
const RETRYABLE = new Set(["rate_limited", "unavailable", "bad_gateway"]);

export class PumperCliError extends Error {
  constructor(
    readonly status: number,
    readonly code: string | null,
    message: string,
  ) {
    super(message);
  }
}

export interface ApiOptions {
  baseUrl: string;
  apiKey?: string | undefined;
  retries?: number;
  timeoutMs?: number;
}

function headers(opts: ApiOptions): Record<string, string> {
  const h: Record<string, string> = { accept: "application/json" };
  if (opts.apiKey) h.authorization = `Bearer ${opts.apiKey}`;
  return h;
}

async function once(url: string, opts: ApiOptions): Promise<Response> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), opts.timeoutMs ?? 30_000);
  try {
    return await fetch(url, { headers: headers(opts), signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}

/** GET with backoff on exactly the transient codes, and a typed refusal
 *  otherwise. The CLI prints `code` — never the prose — so a script wrapping it
 *  has something stable to branch on. */
export async function get<T>(url: string, opts: ApiOptions): Promise<T> {
  const retries = opts.retries ?? 3;
  for (let attempt = 0; ; attempt += 1) {
    const res = await once(url, opts);
    if (res.ok) return (await res.json()) as T;
    const body = await res.text();
    let code: string | null = null;
    let message = body.slice(0, 300);
    try {
      const parsed = JSON.parse(body) as { code?: string; error?: string };
      code = parsed.code ?? null;
      message = parsed.error ?? message;
    } catch {
      // Not the standard envelope — a proxy in front of the node, most likely.
      // Keep the raw text: losing it to a parse failure is the worst possible
      // moment to be strict about JSON.
    }
    const canRetry = code ? RETRYABLE.has(code) : res.status >= 500;
    if (!canRetry || attempt >= retries) {
      throw new PumperCliError(res.status, code, message);
    }
    await new Promise((r) => setTimeout(r, 500 * 2 ** attempt));
  }
}

/** Stream a text body line by line, so `datasets export` holds one row in
 *  memory rather than the whole dataset. */
export async function* getLines(url: string, opts: ApiOptions): AsyncGenerator<string> {
  const res = await once(url, opts);
  if (!res.ok) {
    throw new PumperCliError(res.status, null, (await res.text()).slice(0, 300));
  }
  const reader = res.body?.getReader();
  if (!reader) return;
  const decoder = new TextDecoder();
  let buffer = "";
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    let nl = buffer.indexOf("\n");
    while (nl >= 0) {
      const line = buffer.slice(0, nl).trim();
      if (line) yield line;
      buffer = buffer.slice(nl + 1);
      nl = buffer.indexOf("\n");
    }
  }
  const tail = buffer.trim();
  if (tail) yield tail;
}
