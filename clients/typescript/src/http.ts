// Transport primitives: capped/typed JSON GETs and a streaming ndjson reader.
// Dependency-free — uses the global fetch/WebStreams (Node >= 20, Deno, Bun,
// browsers). All Pumper reads go through here so guards live in one place.

export interface HttpOptions {
  fetch?: typeof fetch;
  /** Abort a control request (JSON) after this many ms. Streams are not bounded
   *  by this — pass an AbortSignal to `streamNdjson` if you need to stop one. */
  timeoutMs?: number;
  /** Reject a response whose body exceeds this many bytes (backstop against a
   *  runaway export). Applies to both buffered and streamed reads. */
  maxBytes?: number;
}

/** A typed error carrying Pumper's stable `{error, code}` envelope. Branch on
 *  `.code` (`not_found`, `conflict`, `too_large`, …), never the message. */
export class PumperHttpError extends Error {
  constructor(
    message: string,
    readonly status: number,
    readonly code: string | null = null,
  ) {
    super(message);
    this.name = "PumperHttpError";
  }
}

function errorFromBody(status: number, body: string): PumperHttpError {
  try {
    const parsed = JSON.parse(body) as { error?: string; code?: string };
    if (parsed && typeof parsed.error === "string") {
      return new PumperHttpError(parsed.error, status, parsed.code ?? null);
    }
  } catch {
    // fall through to a generic message
  }
  return new PumperHttpError(`HTTP ${status}`, status);
}

async function* byteChunks(res: Response, maxBytes?: number): AsyncGenerator<Uint8Array> {
  if (!res.body) {
    const buf = new TextEncoder().encode(await res.text());
    if (maxBytes && buf.byteLength > maxBytes) {
      throw new PumperHttpError(`response exceeded ${maxBytes} bytes`, 413, "too_large");
    }
    yield buf;
    return;
  }
  const reader = res.body.getReader();
  let total = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      if (!value) continue;
      total += value.byteLength;
      if (maxBytes && total > maxBytes) {
        throw new PumperHttpError(`response exceeded ${maxBytes} bytes`, 413, "too_large");
      }
      yield value;
    }
  } finally {
    reader.cancel().catch(() => {});
  }
}

/** GET a JSON body with an optional timeout and size cap. */
export async function getJson<T>(url: string, opts: HttpOptions = {}): Promise<T> {
  const f = opts.fetch ?? fetch;
  const ctrl = new AbortController();
  const timer = opts.timeoutMs ? setTimeout(() => ctrl.abort(), opts.timeoutMs) : null;
  try {
    const res = await f(url, { signal: ctrl.signal, headers: { accept: "application/json" } });
    let text = "";
    const decoder = new TextDecoder();
    for await (const chunk of byteChunks(res, opts.maxBytes)) text += decoder.decode(chunk, { stream: true });
    text += decoder.decode();
    if (!res.ok) throw errorFromBody(res.status, text);
    return JSON.parse(text) as T;
  } finally {
    if (timer) clearTimeout(timer);
  }
}

/** Stream an ndjson endpoint line-by-line at constant memory. `signal` lets a
 *  caller stop a long export; there is no implicit timeout on the body. */
export async function* streamNdjson<T>(
  url: string,
  opts: HttpOptions & { signal?: AbortSignal } = {},
): AsyncGenerator<T> {
  const f = opts.fetch ?? fetch;
  const res = await f(url, { signal: opts.signal, headers: { accept: "application/x-ndjson" } });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw errorFromBody(res.status, body);
  }
  const decoder = new TextDecoder();
  let buf = "";
  for await (const chunk of byteChunks(res, opts.maxBytes)) {
    buf += decoder.decode(chunk, { stream: true });
    let nl: number;
    while ((nl = buf.indexOf("\n")) >= 0) {
      const line = buf.slice(0, nl).trim();
      buf = buf.slice(nl + 1);
      if (line) yield JSON.parse(line) as T;
    }
  }
  buf += decoder.decode();
  const last = buf.trim();
  if (last) yield JSON.parse(last) as T;
}
