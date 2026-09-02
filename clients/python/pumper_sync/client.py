"""Low-level, stateless Pumper client: thin typed wrappers over the dataset read
surface (`docs/features/http-api.md`). No watermark, no persistence — that is
`sync.py`.

The retry policy is the one thing here that is a POLICY and not a wrapper, so it
is stated rather than tuned: the server's error envelope carries a stable `code`
(`crates/server/src/routes/error.rs`), and only three of those codes describe a
condition that a later, identical request could survive — `rate_limited`,
`unavailable`, `bad_gateway`. Everything else is the caller's fault or a real
refusal, and retrying it converts one clear error into several slow ones. A
`budget_exhausted` in particular must NEVER be retried: the ceiling it names is
the point.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Iterator

from .generated import EventLogPage, Json, RecordDto, RevisionPageDto

DEFAULT_BASE_URL = "http://127.0.0.1:8088"

#: Error codes whose condition can clear on its own. Everything else — including
#: `budget_exhausted`, `unprocessable`, `not_found`, `conflict` — is terminal.
RETRYABLE_CODES = frozenset({"rate_limited", "unavailable", "bad_gateway"})
RETRYABLE_STATUSES = frozenset({429, 502, 503, 504})


class PumperHttpError(RuntimeError):
    """A non-2xx answer, carrying the server's stable `code` alongside the text.

    `code` is what a caller should branch on; `message` is prose and is not a
    contract. `code` is None when the body was not the standard envelope (a
    proxy's HTML error page, say) — which is itself worth knowing, because it
    means the answer did not come from Pumper.
    """

    def __init__(self, status: int, code: str | None, message: str, url: str) -> None:
        super().__init__(f"{status} {code or 'unknown'} for {url}: {message}")
        self.status = status
        self.code = code
        self.message = message
        self.url = url

    @property
    def retryable(self) -> bool:
        if self.code is not None:
            return self.code in RETRYABLE_CODES
        return self.status in RETRYABLE_STATUSES


class PumperClient:
    """Stateless reader for one Pumper node.

    :param base_url: defaults to ``$PUMPER_URL`` then ``http://127.0.0.1:8088``.
    :param api_key: sent as ``Authorization: Bearer`` when ``[auth] mode = keys``.
    :param retries: attempts AFTER the first for a retryable failure.
    """

    def __init__(
        self,
        base_url: str | None = None,
        *,
        api_key: str | None = None,
        timeout: float = 30.0,
        retries: int = 3,
        backoff: float = 0.5,
    ) -> None:
        import os

        self.base_url = (base_url or os.environ.get("PUMPER_URL") or DEFAULT_BASE_URL).rstrip("/")
        self.api_key = api_key or os.environ.get("PUMPER_API_KEY")
        self.timeout = timeout
        self.retries = retries
        self.backoff = backoff

    # -- transport ---------------------------------------------------------

    def _request(self, url: str) -> Any:
        req = urllib.request.Request(url, headers=self._headers())
        return urllib.request.urlopen(req, timeout=self.timeout)  # noqa: S310

    def _headers(self) -> dict[str, str]:
        headers = {"accept": "application/json", "user-agent": "pumper-sync-python/0.1.0"}
        if self.api_key:
            headers["authorization"] = f"Bearer {self.api_key}"
        return headers

    def _open(self, url: str) -> Any:
        """Open `url`, retrying only what the error map says can clear.

        Exponential, not jittered: this is a single-consumer batch mirror
        talking to one local node, so there is no thundering herd to spread.
        Saying that is cheaper than pretending the jitter would matter.
        """
        attempt = 0
        while True:
            try:
                return self._request(url)
            except urllib.error.HTTPError as exc:
                body = exc.read().decode("utf-8", "replace")
                code, message = _parse_error(body)
                err = PumperHttpError(exc.code, code, message or body[:200], url)
                if not err.retryable or attempt >= self.retries:
                    raise err from None
            except urllib.error.URLError as exc:
                if attempt >= self.retries:
                    raise PumperHttpError(0, "unavailable", str(exc.reason), url) from None
            time.sleep(self.backoff * (2**attempt))
            attempt += 1

    def get_json(self, path: str, params: dict[str, Any] | None = None) -> Any:
        url = f"{self.base_url}{path}"
        if params:
            url = f"{url}?{urllib.parse.urlencode(params, doseq=True)}"
        with self._open(url) as resp:
            return json.loads(resp.read().decode("utf-8"))

    # -- dataset surface ---------------------------------------------------

    def export_records(
        self,
        app: str,
        dataset: str,
        filters: list[str] | None = None,
    ) -> Iterator[RecordDto]:
        """Stream a full (optionally filtered) snapshot as canonical records.

        Constant memory, no row cap — filters are pushed into SQL server-side,
        so only matching rows cross the wire.

        Explicitly requests ``trust=all&removed=include``, for the same reason
        the TypeScript SDK does: a snapshot mirror must see every record (each
        carries its own ``trust`` stamp to branch on) *and* every tombstone.
        Without ``removed=include`` a cold start would never see a
        previously-removed key and could never tombstone it through the sink.
        """
        query = [("format", "ndjson"), ("trust", "all"), ("removed", "include")]
        query += [("filter", f) for f in (filters or [])]
        url = (
            f"{self.base_url}/datasets/{urllib.parse.quote(app)}/"
            f"{urllib.parse.quote(dataset)}/export?{urllib.parse.urlencode(query)}"
        )
        with self._open(url) as resp:
            for line in resp:
                text = line.decode("utf-8").strip()
                if text:
                    yield json.loads(text)

    def changes_page(
        self,
        app: str,
        dataset: str,
        since: str | None,
        cursor: str = "",
        limit: int = 1000,
        trust: str = "stable",
    ) -> RevisionPageDto:
        """One keyset page of the change feed (newest-first).

        ``cursor`` is sent even when empty: its PRESENCE is what selects the
        paged response shape and walks the feed past the legacy row clamp.
        """
        params: dict[str, Any] = {"cursor": cursor, "limit": limit, "trust": trust}
        if since:
            params["since"] = since
        return self.get_json(
            f"/datasets/{urllib.parse.quote(app)}/{urllib.parse.quote(dataset)}/changes",
            params,
        )

    def events_page(
        self,
        after: int = 0,
        *,
        kind: str | None = None,
        app: str | None = None,
        limit: int | None = None,
    ) -> EventLogPage:
        """One page of the durable event log past ``after`` (N05)."""
        params: dict[str, Any] = {"after": after}
        if kind:
            params["kind"] = kind
        if app:
            params["app"] = app
        if limit:
            params["limit"] = limit
        return self.get_json("/events/log", params)

    def subscribe(
        self,
        cursor: int = 0,
        *,
        kind: str | None = None,
        app: str | None = None,
        limit: int | None = None,
    ) -> Iterator[Json]:
        """Walk the event log forward from ``cursor`` until the server says you
        are caught up (``next_after is None``).

        This terminates; it does not tail. Poll it on your own interval — the
        ``next_after: null`` page is the signal to back off. The consumer owns
        the cursor: persist ``event["seq"]`` once you have DURABLY handled the
        event and pass it back next time. At-least-once is the server's cursor
        plus your commit, never client state.
        """
        after = cursor
        while True:
            page = self.events_page(after, kind=kind, app=app, limit=limit)
            for event in page.get("events", []):
                after = event["seq"]
                yield event
            next_after = page.get("next_after")
            if next_after is None:
                return
            after = next_after


def _parse_error(body: str) -> tuple[str | None, str]:
    """Pull `{code, error}` out of the standard envelope, tolerating anything
    that is not one — an error page from a proxy in front of the node is still
    an error, and losing it to a JSON parse failure would be the worst possible
    time to be strict."""
    try:
        parsed = json.loads(body)
    except (ValueError, TypeError):
        return None, body[:200]
    if not isinstance(parsed, dict):
        return None, body[:200]
    code = parsed.get("code")
    return (code if isinstance(code, str) else None), str(parsed.get("error", ""))
