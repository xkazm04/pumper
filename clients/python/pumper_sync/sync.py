"""Watermark-driven incremental mirror — the Python twin of `sync.ts`.

Cold start streams a filtered snapshot; every run after that pulls only the
change-feed delta since the last watermark. Post-images are applied straight
from the feed (no follow-up record reads), removed keys are tombstoned, and the
watermark advances only AFTER the sink commits — so a crash mid-run
re-processes idempotently rather than skipping.

The loop is deliberately identical to the TypeScript one, step for step,
including the newest-first de-duplication. Two SDKs that mirror the same feed
with subtly different loops are two different products, and the difference
would only ever show up as a divergent mirror in somebody's database.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Literal, Protocol

from .client import PumperClient
from .generated import Json

SyncMode = Literal["snapshot", "incremental"]


@dataclass(frozen=True)
class DatasetRef:
    """A dataset address: ``<app>/<name>`` (e.g. ``grants/unified``)."""

    app: str
    name: str


@dataclass
class SyncResult:
    mode: SyncMode
    upserted: int
    tombstoned: int
    #: The watermark persisted at the end of this run. ``None`` when the dataset
    #: was empty and nothing advanced it — an honest "we learned nothing", not a
    #: zero.
    watermark: str | None


class WatermarkStore(Protocol):
    """Where the product persists the per-dataset sync watermark.

    The product owns storage (a row, a KV entry, a file); the SDK only reads and
    advances it. The value is an opaque RFC 3339 token — do not parse it.
    """

    def get(self, dataset: DatasetRef) -> str | None: ...

    def set(self, dataset: DatasetRef, watermark: str) -> None: ...


class SyncSink(Protocol):
    """The product's persistence boundary. Returns the count actually written,
    purely for reporting."""

    def upsert(self, records: list[tuple[str, Json]]) -> int: ...

    def tombstone(self, keys: list[str]) -> int: ...


@dataclass
class MemoryWatermark:
    """In-process watermark. For tests and one-shot scripts — a mirror that must
    survive a restart needs a real store, and using this one by accident would
    silently re-snapshot on every run."""

    values: dict[tuple[str, str], str] = field(default_factory=dict)

    def get(self, dataset: DatasetRef) -> str | None:
        return self.values.get((dataset.app, dataset.name))

    def set(self, dataset: DatasetRef, watermark: str) -> None:
        self.values[(dataset.app, dataset.name)] = watermark


def later_iso(a: str | None, b: str) -> str:
    """Lexicographic max of two RFC 3339 watermarks.

    Valid because Pumper stamps fixed-width UTC micros, so string order IS
    chronological order — no parsing, and therefore no timezone to get wrong.
    """
    return b if a is None or b > a else a


class PumperSync:
    """Mirror one canonical dataset. Call :meth:`run` on a schedule; each call
    advances the watermark."""

    def __init__(
        self,
        dataset: DatasetRef,
        watermark: WatermarkStore,
        sink: SyncSink,
        *,
        client: PumperClient | None = None,
        filters: list[str] | None = None,
        map_record: Callable[[Json, str], Json | None] | None = None,
        batch_size: int = 500,
    ) -> None:
        self.dataset = dataset
        self.watermark = watermark
        self.sink = sink
        self.client = client or PumperClient()
        self.filters = filters or []
        self.map_record = map_record
        self.batch_size = batch_size

    def run(self) -> SyncResult:
        since = self.watermark.get(self.dataset)
        return self._snapshot() if since is None else self._incremental(since)

    def _to_out(self, raw: Json, key: str) -> Json | None:
        return self.map_record(raw, key) if self.map_record else raw

    def _snapshot(self) -> SyncResult:
        upserts: list[tuple[str, Json]] = []
        tombstones: list[str] = []
        upserted = tombstoned = 0
        watermark: str | None = None

        def flush() -> None:
            nonlocal upserts, tombstones, upserted, tombstoned
            if upserts:
                upserted += self.sink.upsert(upserts)
                upserts = []
            if tombstones:
                tombstoned += self.sink.tombstone(tombstones)
                tombstones = []

        for rec in self.client.export_records(self.dataset.app, self.dataset.name, self.filters):
            watermark = later_iso(watermark, rec["updated_at"])
            if rec.get("removed_at"):
                tombstones.append(rec["key"])
            else:
                out = self._to_out(rec["data"], rec["key"])
                if out is not None:
                    upserts.append((rec["key"], out))
            if len(upserts) >= self.batch_size or len(tombstones) >= self.batch_size:
                flush()
        flush()

        if watermark is not None:
            self.watermark.set(self.dataset, watermark)
        return SyncResult("snapshot", upserted, tombstoned, watermark)

    def _incremental(self, since: str) -> SyncResult:
        # The feed is newest-first, so the FIRST revision seen for a key is its
        # latest — track seen keys and skip older duplicates so the latest wins.
        seen: set[str] = set()
        upserts: list[tuple[str, Json]] = []
        tombstones: list[str] = []
        watermark = since
        cursor = ""

        while True:
            page = self.client.changes_page(self.dataset.app, self.dataset.name, since, cursor)
            for rev in page.get("items", []):
                watermark = later_iso(watermark, rev["created_at"])
                if rev["key"] in seen:
                    continue
                seen.add(rev["key"])
                if rev["change"] == "removed":
                    tombstones.append(rev["key"])
                elif rev.get("data") is not None:
                    out = self._to_out(rev["data"], rev["key"])
                    if out is not None:
                        upserts.append((rev["key"], out))
            next_cursor = page.get("next_cursor")
            if not next_cursor:
                break
            cursor = next_cursor

        upserted = self.sink.upsert(upserts) if upserts else 0
        tombstoned = self.sink.tombstone(tombstones) if tombstones else 0
        self.watermark.set(self.dataset, watermark)
        return SyncResult("incremental", upserted, tombstoned, watermark)
