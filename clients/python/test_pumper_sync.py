"""Conformance tests for `pumper-sync` (Python), the twin of
`clients/typescript/test/conformance.test.ts`.

Stdlib `unittest`, no network: the client is driven against the SAME fixture
files the TypeScript SDK's conformance test uses, so the two SDKs are pinned to
one set of shapes rather than to each other's beliefs about them. Run with

    python -m unittest discover -s clients/python

The shape half reads the required property set out of `clients/openapi.json` —
the document the server's router generates and a Rust test pins to it — so a
field renamed or ADDED server-side fails here on the next regeneration.
"""

from __future__ import annotations

import io
import json
import unittest
from pathlib import Path

from pumper_sync import DatasetRef, MemoryWatermark, PumperClient, PumperHttpError, PumperSync
from pumper_sync.sync import later_iso

ROOT = Path(__file__).resolve().parents[2]
SPEC = ROOT / "clients" / "openapi.json"
FIXTURES = ROOT / "clients" / "typescript" / "test" / "fixtures"


def fixture(name: str):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def schema_properties(name: str) -> set[str]:
    doc = json.loads(SPEC.read_text(encoding="utf-8"))
    schema = doc["components"]["schemas"][name]
    return set(schema.get("properties", {}))


class ShapeConformance(unittest.TestCase):
    def test_record_fixture_carries_every_served_field(self):
        record = fixture("record.json")
        missing = schema_properties("RecordDto") - set(record)
        self.assertEqual(missing, set(), f"record fixture is missing served fields: {missing}")

    def test_revision_fixture_carries_every_served_field(self):
        page = fixture("revision-page.json")
        for item in page["items"]:
            missing = schema_properties("RevisionDto") - set(item)
            self.assertEqual(missing, set(), f"revision fixture is missing: {missing}")

    def test_generated_module_covers_the_whole_document(self):
        from pumper_sync import generated

        doc = json.loads(SPEC.read_text(encoding="utf-8"))
        declared = set(doc["components"]["schemas"])
        emitted = set(generated.__all__) - {"Json"}
        self.assertEqual(
            declared - emitted,
            set(),
            "the generated module is stale — run `just clients`",
        )


class RetryPolicy(unittest.TestCase):
    """The retry map is a POLICY, and the one entry that must never move is
    `budget_exhausted`: a spend ceiling that a client retries around is not a
    ceiling."""

    def error(self, status: int, code: str) -> PumperHttpError:
        return PumperHttpError(status, code, "x", "http://x")

    def test_transient_codes_retry(self):
        for code, status in (("rate_limited", 429), ("unavailable", 503), ("bad_gateway", 502)):
            self.assertTrue(self.error(status, code).retryable, code)

    def test_budget_exhausted_is_not_retried(self):
        self.assertFalse(self.error(402, "budget_exhausted").retryable)

    def test_terminal_codes_are_not_retried(self):
        for code, status in (
            ("not_found", 404),
            ("unprocessable", 422),
            ("conflict", 409),
            ("forbidden", 403),
            ("confirmation_required", 428),
        ):
            self.assertFalse(self.error(status, code).retryable, code)

    def test_a_body_that_is_not_the_envelope_falls_back_to_the_status(self):
        # A proxy's HTML error page in front of the node: `code` is None, which
        # is itself the signal that the answer did not come from Pumper.
        err = PumperHttpError(503, None, "<html>", "http://x")
        self.assertIsNone(err.code)
        self.assertTrue(err.retryable)


class Watermark(unittest.TestCase):
    def test_later_iso_takes_the_max_without_parsing(self):
        self.assertEqual(later_iso(None, "2026-01-01T00:00:00Z"), "2026-01-01T00:00:00Z")
        self.assertEqual(later_iso("2026-02-01T00:00:00Z", "2026-01-01T00:00:00Z"), "2026-02-01T00:00:00Z")
        self.assertEqual(later_iso("2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z"), "2026-02-01T00:00:00Z")


class RecordingSink:
    def __init__(self):
        self.upserts: list[tuple[str, object]] = []
        self.tombstones: list[str] = []

    def upsert(self, records):
        self.upserts.extend(records)
        return len(records)

    def tombstone(self, keys):
        self.tombstones.extend(keys)
        return len(keys)


class FakeClient(PumperClient):
    """A client whose transport is a canned body, so the loop is exercised
    without a server. Overrides `_request`, not the public methods, so the query string
    and JSON decoding under test stay real."""

    def __init__(self, body: str, urls: list[str]):
        super().__init__("http://test", retries=0)
        self._body = body
        self._urls = urls

    def _request(self, url: str):
        self._urls.append(url)
        return io.BytesIO(self._body.encode("utf-8"))


class SyncLoop(unittest.TestCase):
    def test_snapshot_upserts_live_records_and_tombstones_removed_ones(self):
        live = fixture("record.json")
        removed = fixture("record-removed.json")
        urls: list[str] = []
        client = FakeClient(json.dumps(live) + "\n" + json.dumps(removed) + "\n", urls)
        sink = RecordingSink()
        result = PumperSync(
            DatasetRef("grants", "unified"), MemoryWatermark(), sink, client=client
        ).run()

        self.assertEqual(result.mode, "snapshot")
        self.assertEqual([k for k, _ in sink.upserts], [live["key"]])
        self.assertEqual(sink.tombstones, [removed["key"]])
        self.assertIsNotNone(result.watermark)

    def test_snapshot_asks_for_tombstones_and_every_trust_level(self):
        # A mirror that cannot see a tombstone can never delete, and one that
        # only sees `stable` silently drops provisional rows the product may
        # still want. Both are the query string, not the loop, so pin it.
        urls: list[str] = []
        client = FakeClient("", urls)
        PumperSync(DatasetRef("grants", "unified"), MemoryWatermark(), RecordingSink(), client=client).run()
        self.assertIn("trust=all", urls[0])
        self.assertIn("removed=include", urls[0])
        self.assertIn("format=ndjson", urls[0])

    def test_incremental_applies_the_newest_revision_per_key_only(self):
        page = {
            "items": [
                {"key": "a", "change": "changed", "data": {"v": 2}, "created_at": "2026-02-02T00:00:00Z"},
                {"key": "a", "change": "changed", "data": {"v": 1}, "created_at": "2026-02-01T00:00:00Z"},
                {"key": "b", "change": "removed", "data": None, "created_at": "2026-02-01T00:00:00Z"},
            ],
            "next_cursor": None,
        }
        urls: list[str] = []
        client = FakeClient(json.dumps(page), urls)
        sink = RecordingSink()
        wm = MemoryWatermark()
        ds = DatasetRef("grants", "unified")
        wm.set(ds, "2026-01-01T00:00:00Z")

        result = PumperSync(ds, wm, sink, client=client).run()

        self.assertEqual(result.mode, "incremental")
        self.assertEqual(sink.upserts, [("a", {"v": 2})], "the feed is newest-first; the first wins")
        self.assertEqual(sink.tombstones, ["b"])
        self.assertEqual(wm.get(ds), "2026-02-02T00:00:00Z")

    def test_an_empty_dataset_does_not_fabricate_a_watermark(self):
        client = FakeClient("", [])
        wm = MemoryWatermark()
        ds = DatasetRef("grants", "unified")
        result = PumperSync(ds, wm, RecordingSink(), client=client).run()
        self.assertIsNone(result.watermark)
        self.assertIsNone(wm.get(ds), "an empty snapshot must not advance anything")


class Subscribe(unittest.TestCase):
    def test_subscribe_walks_forward_and_stops_when_caught_up(self):
        page = {
            "count": 2,
            "next_after": None,
            "latest_seq": 2,
            "retained": 2,
            "pending": 0,
            "retention_days": 7,
            "events": [
                {"seq": 1, "kind": "job.succeeded", "app": "grants-gov", "subject_id": "j1", "payload": {}, "created_at": "2026-01-01T00:00:00Z"},
                {"seq": 2, "kind": "dataset.changed", "app": "grants", "subject_id": "unified", "payload": {}, "created_at": "2026-01-01T00:00:01Z"},
            ],
        }
        client = FakeClient(json.dumps(page), [])
        seqs = [e["seq"] for e in client.subscribe(cursor=0)]
        self.assertEqual(seqs, [1, 2], "a null next_after ends the walk rather than spinning")


if __name__ == "__main__":
    unittest.main()
