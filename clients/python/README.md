# pumper-sync (Python)

The Python twin of [`@pumper/sync`](../typescript/README.md). Same job, same
watermark loop, same guarantees — mirror one canonical Pumper dataset
incrementally into whatever store your product already has.

```python
from pumper_sync import DatasetRef, PumperClient, PumperSync

class DuckSink:
    def upsert(self, records):   # [(key, data), …]
        ...
        return len(records)
    def tombstone(self, keys):
        ...
        return len(keys)

sync = PumperSync(
    DatasetRef("grants", "unified"),
    watermark=MyWatermarkTable(),      # get(ds) -> str | None; set(ds, str)
    sink=DuckSink(),
    client=PumperClient("http://127.0.0.1:8088"),
    filters=["$.status:eq:open"],      # pushed into SQL server-side
)
result = sync.run()   # SyncResult(mode=…, upserted=…, tombstoned=…, watermark=…)
```

Call `run()` on a schedule. The first call with no stored watermark streams a
filtered snapshot; every call after that pulls only the change-feed delta. The
watermark advances **after** the sink commits, so a crash mid-run re-processes
idempotently rather than skipping.

## What is generated and what is not

`pumper_sync/generated.py` is emitted from `clients/openapi.json` by
`just clients` — do not edit it. Everything else (the client, the sync loop,
the protocols your product implements) is hand-written, because none of it is a
wire shape.

## Reading the event log

```python
for event in PumperClient().subscribe(cursor=last_seq_you_durably_handled):
    handle(event)
    last_seq_you_durably_handled = event["seq"]
```

`subscribe` terminates when the server says you are caught up (`next_after`
is `null`); poll it on your own interval. It does not tail, and it remembers
nothing — at-least-once is the server's cursor plus your commit.

## Retries

Only `rate_limited`, `unavailable` and `bad_gateway` are retried, with
exponential backoff. Everything else is a real refusal. `budget_exhausted` is
never retried: the ceiling it names is the point.

## Tests

```
python -m unittest discover -s clients/python -t clients/python
```

Stdlib only, no network. They read the same fixtures the TypeScript conformance
test uses, and check the field sets against `clients/openapi.json`, so both SDKs
are answerable to one artifact rather than to each other.
