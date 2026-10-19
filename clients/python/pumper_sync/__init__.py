"""pumper-sync — the Python twin of `@pumper/sync`.

Same job, same guarantees, same watermark loop: mirror one canonical Pumper
dataset incrementally into whatever store the product already has, instead of
hand-rolling an export → normalize → upsert script per product.

Deliberately stdlib-only (`urllib`, `json`). A consumer SDK whose reason to
exist is "stop re-deriving this in every product" should not arrive with a
dependency tree; `requests` buys nothing this needs.

The wire types in `pumper_sync.generated` are generated from
`clients/openapi.json` by `just clients` — they are not maintained by hand, and
CI fails if they drift from the spec the server serves.
"""

from .client import PumperClient, PumperHttpError
from .sync import (
    DatasetRef,
    MemoryWatermark,
    PumperSync,
    SyncResult,
    SyncSink,
    WatermarkStore,
)

__all__ = [
    "DatasetRef",
    "MemoryWatermark",
    "PumperClient",
    "PumperHttpError",
    "PumperSync",
    "SyncResult",
    "SyncSink",
    "WatermarkStore",
]

__version__ = "0.1.0"
