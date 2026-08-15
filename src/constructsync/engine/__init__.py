"""
ConstructSync Ingestion Engine.

Streaming file reader → batch splitter → async worker pool → Constructor API.
"""

from constructsync.engine.client import ConstructorClient
from constructsync.engine.controller import ConcurrencyController
from constructsync.engine.engine import IngestionEngine
from constructsync.engine.models import BatchResult, IngestionStats, PipelineStage
from constructsync.engine.reader import CatalogReader
from constructsync.engine.sanitizer import SanitizerStage

__all__ = [
    "CatalogReader",
    "ConstructorClient",
    "ConcurrencyController",
    "IngestionEngine",
    "BatchResult",
    "IngestionStats",
    "PipelineStage",
    "SanitizerStage",
]
