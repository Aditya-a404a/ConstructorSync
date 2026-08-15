"""
ConstructSync Ingestion Engine.

Streaming file reader → batch splitter → async worker pool → Constructor API.
"""

from constructsync.engine.client import ConstructorClient
from constructsync.engine.controller import ConcurrencyController
from constructsync.engine.dlq import DeadLetterQueue
from constructsync.engine.dummyjson import DummyJSONReader
from constructsync.engine.engine import IngestionEngine
from constructsync.engine.models import BatchResult, IngestionStats, PipelineStage
from constructsync.engine.reader import CatalogReader
from constructsync.engine.sanitizer import SanitizerStage
from constructsync.engine.scorer import HealthScorer

__all__ = [
    "CatalogReader",
    "ConstructorClient",
    "ConcurrencyController",
    "DeadLetterQueue",
    "DummyJSONReader",
    "HealthScorer",
    "IngestionEngine",
    "BatchResult",
    "IngestionStats",
    "PipelineStage",
    "SanitizerStage",
]
