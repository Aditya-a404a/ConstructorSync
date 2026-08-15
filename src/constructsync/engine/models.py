"""
Data models for the ingestion engine.

- BatchResult: outcome of a single batch POST.
- IngestionStats: aggregate counters for an entire sync run.
- PipelineStage: protocol that future stages (sanitize, health-score, DLQ) implement.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable


@dataclass
class BatchResult:
    """Outcome of sending a single batch to the Constructor API."""

    success: bool
    status_code: int
    task_id: str | None = None
    error_message: str | None = None
    latency_ms: float = 0.0
    item_count: int = 0


@dataclass
class IngestionStats:
    """Live-updating aggregate stats for an ingestion run."""

    total_items: int = 0
    items_sent: int = 0
    items_failed: int = 0
    items_skipped: int = 0
    batches_total: int = 0
    batches_sent: int = 0
    batches_failed: int = 0
    retries: int = 0
    api_calls: int = 0
    status_codes: dict[int, int] = field(default_factory=dict)
    start_time: float = field(default_factory=time.monotonic)

    @property
    def elapsed_seconds(self) -> float:
        return time.monotonic() - self.start_time

    @property
    def items_per_second(self) -> float:
        elapsed = self.elapsed_seconds
        return self.items_sent / elapsed if elapsed > 0 else 0.0

    @property
    def batches_remaining(self) -> int:
        return self.batches_total - self.batches_sent - self.batches_failed

    def record_status_code(self, code: int) -> None:
        self.status_codes[code] = self.status_codes.get(code, 0) + 1


@runtime_checkable
class PipelineStage(Protocol):
    """
    Extension point for pipeline stages.

    Each stage receives a batch of item dicts, processes them,
    and returns the (possibly modified) batch. Stages can filter
    items out by returning a shorter list.

    Future implementations:
    - Issue #5: SanitizationStage
    - Issue #9: HealthScoringStage
    - Issue #7: DLQ recording stage
    """

    async def process(self, batch: list[dict]) -> list[dict]:
        """Process a batch of items. Return the processed batch."""
        ...
