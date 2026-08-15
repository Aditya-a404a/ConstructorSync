"""
Tests for the ingestion engine.

Covers:
- CatalogReader: streaming CSV and JSONL reading
- ConstructorClient: batch sending, error handling
- IngestionEngine: end-to-end integration against the mock API
"""

from __future__ import annotations

import asyncio
import csv
import json
import os
import tempfile
import time
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from constructsync.engine.models import BatchResult, IngestionStats, PipelineStage
from constructsync.engine.reader import CatalogReader


# ═══════════════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════════════

@pytest.fixture
def csv_file(tmp_path: Path) -> Path:
    """Create a test CSV file with 50 rows."""
    filepath = tmp_path / "test_products.csv"
    fieldnames = ["sku", "name", "price", "description", "image_url", "category", "brand"]
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for i in range(50):
            writer.writerow({
                "sku": f"SKU-{i:05d}",
                "name": f"Product {i}",
                "price": round(9.99 + i * 0.5, 2),
                "description": f"Description for product {i}",
                "image_url": f"https://img.example.com/{i}.jpg",
                "category": "Electronics",
                "brand": "TestBrand",
            })
    return filepath


@pytest.fixture
def jsonl_file(tmp_path: Path) -> Path:
    """Create a test JSONL file with 30 items."""
    filepath = tmp_path / "test_products.jsonl"
    with open(filepath, "w") as f:
        for i in range(30):
            item = {
                "id": f"JSONL-{i:05d}",
                "name": f"JSONL Product {i}",
                "data": {"price": round(5.0 + i, 2), "category": "Books"},
            }
            f.write(json.dumps(item) + "\n")
    return filepath


@pytest.fixture
def large_csv_file(tmp_path: Path) -> Path:
    """Create a test CSV file with 5,000 rows for batch testing."""
    filepath = tmp_path / "large_products.csv"
    fieldnames = ["sku", "name", "price", "description", "image_url", "category", "brand"]
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for i in range(5000):
            writer.writerow({
                "sku": f"BULK-{i:06d}",
                "name": f"Bulk Product {i}",
                "price": round(1.0 + (i % 100), 2),
                "description": f"Bulk desc {i}",
                "image_url": f"https://img.example.com/bulk/{i}.jpg",
                "category": "General",
                "brand": "BulkBrand",
            })
    return filepath


# ═══════════════════════════════════════════════════════════════════════
# CatalogReader Tests
# ═══════════════════════════════════════════════════════════════════════

class TestCatalogReader:
    """Tests for the streaming file reader."""

    def test_csv_read_all_rows(self, csv_file: Path):
        """Reader should yield all 50 rows from the CSV."""
        reader = CatalogReader(csv_file, batch_size=1000)
        all_items = []
        for batch in reader.read_batches():
            all_items.extend(batch)
        assert len(all_items) == 50

    def test_csv_batch_size_respected(self, csv_file: Path):
        """Batches should not exceed the configured batch_size."""
        reader = CatalogReader(csv_file, batch_size=10)
        batches = list(reader.read_batches())
        assert len(batches) == 5  # 50 items / 10 per batch
        for batch in batches:
            assert len(batch) <= 10

    def test_csv_row_structure(self, csv_file: Path):
        """Each row dict should have the expected CSV columns."""
        reader = CatalogReader(csv_file, batch_size=50)
        batches = list(reader.read_batches())
        row = batches[0][0]
        assert "sku" in row
        assert "name" in row
        assert "price" in row

    def test_csv_count_rows(self, csv_file: Path):
        """count_rows() should return the correct total."""
        reader = CatalogReader(csv_file, batch_size=10)
        assert reader.count_rows() == 50

    def test_jsonl_read_all_items(self, jsonl_file: Path):
        """Reader should yield all 30 items from the JSONL."""
        reader = CatalogReader(jsonl_file, batch_size=1000)
        all_items = []
        for batch in reader.read_batches():
            all_items.extend(batch)
        assert len(all_items) == 30

    def test_jsonl_batch_size_respected(self, jsonl_file: Path):
        """JSONL batches should not exceed batch_size."""
        reader = CatalogReader(jsonl_file, batch_size=7)
        batches = list(reader.read_batches())
        # 30 items / 7 per batch = 4 full + 1 partial = 5
        assert len(batches) == 5
        assert len(batches[-1]) == 2  # remaining 30 % 7 = 2

    def test_jsonl_count_rows(self, jsonl_file: Path):
        """count_rows() should return the correct total for JSONL."""
        reader = CatalogReader(jsonl_file, batch_size=10)
        assert reader.count_rows() == 30

    def test_file_not_found(self, tmp_path: Path):
        """Should raise FileNotFoundError for missing files."""
        with pytest.raises(FileNotFoundError):
            CatalogReader(tmp_path / "nonexistent.csv")

    def test_unsupported_format(self, tmp_path: Path):
        """Should raise ValueError for unsupported file extensions."""
        bad_file = tmp_path / "data.xml"
        bad_file.write_text("<items/>")
        with pytest.raises(ValueError, match="Unsupported file format"):
            CatalogReader(bad_file)

    def test_jsonl_skips_malformed_lines(self, tmp_path: Path):
        """Malformed JSONL lines should be skipped, not crash."""
        filepath = tmp_path / "bad.jsonl"
        with open(filepath, "w") as f:
            f.write('{"id": "good-1", "name": "Good"}\n')
            f.write('NOT VALID JSON\n')
            f.write('{"id": "good-2", "name": "Also Good"}\n')

        reader = CatalogReader(filepath, batch_size=100)
        all_items = []
        for batch in reader.read_batches():
            all_items.extend(batch)
        assert len(all_items) == 2

    def test_large_file_batching(self, large_csv_file: Path):
        """Verify correct batching for a larger file (5000 rows)."""
        reader = CatalogReader(large_csv_file, batch_size=1000)
        batches = list(reader.read_batches())
        total = sum(len(b) for b in batches)
        assert total == 5000
        # Each batch should be at most 1000
        for batch in batches:
            assert len(batch) <= 1000


# ═══════════════════════════════════════════════════════════════════════
# ConstructorClient Tests
# ═══════════════════════════════════════════════════════════════════════

class TestConstructorClient:
    """Tests for the async API client."""

    @pytest.mark.asyncio
    async def test_send_batch_success(self):
        """Client should return a successful BatchResult on 200."""
        from constructsync.engine.client import ConstructorClient

        items = [{"id": "test-1", "name": "Test", "data": {}}]
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={"task_id": "task-abc"})
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        async with ConstructorClient("http://fake", "key") as client:
            with patch.object(client.session, "post", return_value=mock_response):
                result = await client.send_batch(items)

        assert result.success is True
        assert result.status_code == 200
        assert result.task_id == "task-abc"
        assert result.item_count == 1

    @pytest.mark.asyncio
    async def test_send_batch_failure(self):
        """Client should return a failed BatchResult on 4xx/5xx."""
        from constructsync.engine.client import ConstructorClient

        items = [{"id": "test-1", "name": "Test", "data": {}}]
        mock_response = AsyncMock()
        mock_response.status = 400
        mock_response.json = AsyncMock(return_value={"detail": "Bad batch"})
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        async with ConstructorClient("http://fake", "key") as client:
            with patch.object(client.session, "post", return_value=mock_response):
                result = await client.send_batch(items)

        assert result.success is False
        assert result.status_code == 400
        assert "Bad batch" in result.error_message

    @pytest.mark.asyncio
    async def test_send_batch_connection_error(self):
        """Client should handle connection errors gracefully."""
        import aiohttp
        from constructsync.engine.client import ConstructorClient

        items = [{"id": "test-1", "name": "Test", "data": {}}]

        async with ConstructorClient("http://fake", "key") as client:
            with patch.object(
                client.session,
                "post",
                side_effect=aiohttp.ClientError("Connection refused"),
            ):
                result = await client.send_batch(items)

        assert result.success is False
        assert result.status_code == 0
        assert "Connection error" in result.error_message


# ═══════════════════════════════════════════════════════════════════════
# Item Mapping Tests
# ═══════════════════════════════════════════════════════════════════════

class TestItemMapping:
    """Tests for the CSV-to-Constructor field mapping."""

    def test_map_csv_row(self):
        """Should map 'sku' to 'id' and pack extras into 'data'."""
        from constructsync.engine.engine import _map_item

        raw = {
            "sku": "B07XYZ123",
            "name": "Cool Widget",
            "price": 29.99,
            "description": "A cool widget",
            "image_url": "https://img.example.com/w.jpg",
            "category": "Electronics",
            "brand": "WidgetCo",
        }
        mapped = _map_item(raw)

        assert mapped["id"] == "B07XYZ123"
        assert mapped["name"] == "Cool Widget"
        assert mapped["image_url"] == "https://img.example.com/w.jpg"
        assert mapped["data"]["price"] == 29.99
        assert mapped["data"]["description"] == "A cool widget"
        assert mapped["data"]["category"] == "Electronics"
        assert mapped["data"]["brand"] == "WidgetCo"

    def test_map_jsonl_row(self):
        """Should handle JSONL rows that already have 'id' field."""
        from constructsync.engine.engine import _map_item

        raw = {
            "id": "JSONL-001",
            "name": "JSONL Product",
            "data": {"price": 10.0},
        }
        mapped = _map_item(raw)

        assert mapped["id"] == "JSONL-001"
        assert mapped["name"] == "JSONL Product"

    def test_map_empty_fields(self):
        """Should handle missing/empty fields gracefully."""
        from constructsync.engine.engine import _map_item

        raw = {"sku": "EMPTY-001", "name": ""}
        mapped = _map_item(raw)

        assert mapped["id"] == "EMPTY-001"
        assert mapped["name"] == ""
        assert mapped["data"] == {}  # no extra fields with values


# ═══════════════════════════════════════════════════════════════════════
# IngestionStats Tests
# ═══════════════════════════════════════════════════════════════════════

class TestIngestionStats:
    """Tests for the stats tracking object."""

    def test_items_per_second(self):
        """Throughput calculation should be correct."""
        stats = IngestionStats()
        stats.items_sent = 1000
        # Fake start time to be 10 seconds ago
        stats.start_time = time.monotonic() - 10.0
        ips = stats.items_per_second
        assert 90 <= ips <= 110  # ~100 items/sec with some tolerance

    def test_batches_remaining(self):
        """Remaining batches = total - sent - failed."""
        stats = IngestionStats(batches_total=100, batches_sent=60, batches_failed=5)
        assert stats.batches_remaining == 35

    def test_record_status_code(self):
        """Status code tracking should accumulate correctly."""
        stats = IngestionStats()
        stats.record_status_code(200)
        stats.record_status_code(200)
        stats.record_status_code(429)
        assert stats.status_codes == {200: 2, 429: 1}


# ═══════════════════════════════════════════════════════════════════════
# PipelineStage Protocol Tests
# ═══════════════════════════════════════════════════════════════════════

class TestPipelineStage:
    """Tests for the PipelineStage protocol."""

    def test_protocol_check(self):
        """A class with the right signature should match the protocol."""

        class MockStage:
            async def process(self, batch: list[dict]) -> list[dict]:
                return batch

        assert isinstance(MockStage(), PipelineStage)

    def test_protocol_negative(self):
        """A class without the method should NOT match."""

        class NotAStage:
            pass

        assert not isinstance(NotAStage(), PipelineStage)


# ═══════════════════════════════════════════════════════════════════════
# Integration Test — Engine against Mock API
# ═══════════════════════════════════════════════════════════════════════

class TestIngestionEngineIntegration:
    """
    End-to-end integration tests running the engine against the
    Constructor mock API (using httpx TestClient via aiohttp mock).

    These tests mock the HTTP layer to avoid needing a running server.
    """

    @pytest.mark.asyncio
    async def test_full_ingestion_small_file(self, csv_file: Path):
        """Ingest a 50-row CSV and verify all items are sent."""
        from constructsync.engine.engine import IngestionEngine
        from constructsync.settings import ConstructSyncSettings

        call_log: list[dict] = []

        async def mock_send_batch(self_client, items):
            call_log.append({"items": items, "count": len(items)})
            return BatchResult(
                success=True,
                status_code=200,
                task_id=f"task-{len(call_log)}",
                latency_ms=5.0,
                item_count=len(items),
            )

        settings = ConstructSyncSettings(
            constructor_api_key="test_key",
            constructor_base_url="http://localhost:9999",
            hash_store_database_path=str(csv_file.parent / "hashes.db"),
        )
        engine = IngestionEngine(
            file_path=csv_file,
            settings=settings,
            batch_size=10,
            concurrency=2,
        )

        with patch(
            "constructsync.engine.client.ConstructorClient.send_batch",
            mock_send_batch,
        ):
            stats = await engine.run()

        assert stats.items_sent == 50
        assert stats.items_failed == 0
        assert stats.batches_sent == 5  # 50 / 10
        total_items_sent = sum(c["count"] for c in call_log)
        assert total_items_sent == 50

    @pytest.mark.asyncio
    async def test_retry_on_server_error(self, csv_file: Path):
        """Engine should retry on 500 errors and eventually succeed."""
        from constructsync.engine.engine import IngestionEngine
        from constructsync.settings import ConstructSyncSettings

        attempt_count = {"value": 0}

        async def flaky_send_batch(self_client, items):
            attempt_count["value"] += 1
            # Fail first attempt, succeed on second
            if attempt_count["value"] == 1:
                return BatchResult(
                    success=False,
                    status_code=500,
                    error_message="Internal Server Error (Chaos)",
                    latency_ms=10.0,
                    item_count=len(items),
                )
            return BatchResult(
                success=True,
                status_code=200,
                task_id="task-retry",
                latency_ms=5.0,
                item_count=len(items),
            )

        settings = ConstructSyncSettings(
            constructor_api_key="test_key",
            constructor_base_url="http://localhost:9999",
            hash_store_database_path=str(csv_file.parent / "hashes.db"),
        )
        engine = IngestionEngine(
            file_path=csv_file,
            settings=settings,
            batch_size=50,  # Single batch for all 50 items
            concurrency=1,
        )
        engine.RETRY_BASE_DELAY = 0.01  # Speed up test

        with patch(
            "constructsync.engine.client.ConstructorClient.send_batch",
            flaky_send_batch,
        ):
            stats = await engine.run()

        assert stats.items_sent == 50
        assert stats.retries >= 1

    @pytest.mark.asyncio
    async def test_no_item_duplication(self, large_csv_file: Path):
        """Verify no item is sent more than once during ingestion."""
        from constructsync.engine.engine import IngestionEngine
        from constructsync.settings import ConstructSyncSettings

        seen_ids: set[str] = set()
        duplicates: list[str] = []

        async def tracking_send_batch(self_client, items):
            for item in items:
                if item["id"] in seen_ids:
                    duplicates.append(item["id"])
                seen_ids.add(item["id"])
            return BatchResult(
                success=True,
                status_code=200,
                task_id="task-dup-check",
                latency_ms=2.0,
                item_count=len(items),
            )

        settings = ConstructSyncSettings(
            constructor_api_key="test_key",
            constructor_base_url="http://localhost:9999",
            hash_store_database_path=str(large_csv_file.parent / "hashes.db"),
        )
        engine = IngestionEngine(
            file_path=large_csv_file,
            settings=settings,
            batch_size=1000,
            concurrency=4,
        )

        with patch(
            "constructsync.engine.client.ConstructorClient.send_batch",
            tracking_send_batch,
        ):
            stats = await engine.run()

        assert len(duplicates) == 0, f"Duplicate items found: {duplicates[:10]}"
        assert stats.items_sent == 5000
        assert len(seen_ids) == 5000

    @pytest.mark.asyncio
    async def test_pipeline_stage_integration(self, csv_file: Path):
        """Pipeline stages should be called for each batch."""
        from constructsync.engine.engine import IngestionEngine
        from constructsync.settings import ConstructSyncSettings

        stage_calls: list[int] = []

        class CountingStage:
            async def process(self, batch: list[dict]) -> list[dict]:
                stage_calls.append(len(batch))
                return batch

        async def mock_send(self_client, items):
            return BatchResult(
                success=True, status_code=200, task_id="t",
                latency_ms=1.0, item_count=len(items),
            )

        settings = ConstructSyncSettings(
            constructor_api_key="test_key",
            constructor_base_url="http://localhost:9999",
            hash_store_database_path=str(csv_file.parent / "hashes.db"),
        )
        engine = IngestionEngine(
            file_path=csv_file,
            settings=settings,
            batch_size=25,
            concurrency=1,
            pipeline_stages=[CountingStage()],
        )

        with patch(
            "constructsync.engine.client.ConstructorClient.send_batch",
            mock_send,
        ):
            stats = await engine.run()

        assert len(stage_calls) == 2  # 50 items / 25 = 2 batches
        assert sum(stage_calls) == 50
