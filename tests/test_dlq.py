"""
Tests for the Dead-Letter Queue (DLQ) and Ingestion Sync Report.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from constructsync.engine.client import ConstructorClient
from constructsync.engine.dlq import DeadLetterQueue
from constructsync.engine.models import BatchResult, IngestionStats
from constructsync.engine.report import SyncReportGenerator


def test_dlq_sqlite_crud(tmp_path: Path):
    """Verify standard CRUD and filtering operations on the SQLite DLQ database."""
    db_file = tmp_path / "dlq_test.db"
    dlq = DeadLetterQueue(db_file)

    # Empty list check
    assert len(dlq.list_items()) == 0

    # 1. Insert items
    failed_items = [
        {"id": "SKU-001", "name": "Item 1", "data": {"price": 10.0}},
        {"id": "SKU-002", "name": "Item 2", "data": {"price": 15.0}},
    ]
    dlq.insert_failed_items(failed_items, reason="HTTP 400 Bad Request", retry_count=3)
    
    records = dlq.list_items()
    assert len(records) == 2
    assert records[0]["sku"] in ("SKU-001", "SKU-002")
    assert records[0]["reason"] == "HTTP 400 Bad Request"
    assert records[0]["retry_count"] == 3
    assert records[0]["sanitized_data"]["name"] in ("Item 1", "Item 2")

    # 2. Filter by SKU
    sku_records = dlq.list_items(sku="SKU-001")
    assert len(sku_records) == 1
    assert sku_records[0]["sku"] == "SKU-001"

    # 3. Filter by Reason
    reason_records = dlq.list_items(reason="400 Bad")
    assert len(reason_records) == 2

    # 4. Get individual item
    item_id = records[0]["id"]
    item = dlq.get_item(item_id)
    assert item is not None
    assert item["sku"] == records[0]["sku"]

    # 5. Delete items
    dlq.delete_items([item_id])
    assert len(dlq.list_items()) == 1

    # 6. Clear DLQ
    dlq.clear()
    assert len(dlq.list_items()) == 0


def test_sync_report_generator(tmp_path: Path):
    """Verify that the SyncReportGenerator aggregates stats and writes valid JSON files."""
    stats = IngestionStats(total_items=100)
    stats.items_sent = 90
    stats.items_failed = 10
    stats.batches_total = 10
    stats.batches_sent = 9
    stats.batches_failed = 1
    stats.api_calls = 12
    stats.retries = 3
    stats.record_status_code(200)
    stats.record_status_code(500)

    sanitizer_stats = {
        "items_sanitized": 50,
        "items_failed_validation": 5,
        "tags_stripped": 30,
        "entities_encoded": 15,
        "double_encoded_normalized": 5,
    }

    report = SyncReportGenerator.generate_report_dict(
        stats=stats,
        sanitizer_stats=sanitizer_stats,
        peak_concurrency=8,
    )

    # Dictionary validation
    assert report["run_summary"]["total_items"] == 100
    assert report["run_summary"]["items_sent"] == 90
    assert report["run_summary"]["items_failed"] == 10
    assert report["run_summary"]["peak_concurrency"] == 8
    assert report["sanitization_stats"]["items_sanitized"] == 50
    assert report["status_codes"]["200"] == 1
    assert report["status_codes"]["500"] == 1

    # JSON output verification
    report_file = SyncReportGenerator.write_json_report(report, output_dir=tmp_path)
    assert Path(report_file).exists()

    with open(report_file, "r") as f:
        loaded_report = json.load(f)
    assert loaded_report["run_summary"]["items_sent"] == 90


@pytest.mark.asyncio
async def test_dlq_engine_integration(tmp_path: Path):
    """E2E integration test: failed items after max retries must end up in the DLQ."""
    from constructsync.engine.engine import IngestionEngine
    from constructsync.settings import ConstructSyncSettings
    import csv

    # 1. Create a dummy CSV file with 2 batches (20 items)
    csv_file = tmp_path / "integration_dlq.csv"
    fieldnames = ["sku", "name", "price", "description", "image_url", "category", "brand"]
    with open(csv_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for i in range(20):
            writer.writerow({
                "sku": f"SKU-{i}",
                "name": f"Product-{i}",
                "price": 10.0,
                "description": "description",
                "image_url": "url",
                "category": "category",
                "brand": "brand",
            })

    # 2. Mock client sending to fail on first batch, succeed on second batch
    results_sequence = [
        # Batch 0 failures (3 attempts)
        BatchResult(success=False, status_code=500, error_message="Fatal server error", latency_ms=1.0, item_count=10),
        BatchResult(success=False, status_code=500, error_message="Fatal server error", latency_ms=1.0, item_count=10),
        BatchResult(success=False, status_code=500, error_message="Fatal server error", latency_ms=1.0, item_count=10),
        # Batch 1 success
        BatchResult(success=True, status_code=200, task_id="t2", latency_ms=1.0, item_count=10),
    ]
    call_idx = 0

    async def mock_send(self_client, items):
        nonlocal call_idx
        res = results_sequence[call_idx]
        call_idx += 1
        return res

    settings = ConstructSyncSettings(
        constructor_api_key="test_key",
        constructor_base_url="http://localhost:9999",
        dlq_database_path=str(tmp_path / "dlq_integ.db"),
        hash_store_database_path=str(tmp_path / "hashes.db"),
    )
    engine = IngestionEngine(
        file_path=csv_file,
        settings=settings,
        batch_size=10,
        concurrency=1,
    )
    engine.RETRY_BASE_DELAY = 0.001  # accelerate retry delay

    with patch(
        "constructsync.engine.client.ConstructorClient.send_batch",
        mock_send,
    ):
        stats = await engine.run()

    # 3. Assert counts and DLQ contents
    assert stats.items_sent == 10
    assert stats.items_failed == 10
    assert stats.batches_failed == 1

    # Verify DLQ contains exactly 10 failed items from batch 0
    dlq = DeadLetterQueue(settings.dlq_database_path)
    dlq_items = dlq.list_items()
    assert len(dlq_items) == 10
    
    # Assert SKUs belong to first batch (0 to 9)
    skus = [item["sku"] for item in dlq_items]
    assert "SKU-0" in skus
    assert "SKU-9" in skus
    assert "SKU-10" not in skus  # Batch 1 succeeded
    assert dlq_items[0]["reason"] == "HTTP 500: Fatal server error"


def test_cli_dlq_retry(tmp_path: Path):
    """Verify that reprocessing failed DLQ items sends them to Constructor and deletes them."""
    import argparse
    from constructsync.cli import cmd_dlq_retry
    from constructsync.settings import ConstructSyncSettings, get_settings
    
    # 1. Setup DLQ in temporary path with 5 failed items
    db_file = tmp_path / "dlq_cli_test.db"
    dlq = DeadLetterQueue(db_file)
    failed_items = [{"id": f"SKU-{i}", "name": f"Item {i}"} for i in range(5)]
    dlq.insert_failed_items(failed_items, reason="Error", retry_count=3)
    
    assert len(dlq.list_items()) == 5

    # 2. Setup mock client to return success on retry
    mock_send = AsyncMock(return_value=BatchResult(success=True, status_code=200, task_id="t-retry", item_count=5))
    
    settings = ConstructSyncSettings(
        constructor_api_key="test_key",
        constructor_base_url="http://localhost:9999",
        dlq_database_path=str(db_file),
        hash_store_database_path=str(tmp_path / "hashes.db"),
    )
    
    args = argparse.Namespace(
        base_url="http://localhost:9999",
        api_key="test_key",
    )
    
    with patch(
        "constructsync.settings.get_settings",
        return_value=settings,
    ), patch(
        "constructsync.engine.client.ConstructorClient.send_batch",
        mock_send,
    ):
        cmd_dlq_retry(args)

    # 3. Assert items were retried and cleared from DLQ
    assert mock_send.call_count == 1
    assert len(dlq.list_items()) == 0
