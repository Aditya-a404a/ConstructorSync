"""
Tests for Content Hashing and Deduplication (HashStore & HashFilterStage).
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from constructsync.engine.hash_filter import HashFilterStage
from constructsync.engine.hash_store import HashStore
from constructsync.engine.models import BatchResult


def test_hash_store_crud(tmp_path: Path):
    """Verify standard CRUD and bulk update operations on the SQLite HashStore."""
    db_file = tmp_path / "hashes_test.db"
    store = HashStore(db_file)

    # 1. Initially empty
    assert store.get_hash("SKU-001") is None

    # 2. Update hash
    store.update_hash("SKU-001", "hash123")
    assert store.get_hash("SKU-001") == "hash123"

    # 3. Overwrite hash
    store.update_hash("SKU-001", "hash456")
    assert store.get_hash("SKU-001") == "hash456"

    # 4. Bulk updates
    pairs = [("SKU-002", "hashA"), ("SKU-003", "hashB")]
    store.update_hashes(pairs)
    assert store.get_hash("SKU-002") == "hashA"
    assert store.get_hash("SKU-003") == "hashB"

    # 5. Clear store
    store.clear()
    assert store.get_hash("SKU-001") is None
    assert store.get_hash("SKU-002") is None


@pytest.mark.asyncio
async def test_hash_filter_stage_logic(tmp_path: Path):
    """Verify that HashFilterStage correctly filters out unchanged items and handles force_sync."""
    db_file = tmp_path / "hashes_filter_test.db"
    store = HashStore(db_file)
    
    stage = HashFilterStage(hash_store=store, force_sync=False)

    item1 = {"id": "SKU-1", "name": "Item 1", "data": {"price": 10.0}}
    item2 = {"id": "SKU-2", "name": "Item 2", "data": {"price": 20.0}}
    batch = [item1, item2]

    # Run 1: Store is empty. All items should be processed and retained.
    processed1 = await stage.process(batch)
    assert len(processed1) == 2
    assert stage.stats["items_skipped"] == 0

    # Commit hashes (simulate successful sync)
    stage.commit_hashes(["SKU-1", "SKU-2"])
    assert store.get_hash("SKU-1") is not None
    assert store.get_hash("SKU-2") is not None

    # Run 2: Identical content. All items should be skipped.
    stage.stats = {"items_skipped": 0, "items_processed": 0}
    processed2 = await stage.process(batch)
    assert len(processed2) == 0
    assert stage.stats["items_skipped"] == 2

    # Run 3: Modify item1 content. item1 should be retained, item2 skipped.
    item1_modified = {"id": "SKU-1", "name": "Item 1 Modified", "data": {"price": 10.0}}
    stage.stats = {"items_skipped": 0, "items_processed": 0}
    processed3 = await stage.process([item1_modified, item2])
    assert len(processed3) == 1
    assert processed3[0]["name"] == "Item 1 Modified"
    assert stage.stats["items_skipped"] == 1

    # Run 4: Force Sync enabled. All items retained regardless of matches.
    stage_force = HashFilterStage(hash_store=store, force_sync=True)
    processed4 = await stage_force.process(batch)
    assert len(processed4) == 2
    assert stage_force.stats["items_skipped"] == 0


@pytest.mark.asyncio
async def test_engine_hashing_integration(tmp_path: Path):
    """E2E integration test: verify IngestionEngine skips files in second sync and uses force_sync."""
    import csv
    from constructsync.engine.engine import IngestionEngine
    from constructsync.settings import ConstructSyncSettings

    csv_file = tmp_path / "sync_products.csv"
    fieldnames = ["sku", "name", "price", "description", "image_url", "category", "brand"]
    
    # 1. Setup CSV file
    with open(csv_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow({
            "sku": "OAT-1",
            "name": "Oatmeal",
            "price": "4.50",
            "description": "Tasty organic breakfast oats description to score well",
            "image_url": "http://img.jpg",
            "category": "Food",
            "brand": "EcoFoods",
        })

    settings = ConstructSyncSettings(
        constructor_api_key="key",
        constructor_base_url="http://localhost:9999",
        dlq_database_path=str(tmp_path / "dlq.db"),
        hash_store_database_path=str(tmp_path / "hashes.db"),
    )

    # First Run: Ingestion succeeds, item is synced and hash is saved.
    engine1 = IngestionEngine(file_path=csv_file, settings=settings, batch_size=10, concurrency=1)
    
    mock_send = patch("constructsync.engine.client.ConstructorClient.send_batch", return_value=MagicMock(success=True, status_code=200, item_count=1))
    with mock_send as mock:
        stats1 = await engine1.run()
        assert stats1.items_sent == 1
        assert stats1.items_skipped == 0
        assert mock.call_count == 1

    # Second Run: Content identical. Item should be skipped (0 items sent, 1 items skipped).
    engine2 = IngestionEngine(file_path=csv_file, settings=settings, batch_size=10, concurrency=1)
    with patch("constructsync.engine.client.ConstructorClient.send_batch") as mock_second_send:
        stats2 = await engine2.run()
        assert stats2.items_sent == 0
        assert stats2.items_skipped == 1
        assert mock_second_send.call_count == 0  # No API calls made!

    # Third Run: Force Sync enabled. Item should be synced again.
    engine3 = IngestionEngine(file_path=csv_file, settings=settings, force_sync=True, batch_size=10, concurrency=1)
    with patch("constructsync.engine.client.ConstructorClient.send_batch", return_value=MagicMock(success=True, status_code=200, item_count=1)) as mock_third_send:
        stats3 = await engine3.run()
        assert stats3.items_sent == 1
        assert stats3.items_skipped == 0
        assert mock_third_send.call_count == 1
