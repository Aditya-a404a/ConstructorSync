"""
Unit and integration tests for Kafka Event-Driven Ingestion.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from constructsync.engine.engine import IngestionEngine
from constructsync.engine.models import BatchResult
from constructsync.settings import ConstructSyncSettings


@pytest.mark.asyncio
async def test_kafka_event_ingestion_micro_batching(tmp_path: Path):
    """Verify that IngestionEngine consumes events from Kafka, maps, sanitizes, and sends them."""
    settings = ConstructSyncSettings(
        constructor_api_key="test_key",
        constructor_base_url="http://localhost:9999",
        dlq_database_path=str(tmp_path / "dlq.db"),
        hash_store_database_path=str(tmp_path / "hashes.db"),
    )

    # Instantiate IngestionEngine with source="kafka"
    # Flush immediately when we have 2 items
    engine = IngestionEngine(
        source="kafka",
        settings=settings,
        batch_size=2,
        concurrency=1,
    )

    # We simulate three events:
    # 1. product.created (valid data, should ingest)
    # 2. product.deleted (should be skipped)
    # 3. product.updated (valid data, should ingest)
    event1 = {
        "event": "product.created",
        "sku": "SKU-K-1",
        "data": {
            "name": "Laptop",
            "price": 999.99,
            "description": "Premium brand new laptop with a description longer than fifty chars",
        }
    }
    event2 = {
        "event": "product.deleted",
        "sku": "SKU-K-2",
        "data": {}
    }
    event3 = {
        "event": "product.updated",
        "sku": "SKU-K-3",
        "data": {
            "name": "Phone",
            "price": 499.99,
            "description": "Standard smartphone description with a description longer than fifty chars",
        }
    }

    class MockMessage:
        def __init__(self, value):
            self.value = value

    messages = [MockMessage(event1), MockMessage(event2), MockMessage(event3)]
    msg_idx = 0

    async def mock_getone():
        nonlocal msg_idx
        if msg_idx < len(messages):
            msg = messages[msg_idx]
            msg_idx += 1
            return msg
        else:
            # Trigger shutdown after all messages are consumed to exit engine run
            engine._shutdown = True
            raise asyncio.TimeoutError()

    # Mock AIOKafkaConsumer methods
    mock_consumer_instance = MagicMock()
    mock_consumer_instance.start = AsyncMock()
    mock_consumer_instance.stop = AsyncMock()
    mock_consumer_instance.getone = AsyncMock(side_effect=mock_getone)

    mock_consumer_cls = MagicMock(return_value=mock_consumer_instance)

    # Mock client sending to return success
    mock_send = patch(
        "constructsync.engine.client.ConstructorClient.send_batch",
        return_value=MagicMock(success=True, status_code=200, item_count=2)
    )

    with patch("aiokafka.AIOKafkaConsumer", mock_consumer_cls), mock_send:
        # Run engine (will terminate when engine._shutdown is set to True)
        stats = await engine.run()

    # Verify consumer calls
    mock_consumer_instance.start.assert_called_once()
    mock_consumer_instance.stop.assert_called_once()

    # We should have processed event1 and event3 (retained) and ignored event2 (deleted)
    assert stats.total_items == 2
    assert stats.items_sent == 2
    assert stats.items_skipped == 0
