"""
Tests for the ConcurrencyController and AIMD dynamic scaling logic.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from constructsync.engine.controller import ConcurrencyController
from constructsync.engine.models import BatchResult


@pytest.mark.asyncio
async def test_aimd_additive_increase():
    """Verify that successful requests increase concurrency up to max_concurrency."""
    controller = ConcurrencyController(initial_concurrency=4, min_concurrency=2, max_concurrency=8)
    
    # Success increment: 4 -> 5
    await controller.register_result(success=True, status_code=200)
    assert controller.current_concurrency == 5
    
    # Success increment: 5 -> 6 -> 7 -> 8
    await controller.register_result(success=True, status_code=200)
    await controller.register_result(success=True, status_code=200)
    await controller.register_result(success=True, status_code=200)
    assert controller.current_concurrency == 8
    
    # Cap check: stays at 8
    await controller.register_result(success=True, status_code=200)
    assert controller.current_concurrency == 8


@pytest.mark.asyncio
async def test_aimd_multiplicative_decrease_and_decrement():
    """Verify rate limit halving and server error decrementing down to min_concurrency."""
    controller = ConcurrencyController(initial_concurrency=8, min_concurrency=2, max_concurrency=12)
    
    # 429 halving: 8 -> 4
    await controller.register_result(success=False, status_code=429)
    assert controller.current_concurrency == 4
    
    # 500 decrement: 4 -> 3
    await controller.register_result(success=False, status_code=500)
    assert controller.current_concurrency == 3
    
    # 429 halving: 3 -> 1 (but capped at min_concurrency which is 2)
    await controller.register_result(success=False, status_code=429)
    assert controller.current_concurrency == 2
    
    # stays at min_concurrency (2)
    await controller.register_result(success=False, status_code=500)
    assert controller.current_concurrency == 2


@pytest.mark.asyncio
async def test_concurrency_blocking_and_release():
    """Verify that ConcurrencyController correctly blocks when limit is reached and resumes on release."""
    controller = ConcurrencyController(initial_concurrency=2, min_concurrency=1, max_concurrency=4)
    
    # First entry: allowed
    await controller.acquire()
    assert controller.active_calls == 1
    
    # Second entry: allowed
    await controller.acquire()
    assert controller.active_calls == 2
    
    # Third entry: should block
    acquired_third = False
    
    async def try_acquire_third():
        nonlocal acquired_third
        await controller.acquire()
        acquired_third = True
        
    task = asyncio.create_task(try_acquire_third())
    
    # Sleep to let loop switch context
    await asyncio.sleep(0.05)
    assert not acquired_third  # Still blocked because active_calls (2) >= current_concurrency (2)
    
    # Release one slot
    await controller.release()
    await asyncio.sleep(0.05)
    
    assert acquired_third  # Successfully acquired now!
    assert controller.active_calls == 2
    
    # Cleanup task and remaining slots
    await controller.release()
    await controller.release()
    assert controller.active_calls == 0


@pytest.mark.asyncio
async def test_aimd_engine_chaos_mode_integration(tmp_path: Path):
    """
    E2E integration test: run IngestionEngine against mock sends with mixed success/failures
    to verify AIMD adjustments trigger correctly.
    """
    from constructsync.engine.engine import IngestionEngine
    from constructsync.settings import ConstructSyncSettings
    import csv

    # 1. Create a dummy CSV file with 15 batches (150 items)
    csv_file = tmp_path / "chaos_test.csv"
    fieldnames = ["sku", "name", "price", "description", "image_url", "category", "brand"]
    with open(csv_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for i in range(150):
            writer.writerow({
                "sku": f"S-{i}",
                "name": f"P-{i}",
                "price": 10.0,
                "description": "desc",
                "image_url": "url",
                "category": "cat",
                "brand": "brand",
            })
            
    # 2. Mock sending batch to fluctuate status codes:
    # Batch 0-2: 200 (Success)
    # Batch 3: 429 (Rate Limit) -> retry -> 200
    # Batch 4: 500 (Server Error) -> retry -> 200
    # Batch 5-14: 200 (Success)
    results_sequence = [
        BatchResult(success=True, status_code=200, task_id="t1", latency_ms=1.0, item_count=10),
        BatchResult(success=True, status_code=200, task_id="t2", latency_ms=1.0, item_count=10),
        BatchResult(success=True, status_code=200, task_id="t3", latency_ms=1.0, item_count=10),
        # Batch 3: first call 429, second 200
        BatchResult(success=False, status_code=429, error_message="Rate limit", latency_ms=1.0, item_count=10),
        BatchResult(success=True, status_code=200, task_id="t4", latency_ms=1.0, item_count=10),
        # Batch 4: first call 500, second 200
        BatchResult(success=False, status_code=500, error_message="Error", latency_ms=1.0, item_count=10),
        BatchResult(success=True, status_code=200, task_id="t5", latency_ms=1.0, item_count=10),
    ] + [
        BatchResult(success=True, status_code=200, task_id=f"t{i}", latency_ms=1.0, item_count=10)
        for i in range(6, 20)
    ]
    
    call_idx = 0
    concurrency_changes = []

    async def mock_send(self_client, items):
        nonlocal call_idx
        res = results_sequence[call_idx]
        call_idx += 1
        return res

    settings = ConstructSyncSettings(
        constructor_api_key="test_key",
        constructor_base_url="http://localhost:9999",
        min_concurrency=2,
        max_concurrency=8,
        hash_store_database_path=str(csv_file.parent / "hashes.db"),
    )
    engine = IngestionEngine(
        file_path=csv_file,
        settings=settings,
        batch_size=10,
        concurrency=4,  # Start at 4
    )
    engine.RETRY_BASE_DELAY = 0.001  # speed up retries
    
    # Track concurrency changes during the run
    original_register = ConcurrencyController.register_result
    
    async def tracking_register(self_ctrl, success, status_code):
        await original_register(self_ctrl, success, status_code)
        concurrency_changes.append(self_ctrl.current_concurrency)

    with patch(
        "constructsync.engine.client.ConstructorClient.send_batch",
        mock_send,
    ), patch(
        "constructsync.engine.controller.ConcurrencyController.register_result",
        tracking_register,
    ):
        stats = await engine.run()
        
    assert stats.items_sent == 150
    assert stats.items_failed == 0
    assert stats.retries == 2  # one for 429, one for 500
    
    # Verify that concurrency actually scaled down (halved on 429, decremented on 500) and scaled back up!
    # Initially 4.
    # Successes: 4 -> 5 -> 6 -> 7
    # 429: halved to 3
    # Retry success: 3 -> 4
    # 500: decremented to 3
    # Retry success: 3 -> 4
    # Successes: 4 -> 5 -> 6 -> 7 -> 8 -> 8 ...
    assert 3 in concurrency_changes  # scaled down
    assert 8 in concurrency_changes  # scaled up to max_concurrency
