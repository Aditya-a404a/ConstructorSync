"""
Tests for live API ingestion mode (DummyJSONReader client & schema mapping).
"""

from __future__ import annotations

import pytest
from unittest.mock import patch, MagicMock

from constructsync.engine.dummyjson import DummyJSONReader


def test_dummyjson_reader_fetch_and_replication():
    """Verify that DummyJSONReader fetches products, normalizes them, and scales up to limit."""
    mock_products = [
        {
            "id": 1,
            "title": "Test Phone",
            "price": 500.0,
            "description": "Short desc",
            "thumbnail": "http://image.png",
            "category": "smartphones",
            "brand": "Brand X",
        },
        {
            "id": 2,
            "title": "Test Laptop",
            "price": 1000.0,
            "description": "Lappy desc",
            "thumbnail": "http://image2.png",
            "category": "laptops",
            "brand": "Brand Y",
        }
    ]

    reader = DummyJSONReader(category="laptops", limit=5, batch_size=2)
    
    with patch.object(reader, "_fetch_page", return_value=mock_products):
        batches = list(reader.read_batches())

    # Assert correct batch division (5 items total, batch size 2 -> 3 batches: 2, 2, 1)
    assert len(batches) == 3
    assert len(batches[0]) == 2
    assert len(batches[1]) == 2
    assert len(batches[2]) == 1

    # Assert schema mapping
    first_item = batches[0][0]
    assert first_item["sku"] == "DJ-1-0"  # SKU with replication index
    assert first_item["name"] == "Test Phone"
    assert first_item["price"] == 500.0
    assert first_item["category"] == "smartphones"
    assert first_item["brand"] == "Brand X"

    # Assert replication index makes items unique
    second_copy = batches[1][0]  # item 1 replicated again
    assert second_copy["sku"] == "DJ-1-1"
    assert second_copy["name"] == "Test Phone"
