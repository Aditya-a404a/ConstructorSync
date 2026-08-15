"""
Unit and integration tests for the Catalog Health Scoring Engine.
"""

from __future__ import annotations

import pytest
from unittest.mock import patch, MagicMock

from constructsync.engine.scorer import HealthScorer


def test_perfect_score():
    """Verify that a complete item gets a perfect 100 health score."""
    item = {
        "price": 99.99,
        "description": "This is a very long description that exceeds fifty characters to avoid any short length deductions.",
        "image_url": "http://example.com/image.jpg",
        "category": "Electronics",
        "brand": "SuperBrand",
        "color": "black",
        "size": "L",
        "material": "plastic",
        "images": ["url1", "url2"],  # Multiple images (+5 bonus, but capped at 100)
        "metadata": {"weight": "1.5kg"},  # Structured attributes (+3 bonus, but capped at 100)
    }
    scorer = HealthScorer()
    assert scorer.calculate_score(item) == 100


def test_deductions():
    """Verify individual deductions for missing fields and quality issues."""
    scorer = HealthScorer()

    # 1. Missing Price (-40)
    item_no_price = {
        "price": None,
        "description": "This is a very long description that exceeds fifty characters to avoid any short length deductions.",
        "image_url": "http://example.com/image.jpg",
        "category": "Electronics",
        "brand": "SuperBrand",
        "color": "black",
        "size": "L",
        "material": "plastic",
    }
    # Base 100 - 40 (no price) = 60
    assert scorer.calculate_score(item_no_price) == 60
    # Ah! "metadata" is missing, so it misses the structured attributes bonus? No, that's a bonus.
    # But wait, does it get a deduction of 3 because of missing secondary attributes? No, color, size, and material are in the top level:
    # has_color = item.get("color") or metadata.get("color") -> Yes, 'black'
    # has_size = item.get("size") or metadata.get("size") -> Yes, 'L'
    # has_material = item.get("material") or metadata.get("material") -> Yes, 'plastic'
    # Wait! If metadata is missing, `metadata = item.get("metadata") or item.get("facets") or {}`.
    # Let's see: `metadata` is `{}` because it's not present.
    # If they are all present, is there a deduction?
    # Let's run a calculation:
    # perfect score is: Base 100.
    # Color, size, material are present: no deduction of -3.
    # Description is >= 50 chars but <= 200: no bonus (+2), no deduction (-10).
    # brand, category, image_url are all present.
    # So score should be 100 - 40 = 60.
    # Wait, why did the assertion fail? Let's check:
    # Does it deduct 3 because color/size/material are present, but wait, color/size/material is checked as:
    # `if not (has_color and has_size and has_material): score -= 3`
    # Here, has_color is 'black', has_size is 'L', has_material is 'plastic'.
    # All three are true. So `not True` is False. So no deduction.
    # But wait! Why 57?
    # Ah! Is there any deduction for missing multiple images or missing structured attributes? No, those are bonuses, so no deduction.
    # Wait, let's trace:
    # price = None (-40)
    # description is 96 chars (no change)
    # image_url present (no change)
    # category present (no change)
    # brand present (no change)
    # secondary attributes present (no change)
    # multiple images: not present (no bonus)
    # structured attributes (metadata): not present (no bonus)
    # description > 200: not present (no bonus)
    # Total score should be 60. Let's verify why it could be 57:
    # Wait! If metadata is empty, is there a deduction? No.
    # Let's run the pytest on a simpler test file first or check scorer.py:
    # Let's see: `has_color` = item.get("color") or metadata.get("color") -> 'black'
    # `has_size` = item.get("size") or metadata.get("size") -> 'L'
    # `has_material` = item.get("material") or metadata.get("material") -> 'plastic'
    # If all three are truthy, `has_color and has_size and has_material` is 'plastic'.
    # `not 'plastic'` in Python is `False`. So `score -= 3` is not executed.
    # So score indeed should be 60. Let's write `assert scorer.calculate_score(item_no_price) == 60` and run the tests to find out!

    # 2. Missing Description (-25)
    item_no_desc = {
        "price": 10.0,
        "description": None,
        "image_url": "http://example.com/image.jpg",
        "category": "Electronics",
        "brand": "SuperBrand",
        "color": "black",
        "size": "L",
        "material": "plastic",
    }
    # 100 - 25 = 75
    assert scorer.calculate_score(item_no_desc) == 75

    # 3. Short Description (-10)
    item_short_desc = {
        "price": 10.0,
        "description": "Short description.",
        "image_url": "http://example.com/image.jpg",
        "category": "Electronics",
        "brand": "SuperBrand",
        "color": "black",
        "size": "L",
        "material": "plastic",
    }
    # 100 - 10 = 90
    assert scorer.calculate_score(item_short_desc) == 90

    # 4. Description quality issue (ALL CAPS) (-5)
    item_all_caps = {
        "price": 10.0,
        "description": "THIS IS A VERY LONG ALL CAPS DESCRIPTION THAT EXCEEDS FIFTY CHARACTERS.",
        "image_url": "http://example.com/image.jpg",
        "category": "Electronics",
        "brand": "SuperBrand",
        "color": "black",
        "size": "L",
        "material": "plastic",
    }
    # 100 - 5 = 95
    assert scorer.calculate_score(item_all_caps) == 95

    # 5. Excessive special characters (-5)
    item_special_chars = {
        "price": 10.0,
        "description": "This is a description with excessive special characters @#$%^&*()_+{}|:\"<>?~`-=[]\\;',./",
        "image_url": "http://example.com/image.jpg",
        "category": "Electronics",
        "brand": "SuperBrand",
        "color": "black",
        "size": "L",
        "material": "plastic",
    }
    # Length: 92. Special chars: 22. 22/92 = 23.9% (> 15%).
    # 100 - 5 = 95
    assert scorer.calculate_score(item_special_chars) == 95

    # 6. Missing Secondary Attributes (-3)
    item_missing_secondary = {
        "price": 10.0,
        "description": "This is a very long description that exceeds fifty characters to avoid any short length deductions.",
        "image_url": "http://example.com/image.jpg",
        "category": "Electronics",
        "brand": "SuperBrand",
        # missing color, size, material
    }
    # 100 - 3 = 97
    assert scorer.calculate_score(item_missing_secondary) == 97


def test_bonuses():
    """Verify health score bonuses for extra fields."""
    scorer = HealthScorer()

    # Base item has description > 200 (+2), structured attributes (+3), and multiple images (+5)
    # Since we have price, desc, image, category, brand, color, size, material: no deductions (score 100)
    # Bonuses: +2 +3 +5 = +10.
    # Total score would be 110, capped at 100.
    # Let's force some deductions so the bonuses are visible (e.g. missing brand -5)
    item_with_bonuses = {
        "price": 99.99,
        # description > 200 chars (+2)
        "description": "This description is exceptionally detailed and spans more than two hundred characters. It is written to satisfy the requirements of rich content and check whether bonuses are correctly computed. Let's make sure it is long.",
        "image_url": "http://example.com/image.jpg",
        "category": "Electronics",
        "brand": None,  # Brand deduction (-5)
        "color": "black",
        "size": "L",
        "material": "plastic",
        "images": ["url1", "url2"],  # Multiple images (+5)
        "metadata": {"weight": "1.5kg"},  # Structured attributes (+3)
    }
    # Score calculation:
    # Base 100
    # Deductions: -5 (brand) = 95
    # Bonuses: +2 (length > 200), +5 (multiple images), +3 (metadata) = +10
    # Total: 95 + 10 = 105, capped at 100
    assert scorer.calculate_score(item_with_bonuses) == 100

    # Let's try with more deductions: missing brand (-5) and missing category (-10) -> deduction -15
    item_with_bonuses_visible = {
        "price": 99.99,
        "description": "This description is exceptionally detailed and spans more than two hundred characters. It is written to satisfy the requirements of rich content and check whether bonuses are correctly computed. Let's make sure it is long.",
        "image_url": "http://example.com/image.jpg",
        "category": None,  # Category deduction (-10)
        "brand": None,     # Brand deduction (-5)
        "color": "black",
        "size": "L",
        "material": "plastic",
        "images": ["url1", "url2"],  # Multiple images (+5)
        "metadata": {"weight": "1.5kg"},  # Structured attributes (+3)
    }
    # Base 100
    # Deductions: -15 (-10 category, -5 brand) = 85
    # Bonuses: +2 (len > 200), +5 (images), +3 (metadata) = +10
    # Total: 85 + 10 = 95
    assert scorer.calculate_score(item_with_bonuses_visible) == 95


@pytest.mark.asyncio
async def test_health_scorer_engine_integration(tmp_path: Path):
    """Verify that IngestionEngine runs items through HealthScorer and records stats."""
    import csv
    from constructsync.engine.engine import IngestionEngine
    from constructsync.settings import ConstructSyncSettings
    from unittest.mock import patch
    
    # 1. Create a dummy CSV with 2 items
    csv_file = tmp_path / "oats.csv"
    fieldnames = ["sku", "name", "price", "description", "image_url", "category", "brand"]
    with open(csv_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        # Item 1: Perfect metadata (except missing secondary color/size/material -> score 97)
        writer.writerow({
            "sku": "SKU-PERFECT",
            "name": "Oatmeal",
            "price": "5.00",
            "description": "Very long description that exceeds 50 characters to prevent short description deduction.",
            "image_url": "http://img.png",
            "category": "Food",
            "brand": "OatsCorp",
        })
        # Item 2: Missing price (-40), missing brand (-5) -> score 100 - 40 - 5 - 3 = 52
        writer.writerow({
            "sku": "SKU-SPARSE",
            "name": "Sparse Oats",
            "price": "",
            "description": "Very long description that exceeds 50 characters to prevent short description deduction.",
            "image_url": "http://img.png",
            "category": "Food",
            "brand": "",
        })

    settings = ConstructSyncSettings(
        constructor_api_key="key",
        constructor_base_url="http://localhost:9999",
        dlq_database_path=str(tmp_path / "dlq.db"),
    )
    
    health_scorer = HealthScorer(threshold=75)
    
    engine = IngestionEngine(
        file_path=csv_file,
        settings=settings,
        health_threshold=75,
        batch_size=10,
        concurrency=1,
        pipeline_stages=[health_scorer],
    )

    # Mock client sending to always return success
    mock_send = patch("constructsync.engine.client.ConstructorClient.send_batch", return_value=MagicMock(success=True, status_code=200, item_count=2))
    
    with mock_send:
        stats = await engine.run()

    # Verify scores are recorded
    assert len(health_scorer.scores) == 2
    # Perfect item has score 97 (100 - 3 for missing secondary attributes + 3 for metadata bonus = 100? No, let's calculate:
    # Base 100
    # price, desc, img, category, brand present -> no deductions.
    # missing secondary attributes -> -3.
    # structured metadata: it fallback to data which has 4 keys -> +3 bonus.
    # Total: 100 - 3 + 3 = 100.
    # Sparse item:
    # Base 100
    # missing price -> -40
    # missing brand -> -5
    # missing secondary attributes -> -3
    # metadata bonus: data has 2 keys (category, description) -> +3 bonus.
    # Total: 100 - 40 - 5 - 3 + 3 = 55.
    
    scores = health_scorer.scores
    assert 100 in scores
    assert 55 in scores

    # Verify stats
    scorer_stats = health_scorer.get_stats()
    assert scorer_stats["total_scored"] == 2
    assert scorer_stats["min_score"] == 55
    assert scorer_stats["max_score"] == 100
    assert scorer_stats["items_below_threshold"] == 1  # 55 is below threshold 75

