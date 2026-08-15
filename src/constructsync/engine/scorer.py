"""
Catalog Health Scoring Engine pipeline stage.

Evaluates product completeness, quality, and data richness on a 0-100 scale.
Injects the health score into each item and aggregates statistics.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from constructsync.engine.models import PipelineStage

logger = logging.getLogger(__name__)


class HealthScorer(PipelineStage):
    """
    Pipeline stage that scores catalog items based on data quality.
    
    Scores are bounded between 0 and 100.
    Inbound items are modified in-place to contain "health_score".
    """

    def __init__(self, threshold: int = 70) -> None:
        self.threshold = threshold
        self.scores: list[int] = []

    def calculate_score(self, item: dict[str, Any]) -> int:
        """Calculate the 0-100 health score for a single catalog item."""
        score = 100

        # Retrieve fields
        price = item.get("price")
        description = item.get("description")
        image_url = item.get("image_url")
        category = item.get("category")
        brand = item.get("brand")
        metadata = item.get("metadata") or item.get("facets") or {}

        # ── 1. Price Deduction (-40) ──────────────────────────────────────
        # Missing, None, empty string, or price <= 0
        if price is None or price == "" or (isinstance(price, (int, float)) and price <= 0):
            score -= 40

        # ── 2. Description Deduction (-25) ────────────────────────────────
        desc_val = str(description).strip() if description is not None else ""
        if not desc_val:
            score -= 25
        else:
            # ── 5. Description too short (-10) ────────────────────────────
            if len(desc_val) < 50:
                score -= 10
            
            # ── 8. Description Length Bonus (+2) ──────────────────────────
            if len(desc_val) > 200:
                score += 2

            # ── 7. Description Quality Issues (-5) ────────────────────────
            # ALL CAPS deduction
            is_all_caps = desc_val.isupper() and any(c.isalpha() for c in desc_val)
            
            # Excessive special characters deduction (> 15%)
            # Count non-alphanumeric, non-space, non-basic-punctuation characters
            # Basic punctuation: commas, periods, exclamation, question, hyphen, parentheses, single/double quotes
            special_chars = re.sub(r"[a-zA-Z0-9\s,.!?()\-'\"]", "", desc_val)
            is_excessive_special = False
            if len(desc_val) > 0:
                is_excessive_special = (len(special_chars) / len(desc_val)) > 0.15

            if is_all_caps or is_excessive_special:
                score -= 5

        # ── 3. Image URL Deduction (-15) ──────────────────────────────────
        if not image_url or not str(image_url).strip():
            score -= 15

        # ── 4. Category Deduction (-10) ───────────────────────────────────
        if not category or not str(category).strip():
            score -= 10

        # ── 6. Brand Deduction (-5) ───────────────────────────────────────
        if not brand or not str(brand).strip():
            score -= 5

        # ── 8. Missing Secondary Attributes Deduction (-3) ────────────────
        # Look for color, size, material in top level or in metadata
        has_color = item.get("color") or metadata.get("color")
        has_size = item.get("size") or metadata.get("size")
        has_material = item.get("material") or metadata.get("material")
        if not (has_color and has_size and has_material):
            score -= 3

        # ── 9. Multiple Images Bonus (+5) ─────────────────────────────────
        images = item.get("images") or metadata.get("images")
        if isinstance(images, list) and len(images) > 1:
            score += 5

        # ── 10. Structured Attributes Bonus (+3) ──────────────────────────
        # Check if we have key-value pairs in metadata (ignoring any dummy/empty structure)
        if isinstance(metadata, dict) and len(metadata) > 0:
            score += 3

        # Bound score between 0 and 100
        return max(0, min(100, score))

    async def process(self, batch: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Process the batch, injecting health scores in-place."""
        for item in batch:
            score = self.calculate_score(item)
            item["health_score"] = score
            self.scores.append(score)
        return batch

    def get_stats(self) -> dict[str, Any]:
        """Return aggregate statistics of scored items."""
        if not self.scores:
            return {
                "avg_score": 0.0,
                "min_score": 0.0,
                "max_score": 0.0,
                "items_below_threshold": 0,
                "total_scored": 0,
            }
        
        below = sum(1 for s in self.scores if s < self.threshold)
        return {
            "avg_score": round(sum(self.scores) / len(self.scores), 2),
            "min_score": min(self.scores),
            "max_score": max(self.scores),
            "items_below_threshold": below,
            "total_scored": len(self.scores),
        }
