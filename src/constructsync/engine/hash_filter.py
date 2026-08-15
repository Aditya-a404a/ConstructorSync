"""
Content Hashing & Deduplication Stage.

Computes SHA-256 hashes of items, filters out unchanged items (unless force is enabled),
and saves computed hashes in HashStore upon successful API sync.
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any

from constructsync.engine.hash_store import HashStore
from constructsync.engine.models import PipelineStage

logger = logging.getLogger(__name__)


class HashFilterStage(PipelineStage):
    """
    Pipeline stage that hashes product content to skip unchanged items.
    """

    def __init__(self, hash_store: HashStore, force_sync: bool = False) -> None:
        self.hash_store = hash_store
        self.force_sync = force_sync
        
        # Maps SKU to computed SHA-256 hash string for items in flight
        self.pending_hashes: dict[str, str] = {}
        
        # Track counts of skipped items
        self.stats = {
            "items_skipped": 0,
            "items_processed": 0,
        }

    @staticmethod
    def calculate_hash(item: dict[str, Any]) -> str:
        """
        Compute SHA-256 hash of Constructor-relevant fields.
        Ignores pipeline-internal or transient fields like health_score.
        """
        payload = {
            "id": item.get("id"),
            "name": item.get("name"),
            "image_url": item.get("image_url"),
            "url": item.get("url"),
            "data": item.get("data", {}),
        }
        serialized = json.dumps(payload, sort_keys=True)
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    async def process(self, batch: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Filter out items whose hashes match the SQLite store."""
        retained = []
        for item in batch:
            sku = item.get("id") or item.get("sku")
            if not sku:
                retained.append(item)
                continue

            self.stats["items_processed"] += 1
            new_hash = self.calculate_hash(item)
            self.pending_hashes[str(sku)] = new_hash

            if not self.force_sync:
                existing_hash = self.hash_store.get_hash(str(sku))
                if existing_hash == new_hash:
                    # Content is identical, skip ingestion
                    self.stats["items_skipped"] += 1
                    continue

            retained.append(item)
        return retained

    def commit_hashes(self, skus: list[str]) -> None:
        """Commit successfully synchronized SKUs and their hashes to SQLite."""
        pairs = []
        for sku in skus:
            str_sku = str(sku)
            if str_sku in self.pending_hashes:
                pairs.append((str_sku, self.pending_hashes[str_sku]))
        
        if pairs:
            self.hash_store.update_hashes(pairs)
            # Clean up pending hashes to free memory
            for sku, _ in pairs:
                self.pending_hashes.pop(sku, None)
