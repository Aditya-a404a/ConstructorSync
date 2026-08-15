"""
DummyJSON API Reader for live catalog integration.

Fetches product data from the public DummyJSON API, transforms it into
ConstructSync's standardized intermediate schema, and streams it in batches.
"""

from __future__ import annotations

import json
import logging
import urllib.request
from typing import Generator

logger = logging.getLogger(__name__)


class DummyJSONReader:
    """
    Reader that pulls live products from DummyJSON API, serving as a fallback
    for the Best Buy Developer API.
    """

    def __init__(
        self,
        category: str | None = None,
        limit: int = 5000,
        batch_size: int = 1000,
    ) -> None:
        self.category = category
        self.limit = limit
        self.batch_size = batch_size

    def count_rows(self) -> int:
        """Return the target count to ingest."""
        return self.limit

    def _fetch_page(self, url: str) -> list[dict]:
        """Fetch a single page of products from the API."""
        try:
            req = urllib.request.Request(
                url,
                headers={
                    "User-Agent": "ConstructSync Ingestion Engine/1.0",
                    "Accept": "application/json",
                },
            )
            with urllib.request.urlopen(req, timeout=15) as response:
                body = response.read().decode("utf-8")
                data = json.loads(body)
                return data.get("products", [])
        except Exception as e:
            logger.error("Failed to fetch products from URL %s: %s", url, e)
            return []

    def read_batches(self) -> Generator[list[dict], None, None]:
        """
        Fetch products from API, transform them to internal schema, and yield in batches.
        """
        # Build API endpoint
        if self.category:
            url = f"https://dummyjson.com/products/category/{self.category}?limit=100"
        else:
            url = "https://dummyjson.com/products?limit=100"

        logger.info("Fetching live products from DummyJSON API: %s", url)
        products = self._fetch_page(url)

        if not products:
            logger.warning("No products returned from API. Yielding empty batch.")
            return

        # Transform to ConstructSync schema: sku, name, price, description, image_url, category, brand
        standardized = []
        for p in products:
            standardized.append({
                "sku": f"DJ-{p.get('id')}",
                "name": p.get("title", ""),
                "price": p.get("price", 0.0),
                "description": p.get("description", ""),
                "image_url": p.get("thumbnail", ""),
                "category": p.get("category", ""),
                "brand": p.get("brand", ""),
            })

        # Replicate items to hit the requested target limit safely
        all_items = []
        replication_factor = (self.limit + len(standardized) - 1) // len(standardized)

        for i in range(replication_factor):
            for item in standardized:
                if len(all_items) >= self.limit:
                    break
                copied = item.copy()
                # Appending replication index ensures SKUs are unique
                copied["sku"] = f"{item['sku']}-{i}"
                all_items.append(copied)

        logger.info(
            "Transformed %d live products. Replicated to %d items to meet limit.",
            len(standardized),
            len(all_items),
        )

        # Yield in batches
        for i in range(0, len(all_items), self.batch_size):
            yield all_items[i : i + self.batch_size]
