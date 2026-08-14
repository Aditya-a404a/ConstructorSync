"""
Async HTTP client for the Constructor API.

Wraps aiohttp.ClientSession to POST item batches to /v2/items.
Does NOT implement retries — that's the engine's responsibility.
"""

from __future__ import annotations

import json
import logging
import time
from types import TracebackType
from typing import Any

import aiohttp

from constructsync.engine.models import BatchResult

logger = logging.getLogger(__name__)


class ConstructorClient:
    """
    Async client for Constructor's catalog ingestion API.

    Usage:
        async with ConstructorClient(base_url, api_key) as client:
            result = await client.send_batch(items)
    """

    def __init__(
        self,
        base_url: str,
        api_key: str,
        timeout_seconds: float = 30.0,
        max_connections: int = 32,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self.max_connections = max_connections
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self) -> "ConstructorClient":
        connector = aiohttp.TCPConnector(
            limit=self.max_connections,
            limit_per_host=self.max_connections,
        )
        self._session = aiohttp.ClientSession(
            connector=connector,
            timeout=self.timeout,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
        )
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self._session:
            await self._session.close()
            self._session = None

    @property
    def session(self) -> aiohttp.ClientSession:
        if self._session is None:
            raise RuntimeError(
                "ConstructorClient is not initialized. Use 'async with' context manager."
            )
        return self._session

    async def send_batch(self, items: list[dict[str, Any]]) -> BatchResult:
        """
        POST a batch of items to Constructor's /v2/items endpoint.

        Args:
            items: List of item dicts conforming to Constructor's schema.
                   Each must have at minimum 'id' and 'name'.

        Returns:
            BatchResult with success/failure status, status code, and timing.
        """
        url = f"{self.base_url}/v2/items"
        payload = {"items": items}
        item_count = len(items)

        start = time.monotonic()

        try:
            async with self.session.post(url, json=payload) as resp:
                latency_ms = (time.monotonic() - start) * 1000
                body = await resp.json()

                if resp.status == 200:
                    return BatchResult(
                        success=True,
                        status_code=200,
                        task_id=body.get("task_id"),
                        latency_ms=latency_ms,
                        item_count=item_count,
                    )
                else:
                    error_msg = body.get("detail") or body.get("message") or str(body)
                    logger.warning(
                        "Batch POST failed: status=%d error=%s latency=%.1fms",
                        resp.status,
                        error_msg,
                        latency_ms,
                    )
                    return BatchResult(
                        success=False,
                        status_code=resp.status,
                        error_message=error_msg,
                        latency_ms=latency_ms,
                        item_count=item_count,
                    )

        except aiohttp.ClientError as e:
            latency_ms = (time.monotonic() - start) * 1000
            logger.error("HTTP client error: %s (%.1fms)", e, latency_ms)
            return BatchResult(
                success=False,
                status_code=0,
                error_message=f"Connection error: {e}",
                latency_ms=latency_ms,
                item_count=item_count,
            )

        except Exception as e:
            latency_ms = (time.monotonic() - start) * 1000
            logger.error("Unexpected error sending batch: %s (%.1fms)", e, latency_ms)
            return BatchResult(
                success=False,
                status_code=0,
                error_message=f"Unexpected error: {e}",
                latency_ms=latency_ms,
                item_count=item_count,
            )
