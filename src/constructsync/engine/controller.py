"""
Adaptive Concurrency Controller using AIMD (Additive-Increase/Multiplicative-Decrease).

Acts as a dynamic semaphore that adjusts worker pool size based on API outcomes.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

logger = logging.getLogger(__name__)


class ConcurrencyController:
    """
    AIMD Concurrency Controller.
    
    Implements `__aenter__` and `__aexit__` context managers to act as a
    drop-in replacement for `asyncio.Semaphore`.
    """

    def __init__(
        self,
        initial_concurrency: int,
        min_concurrency: int = 1,
        max_concurrency: int = 32,
    ) -> None:
        self.min_concurrency = min_concurrency
        self.max_concurrency = max_concurrency
        self.current_concurrency = initial_concurrency
        self.active_calls = 0
        self._cond = asyncio.Condition()

        logger.info(
            "AIMD ConcurrencyController initialized: initial=%d, min=%d, max=%d",
            self.current_concurrency,
            self.min_concurrency,
            self.max_concurrency,
        )

    async def acquire(self) -> None:
        """Acquire a concurrency slot. Blocks if at limit."""
        async with self._cond:
            while self.active_calls >= self.current_concurrency:
                await self._cond.wait()
            self.active_calls += 1

    async def release(self) -> None:
        """Release a concurrency slot."""
        async with self._cond:
            self.active_calls = max(0, self.active_calls - 1)
            self._cond.notify_all()

    async def __aenter__(self) -> ConcurrencyController:
        await self.acquire()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        await self.release()

    async def register_result(self, success: bool, status_code: int) -> None:
        """
        Feedback loop for the AIMD algorithm.
        
        - Success (200): Additive increase (+1)
        - Rate Limit (429): Multiplicative decrease (/2)
        - Server Error (5xx): Decrement (-1)
        """
        async with self._cond:
            old = self.current_concurrency
            if success:
                # Additive Increase
                self.current_concurrency = min(
                    self.current_concurrency + 1, self.max_concurrency
                )
                if self.current_concurrency > old:
                    logger.info(
                        "AIMD: Success (200) → increasing concurrency from %d to %d",
                        old,
                        self.current_concurrency,
                    )
                    self._cond.notify_all()  # Wake up waiters since limit increased
            else:
                if status_code == 429:
                    # Multiplicative Decrease
                    self.current_concurrency = max(
                        self.current_concurrency // 2, self.min_concurrency
                    )
                    logger.warning(
                        "AIMD: 429 Too Many Requests → halving concurrency from %d to %d",
                        old,
                        self.current_concurrency,
                    )
                elif status_code in (500, 502, 503, 504, 0):
                    # Decrement
                    self.current_concurrency = max(
                        self.current_concurrency - 1, self.min_concurrency
                    )
                    logger.warning(
                        "AIMD: Server Error (%d) → decreasing concurrency from %d to %d",
                        status_code,
                        old,
                        self.current_concurrency,
                    )
                
                # In case concurrency decreased, we still notify so any active bookkeeping behaves properly
                self._cond.notify_all()
