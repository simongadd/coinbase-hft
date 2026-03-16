"""Token-bucket rate limiter for Coinbase API rate limits."""

from __future__ import annotations

import asyncio
import logging
import time

logger = logging.getLogger(__name__)


class TokenBucketRateLimiter:
    """Async token-bucket rate limiter.

    Coinbase Advanced Trade allows ~10 private requests/second.
    Acquire a token before each request; if depleted, wait until refilled.
    """

    def __init__(self, rate: float = 10.0, burst: float = 10.0) -> None:
        """
        Args:
            rate: Tokens added per second.
            burst: Maximum token capacity (allows short bursts).
        """
        self._rate = rate
        self._capacity = burst
        self._tokens = burst
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: float = 1.0) -> None:
        """Block until the requested number of tokens are available."""
        async with self._lock:
            await self._wait_for_tokens(tokens)

    async def _wait_for_tokens(self, tokens: float) -> None:
        while True:
            self._refill()
            if self._tokens >= tokens:
                self._tokens -= tokens
                return
            # Sleep for the time needed to accumulate enough tokens
            deficit = tokens - self._tokens
            sleep_time = deficit / self._rate
            logger.debug("Rate limit: sleeping %.3fs for %.1f tokens", sleep_time, tokens)
            await asyncio.sleep(sleep_time)

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
        self._last_refill = now

    @property
    def available_tokens(self) -> float:
        self._refill()
        return self._tokens
