"""Async retry with exponential backoff and jitter."""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Callable, Coroutine
from functools import wraps
from typing import Any, TypeVar

logger = logging.getLogger(__name__)

F = TypeVar("F")


def async_retry(
    *,
    max_attempts: int = 5,
    base_delay: float = 0.5,
    max_delay: float = 30.0,
    backoff: float = 2.0,
    jitter: float = 0.1,
    exceptions: tuple[type[Exception], ...] = (Exception,),
) -> Callable[[Any], Any]:
    """Decorator: retry an async function with exponential backoff + jitter.

    Args:
        max_attempts: Total number of attempts (including first).
        base_delay: Initial delay in seconds.
        max_delay: Cap on delay.
        backoff: Multiplier applied after each failure.
        jitter: Fraction of delay added as random noise.
        exceptions: Only retry on these exception types.
    """
    def decorator(func: Callable[..., Coroutine[Any, Any, Any]]) -> Callable[..., Coroutine[Any, Any, Any]]:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            delay = base_delay
            for attempt in range(1, max_attempts + 1):
                try:
                    return await func(*args, **kwargs)
                except exceptions as exc:
                    if attempt == max_attempts:
                        logger.error(
                            "%s failed after %d attempts: %s",
                            func.__name__, max_attempts, exc,
                        )
                        raise
                    noise = random.uniform(-jitter, jitter) * delay
                    sleep_for = min(delay + noise, max_delay)
                    logger.warning(
                        "%s attempt %d/%d failed: %s — retrying in %.2fs",
                        func.__name__, attempt, max_attempts, exc, sleep_for,
                    )
                    await asyncio.sleep(sleep_for)
                    delay = min(delay * backoff, max_delay)
            return None  # unreachable
        return wrapper
    return decorator
