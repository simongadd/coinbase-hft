"""Normalised clock — provides consistent timestamps for real and simulated time."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from decimal import Decimal


class Clock:
    """Real-time clock that returns UTC timestamps.

    In backtesting, subclass and override `now_ns` / `now` to feed simulated time.
    """

    def now_ns(self) -> int:
        """Current time in nanoseconds (monotonic wall clock)."""
        return time.time_ns()

    def now(self) -> datetime:
        """Current UTC datetime."""
        return datetime.now(tz=timezone.utc)

    def now_ts(self) -> float:
        """Current Unix timestamp as float seconds."""
        return time.time()

    def now_ms(self) -> int:
        """Current time in milliseconds."""
        return self.now_ns() // 1_000_000


class SimulatedClock(Clock):
    """Clock driven by external time injection — used in backtesting."""

    def __init__(self, start_ns: int = 0) -> None:
        self._ns = start_ns

    def set_ns(self, ns: int) -> None:
        self._ns = ns

    def advance_ms(self, ms: int) -> None:
        self._ns += ms * 1_000_000

    def now_ns(self) -> int:
        return self._ns

    def now(self) -> datetime:
        return datetime.fromtimestamp(self._ns / 1e9, tz=timezone.utc)

    def now_ts(self) -> float:
        return self._ns / 1e9

    def now_ms(self) -> int:
        return self._ns // 1_000_000


def ns_to_datetime(ns: int) -> datetime:
    return datetime.fromtimestamp(ns / 1e9, tz=timezone.utc)


def datetime_to_ns(dt: datetime) -> int:
    return int(dt.timestamp() * 1e9)


def latency_ms(start_ns: int, end_ns: int) -> Decimal:
    return Decimal(end_ns - start_ns) / Decimal(1_000_000)
