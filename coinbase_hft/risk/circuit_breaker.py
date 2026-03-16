"""Circuit breakers — auto-halt trading on drawdown, latency, or error spikes."""

from __future__ import annotations

import logging
import time
from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum, auto

logger = logging.getLogger(__name__)


class CircuitBreakerReason(Enum):
    DRAWDOWN = auto()
    DAILY_LOSS = auto()
    LATENCY = auto()
    ERROR_RATE = auto()
    RUNAWAY_POSITION = auto()
    MANUAL = auto()


@dataclass
class CircuitBreakerEvent:
    reason: CircuitBreakerReason
    detail: str
    ts: float


class CircuitBreaker:
    """Monitors trading health metrics and triggers a halt when thresholds are breached.

    Once triggered, must be manually reset (or session restarted) to resume trading.
    """

    def __init__(
        self,
        max_drawdown_pct: Decimal,
        daily_loss_limit_usd: Decimal,
        max_latency_ms: int,
        max_error_rate: int,
        error_window_seconds: int = 60,
    ) -> None:
        self.max_drawdown_pct = max_drawdown_pct
        self.daily_loss_limit_usd = daily_loss_limit_usd
        self.max_latency_ms = max_latency_ms
        self.max_error_rate = max_error_rate
        self.error_window_seconds = error_window_seconds

        self._triggered = False
        self._reason: CircuitBreakerReason | None = None
        self._trigger_event: CircuitBreakerEvent | None = None
        self._error_timestamps: deque[float] = deque()

    @property
    def is_triggered(self) -> bool:
        return self._triggered

    @property
    def trigger_event(self) -> CircuitBreakerEvent | None:
        return self._trigger_event

    def check_drawdown(self, drawdown_pct: Decimal) -> bool:
        """Return True if circuit breaker triggers. drawdown_pct should be negative."""
        if drawdown_pct <= -abs(self.max_drawdown_pct):
            self._trigger(
                CircuitBreakerReason.DRAWDOWN,
                f"Drawdown {float(drawdown_pct)*100:.2f}% exceeded limit "
                f"{float(self.max_drawdown_pct)*100:.2f}%",
            )
            return True
        return False

    def check_daily_loss(self, daily_pnl: Decimal) -> bool:
        if daily_pnl <= -abs(self.daily_loss_limit_usd):
            self._trigger(
                CircuitBreakerReason.DAILY_LOSS,
                f"Daily loss ${daily_pnl:.2f} exceeded limit ${self.daily_loss_limit_usd:.2f}",
            )
            return True
        return False

    def check_latency(self, latency_ms: float) -> bool:
        if latency_ms > self.max_latency_ms:
            self._trigger(
                CircuitBreakerReason.LATENCY,
                f"WebSocket latency {latency_ms:.0f}ms exceeded limit {self.max_latency_ms}ms",
            )
            return True
        return False

    def record_error(self) -> bool:
        """Record an order error; return True if error rate triggers the breaker."""
        now = time.monotonic()
        self._error_timestamps.append(now)
        # Prune old errors outside the window
        cutoff = now - self.error_window_seconds
        while self._error_timestamps and self._error_timestamps[0] < cutoff:
            self._error_timestamps.popleft()
        if len(self._error_timestamps) > self.max_error_rate:
            self._trigger(
                CircuitBreakerReason.ERROR_RATE,
                f"{len(self._error_timestamps)} errors in {self.error_window_seconds}s "
                f"(limit {self.max_error_rate})",
            )
            return True
        return False

    def trigger_manual(self, reason: str = "Manual kill switch") -> None:
        self._trigger(CircuitBreakerReason.MANUAL, reason)

    def reset(self) -> None:
        """Reset the circuit breaker — requires deliberate operator action."""
        logger.warning("Circuit breaker RESET by operator")
        self._triggered = False
        self._reason = None
        self._trigger_event = None
        self._error_timestamps.clear()

    def _trigger(self, reason: CircuitBreakerReason, detail: str) -> None:
        if self._triggered:
            return
        self._triggered = True
        self._reason = reason
        self._trigger_event = CircuitBreakerEvent(reason=reason, detail=detail, ts=time.time())
        logger.critical(
            "CIRCUIT BREAKER TRIGGERED [%s]: %s",
            reason.name, detail,
        )
